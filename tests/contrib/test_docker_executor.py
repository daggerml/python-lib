from __future__ import annotations

import json
import os
import shutil
from typing import Any
from unittest.mock import patch

import pytest

from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.adapters import LocalAdapter
from daggerml.contrib.executors import DockerExecutor, ScriptExecutor


@pytest.fixture(autouse=True)
def _reset_registries(tmp_path, monkeypatch):
    areg._reset_for_tests()
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_TEST_FN_STATE_DIR", str(tmp_path / "state"))
    # Ensure docker_bin resolves on platforms without docker (tests mock all docker calls)
    _orig_which = shutil.which
    monkeypatch.setattr(shutil, "which", lambda n: "/usr/bin/docker" if n == "docker" else _orig_which(n))
    areg.register_adapter(LocalAdapter)
    ereg.register_executor(ScriptExecutor)
    ereg.register_executor(DockerExecutor)
    yield
    areg._reset_for_tests()
    ereg._reset_for_tests()


def _remote() -> dict[str, str]:
    return {"root": os.environ["DML_REMOTE_ROOT"]}


def _sub_runnable() -> Runnable:
    return Runnable(target=Uri("script"), adapter="dml-local-adapter", kwargs={"x": 1}, sub=None)


def _docker_runnable(**kwargs: Any) -> Runnable:
    return Runnable(target=Uri("docker"), adapter="dml-local-adapter", kwargs=kwargs, sub=_sub_runnable())


def test_local_adapter_docker_resolve_runnable_shape():
    sub = _sub_runnable()
    result = LocalAdapter.resolve_runnable("docker", {"image": Uri("s3://bucket/image.tar"), "flags": ["--rm"]}, sub)

    assert isinstance(result, Runnable)
    assert result.target.uri == "docker"
    assert result.adapter == "dml-local-adapter"
    assert result.sub is sub
    assert result.kwargs == {"image": Uri("s3://bucket/image.tar"), "flags": ["--rm"]}


def test_local_adapter_docker_resolve_runnable_rejects_invalid_inputs():
    sub = _sub_runnable()

    with pytest.raises(DmlRepoError, match="requires sub runnable"):
        LocalAdapter.resolve_runnable("docker", {"image": Uri("s3://bucket/image.tar")}, None)

    with pytest.raises(DmlRepoError, match="requires image"):
        LocalAdapter.resolve_runnable("docker", {}, sub)


def test_docker_executor_start_launches_container_and_returns_running(monkeypatch):
    """start() should run docker and return launch state."""
    runnable = _docker_runnable(image=Uri("s3://bucket/image.tar"), flags=["--rm"])
    docker_calls: list[tuple[Any, ...]] = []

    monkeypatch.setattr(DockerExecutor, "_prepare_image", staticmethod(lambda *args, **_: ("repo/name:tag", None)))
    monkeypatch.setattr(
        DockerExecutor,
        "_run_docker",
        staticmethod(lambda *args, **kwargs: docker_calls.append(args) or "cid-123"),
    )

    cache_key = "ck-docker-start"
    argv_ptr = "s3://bucket/argv"

    executor = DockerExecutor()
    result = executor.start(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        execution_id="exec-docker-start",
        remote=_remote(),
    )

    assert result["status"] == "running"
    written_state = result["state"]
    assert written_state["container_id"] == "cid-123"
    assert "workdir" in written_state
    assert "output_path" in written_state

    # Check docker run args
    assert docker_calls[0][0] == "run"
    assert "dml-local-adapter" in docker_calls[0]
    assert "--poll" in docker_calls[0]


def test_docker_executor_poll_returns_succeeded_when_container_exited_with_result(tmp_path, monkeypatch):
    """poll() reads output.json from workdir when container has exited."""
    import subprocess

    output_path = tmp_path / "output.json"
    output_path.write_text(json.dumps({"status": "succeeded", "error": None, "dag_id": "a" * 64}))

    job_state = {
        "container_id": "cid-ok",
        "workdir": str(tmp_path),
        "output_path": str(output_path),
        "cleanup_image": None,
    }

    # Mock docker inspect to return "exited"
    def fake_run(cmd, **kwargs):
        class FakeProc:
            returncode = 0
            stdout = "exited\n"
            stderr = ""
        return FakeProc()

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = DockerExecutor().poll(cache_key="ck-poll-ok", execution_id="exec-ok", state=job_state, remote=_remote())
    assert result["status"] == "succeeded"
    assert result["dag_id"] == "a" * 64


def test_docker_executor_poll_returns_running_when_container_still_running(monkeypatch):
    import subprocess

    job_state = {
        "container_id": "cid-running",
        "workdir": "/tmp/fake",
        "output_path": "/tmp/fake/output.json",
        "cleanup_image": None,
    }

    def fake_run(cmd, **kwargs):
        class FakeProc:
            returncode = 0
            stdout = "running\n"
            stderr = ""
        return FakeProc()

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = DockerExecutor().poll(
        cache_key="ck-poll-running", execution_id="exec-running", state=job_state, remote=_remote()
    )
    assert result["status"] == "running"


def test_docker_executor_poll_returns_failed_when_no_output(tmp_path, monkeypatch):
    import subprocess

    job_state = {
        "container_id": "cid-no-output",
        "workdir": str(tmp_path),
        "output_path": str(tmp_path / "output.json"),
        "cleanup_image": None,
    }

    def fake_run(cmd, **kwargs):
        class FakeProc:
            returncode = 0
            stdout = "exited\n"
            stderr = ""
        return FakeProc()

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = DockerExecutor().poll(
        cache_key="ck-poll-no-output", execution_id="exec-no-output", state=job_state, remote=_remote()
    )
    assert result["status"] == "failed"
    assert "without output" in result["error"]
