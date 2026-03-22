from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any, cast

import pytest

from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.adapters import LocalAdapter
from daggerml.contrib.executor_state import LocalState
from daggerml.contrib.executors import DockerExecutor, ScriptExecutor


@pytest.fixture(autouse=True)
def _reset_registries(tmp_path, monkeypatch):
    areg._reset_for_tests()
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_FN_CACHE_DIR", str(tmp_path / "state"))
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
    return {"root": os.environ["DML_REMOTE_ROOT"], "cache": os.environ["DML_REMOTE_CACHE"]}


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


def test_docker_executor_start_writes_nested_payload_and_state(monkeypatch):
    runnable = _docker_runnable(image=Uri("s3://bucket/image.tar"), flags=["--rm"])
    calls: list[tuple[Any, ...]] = []

    monkeypatch.setattr(DockerExecutor, "_prepare_image", staticmethod(lambda **_: ("repo/name:tag", None)))
    monkeypatch.setattr(
        DockerExecutor,
        "_run_docker",
        staticmethod(lambda *args, **kwargs: calls.append(args) or "cid-123"),
    )

    cache_key = "ck-docker-start"
    with DockerExecutor.state_class(cache_key).lock() as state:
        assert state is not None
        result = DockerExecutor.start(
            runnable=runnable,
            argv_ptr="s3://bucket/argv",
            cache_key=cache_key,
            remote=_remote(),
            state=state,
        )

    assert result == {"status": "running", "error": None}
    record = LocalState(cache_key).get()
    assert isinstance(record, dict)
    metadata = cast(dict[str, Any], cast(dict[str, Any], record["metadata"])["docker"])
    input_path = Path(cast(str, metadata["workdir"])) / "input.json"
    payload = json.loads(input_path.read_text())
    assert payload["runnable"]["target"] == "script"
    assert payload["argv_ptr"] == "s3://bucket/argv"
    assert payload["cache_key"] == cache_key
    assert payload["comms"]["kind"] == "local"
    assert payload["comms"]["spec"]["cache_dir"].endswith("/state")
    assert metadata["state_dir"].endswith("/state")
    assert calls[0][0] == "run"
    assert "dml-local-adapter" in calls[0]
    assert "--poll" in calls[0]


def test_docker_executor_poll_reads_nested_terminal(monkeypatch, tmp_path):
    cache_key = "ck-docker-poll"
    workdir = tmp_path / "docker-run"
    workdir.mkdir()
    monkeypatch.setattr(DockerExecutor, "_run_docker", staticmethod(lambda *args, **kwargs: ""))

    with LocalState(cache_key, cache_dir=str(tmp_path)).lock() as state:
        assert state is not None
        state.put_if_absent(state.init_record(status="running"))
        state.update(
            state.set_executor_metadata(
                executor_id="docker",
                data={
                    "container_id": "cid-1",
                    "workdir": str(workdir),
                    "state_dir": str(tmp_path),
                    "cleanup_image": None,
                },
            )
        )
    nested = LocalState(cache_key, cache_dir=str(tmp_path))
    record = nested.get()
    assert isinstance(record, dict)
    record["status"] = "succeeded"
    record["error"] = None
    nested.update(record)

    with LocalState(cache_key, cache_dir=str(tmp_path)).lock() as state:
        assert state is not None
        result = DockerExecutor.poll(state=state)

    assert result == {"status": "succeeded", "error": None}


def test_docker_executor_poll_fails_on_stale_nested_heartbeat(monkeypatch, tmp_path):
    cache_key = "ck-docker-stale"
    workdir = tmp_path / "docker-stale"
    workdir.mkdir()
    monkeypatch.setattr(DockerExecutor, "_run_docker", staticmethod(lambda *args, **kwargs: ""))

    with LocalState(cache_key, cache_dir=str(tmp_path)).lock() as state:
        assert state is not None
        state.put_if_absent(state.init_record(status="running"))
        state.update(
            state.set_executor_metadata(
                executor_id="docker",
                data={
                    "container_id": "cid-1",
                    "workdir": str(workdir),
                    "output_path": str(workdir / "output.json"),
                    "state_dir": str(tmp_path),
                    "cleanup_image": None,
                },
            )
        )
    nested = LocalState(cache_key, cache_dir=str(tmp_path))
    record = nested.get()
    assert isinstance(record, dict)
    record["heartbeat_ts"] = 1.0
    nested.update(record)

    with LocalState(cache_key, cache_dir=str(tmp_path)).lock() as state:
        assert state is not None
        result = DockerExecutor.poll(state=state)

    assert result == {"status": "failed", "error": "stale docker heartbeat (container ID: cid-1)"}
