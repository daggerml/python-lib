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
from daggerml.contrib.executor_state import ExecutionState
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
    return {"root": os.environ["DML_REMOTE_ROOT"]}


def _sub_runnable() -> Runnable:
    return Runnable(target=Uri("script"), adapter="dml-local-adapter", kwargs={"x": 1}, sub=None)


def _docker_runnable(**kwargs: Any) -> Runnable:
    return Runnable(target=Uri("docker"), adapter="dml-local-adapter", kwargs=kwargs, sub=_sub_runnable())


def _set_child_state(cache_key: str, *, status: str, dag_id: str | None = None, error: str | None = None) -> None:
    state = ExecutionState(cache_key)
    record = state.get()
    assert record is not None
    if record["status"] == "pending":
        assert state.claim_running()
    assert state.lock()
    try:
        if status == "succeeded":
            assert dag_id is not None
            assert state.mark_succeeded(dag_id)
            return
        if status == "failed":
            assert error is not None
            assert state.mark_failed(error)
            return
        raise AssertionError(f"unsupported child status: {status}")
    finally:
        state.unlock()


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
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")

    monkeypatch.setattr(DockerExecutor, "_prepare_image", staticmethod(lambda *args, **_: ("repo/name:tag", None)))
    monkeypatch.setattr(
        DockerExecutor,
        "_run_docker",
        staticmethod(lambda *args, **kwargs: calls.append(args) or "cid-123"),
    )

    cache_key = "ck-docker-start"
    argv_ptr = "s3://bucket/argv"
    ExecutionState.upsert(cache_key, argv_ptr)
    assert ExecutionState(cache_key).claim_running()

    executor = DockerExecutor()
    record = ExecutionState(cache_key).get()
    assert record is not None
    executor.start(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        remote=_remote(),
        state=record,
    )

    record = ExecutionState(cache_key).get()
    assert record is not None
    assert record["status"] == "running"
    metadata = cast(dict[str, Any], record["metadata"]["docker"])
    input_path = Path(cast(str, metadata["workdir"])) / "input.json"
    payload = json.loads(input_path.read_text())
    assert payload["runnable"]["target"] == "script"
    assert payload["argv_ptr"] == "s3://bucket/argv"
    assert payload["cache_key"] == "ck-docker-start:docker-child"
    assert metadata["child_cache_key"] == "ck-docker-start:docker-child"
    assert calls[0][0] == "run"
    assert "dml-local-adapter" in calls[0]
    assert "--poll" in calls[0]
    assert "-e" in calls[0]
    assert f"DML_DYNAMODB_TABLE={os.environ['DML_DYNAMODB_TABLE']}" in calls[0]
    assert f"AWS_ACCESS_KEY_ID={os.environ['AWS_ACCESS_KEY_ID']}" in calls[0]
    assert f"AWS_SECRET_ACCESS_KEY={os.environ['AWS_SECRET_ACCESS_KEY']}" in calls[0]


def test_docker_executor_poll_projects_child_success(tmp_path):
    cache_key = "ck-docker-success"
    child_cache_key = "ck-docker-success:docker-child"
    argv_ptr = "s3://bucket/argv"
    ExecutionState.upsert(cache_key, argv_ptr)
    ExecutionState.upsert(child_cache_key, argv_ptr)
    _set_child_state(child_cache_key, status="succeeded", dag_id="dag-docker-success")

    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.update_metadata(
        {
            "docker": {
                "child_cache_key": child_cache_key,
                "container_id": "cid-1",
                "workdir": str(tmp_path / "docker-success"),
                "cleanup_image": None,
            }
        }
    )
    es.unlock()

    record = es.get()
    assert record is not None
    DockerExecutor().poll(cache_key=cache_key, state=record)

    final = es.get()
    assert final is not None
    assert final["status"] == "succeeded"
    assert final["dag_id"] == "dag-docker-success"


def test_docker_executor_poll_projects_child_failure(tmp_path):
    cache_key = "ck-docker-fail"
    child_cache_key = "ck-docker-fail:docker-child"
    argv_ptr = "s3://bucket/argv"
    ExecutionState.upsert(cache_key, argv_ptr)
    ExecutionState.upsert(child_cache_key, argv_ptr)
    _set_child_state(child_cache_key, status="failed", error="child boom")

    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.update_metadata(
        {
            "docker": {
                "child_cache_key": child_cache_key,
                "container_id": "cid-1",
                "workdir": str(tmp_path / "docker-fail"),
                "cleanup_image": None,
            }
        }
    )
    es.unlock()

    record = es.get()
    assert record is not None
    DockerExecutor().poll(cache_key=cache_key, state=record)

    final = es.get()
    assert final is not None
    assert final["status"] == "failed"
    assert final["error"] == "child boom"


def test_docker_executor_poll_detects_stale_child_heartbeat(monkeypatch, tmp_path):
    import time

    cache_key = "ck-docker-stale"
    child_cache_key = "ck-docker-stale:docker-child"
    argv_ptr = "s3://bucket/argv"
    ExecutionState.upsert(cache_key, argv_ptr)
    ExecutionState.upsert(child_cache_key, argv_ptr)

    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.update_metadata(
        {
            "docker": {
                "child_cache_key": child_cache_key,
                "container_id": "cid-1",
                "workdir": str(tmp_path / "docker-stale"),
                "cleanup_image": None,
            },
        }
    )
    es.unlock()

    child = ExecutionState(child_cache_key)
    assert child.claim_running()
    assert child.lock()
    child.heartbeat()
    child.unlock()

    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + 120.0)

    record = es.get()
    assert record is not None
    executor = DockerExecutor()
    executor.poll(cache_key=cache_key, state=record)

    final = es.get()
    assert final is not None
    assert final["status"] == "failed"
    assert final["error"] is not None
    assert "stale docker heartbeat" in final["error"]
