from __future__ import annotations

import json
import os
import subprocess
from typing import Any, cast

import pytest

from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.adapters import LocalAdapter
from daggerml.contrib.executor_state import ExecutionState
from daggerml.contrib.executors import ScriptExecutor, SshExecutor


@pytest.fixture(autouse=True)
def _reset_registries(tmp_path, monkeypatch):
    areg._reset_for_tests()
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_FN_CACHE_DIR", str(tmp_path / "state"))
    areg.register_adapter(LocalAdapter)
    ereg.register_executor(ScriptExecutor)
    ereg.register_executor(SshExecutor)
    yield
    areg._reset_for_tests()
    ereg._reset_for_tests()


def _remote() -> dict[str, str]:
    return {"root": os.environ["DML_REMOTE_ROOT"]}


def _sub_runnable() -> Runnable:
    return Runnable(target=Uri("script"), adapter="dml-local-adapter", kwargs={"x": 1}, sub=None)


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


def test_local_adapter_ssh_resolve_runnable_shape():
    sub = _sub_runnable()
    result = LocalAdapter.resolve_runnable(
        "ssh",
        {"host": "worker.example", "flags": ["-p", "2222"], "env_files": ["/etc/dml.env"]},
        sub,
    )

    assert isinstance(result, Runnable)
    assert result.target.uri == "ssh"
    assert result.adapter == "dml-local-adapter"
    assert result.sub is sub
    assert result.kwargs == {
        "host": "worker.example",
        "flags": ["-p", "2222"],
        "env_files": ["/etc/dml.env"],
    }


def test_local_adapter_ssh_resolve_runnable_rejects_invalid_inputs():
    sub = _sub_runnable()

    with pytest.raises(DmlRepoError, match="requires sub runnable"):
        LocalAdapter.resolve_runnable("ssh", {"host": "worker.example"}, None)

    with pytest.raises(DmlRepoError, match="requires non-empty host"):
        LocalAdapter.resolve_runnable("ssh", {}, sub)

    with pytest.raises(DmlRepoError, match="Unknown ssh executor kwargs"):
        LocalAdapter.resolve_runnable("ssh", {"host": "worker.example", "user": "alice"}, sub)


def test_ssh_executor_start_runs_nested_adapter_over_ssh(monkeypatch):
    runnable = Runnable(
        target=Uri("ssh"),
        adapter="dml-local-adapter",
        kwargs={"host": "worker.example", "flags": ["-p", "2222"], "env_files": ["/etc/dml.env"]},
        sub=_sub_runnable(),
    )
    seen: dict[str, Any] = {}

    def _fake_run(cmd, input=None, capture_output=None, check=None):
        seen["cmd"] = cmd
        seen["payload"] = json.loads(cast(bytes, input).decode("utf-8"))
        _set_child_state(seen["payload"]["cache_key"], status="succeeded", dag_id="dag-ssh-success")
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=0,
            stdout=b'{"status":"succeeded","error":null}',
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)

    cache_key = "ck-ssh-start"
    argv_ptr = "s3://bucket/argv"
    ExecutionState.upsert(cache_key, argv_ptr)
    assert ExecutionState(cache_key).claim_running()

    executor = SshExecutor()
    record = ExecutionState(cache_key).get()
    assert record is not None
    executor.start(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        remote=_remote(),
        state=record,
    )

    # SSH is synchronous — check state was updated
    final = ExecutionState(cache_key).get()
    assert final is not None
    assert final["status"] == "succeeded"
    assert final["dag_id"] == "dag-ssh-success"
    assert final["metadata"]["ssh"]["child_cache_key"] == "ck-ssh-start:ssh-child"
    assert seen["cmd"][:4] == ["ssh", "-p", "2222", "worker.example"]
    assert seen["cmd"][4].startswith("set -e; . /etc/dml.env; exec dml-local-adapter")
    assert ". /etc/dml.env" in seen["cmd"][4]
    assert "DML_REMOTE_ROOT" not in seen["cmd"][4]
    assert "--poll" not in seen["cmd"][4]
    assert seen["payload"]["runnable"]["target"] == "script"
    assert seen["payload"]["cache_key"] == "ck-ssh-start:ssh-child"
    assert seen["payload"]["argv_ptr"] == "s3://bucket/argv"


def test_ssh_executor_start_marks_failed_on_ssh_error(monkeypatch):
    runnable = Runnable(
        target=Uri("ssh"),
        adapter="dml-local-adapter",
        kwargs={"host": "worker.example"},
        sub=_sub_runnable(),
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=args[0],
            returncode=255,
            stdout=b"",
            stderr=b"permission denied",
        ),
    )

    cache_key = "ck-ssh-fail"
    argv_ptr = "s3://bucket/argv"
    ExecutionState.upsert(cache_key, argv_ptr)
    assert ExecutionState(cache_key).claim_running()

    executor = SshExecutor()
    record = ExecutionState(cache_key).get()
    assert record is not None
    executor.start(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        remote=_remote(),
        state=record,
    )

    final = ExecutionState(cache_key).get()
    assert final is not None
    assert final["status"] == "failed"
    assert final["error"] is not None
    assert "SSH command failed (255): permission denied" in final["error"]


def test_ssh_executor_start_handles_non_terminal_nested_result(monkeypatch):
    runnable = Runnable(
        target=Uri("ssh"),
        adapter="dml-local-adapter",
        kwargs={"host": "worker.example"},
        sub=_sub_runnable(),
    )

    calls: list[str] = []

    def _fake_run(*args, **kwargs):
        payload = json.loads(cast(bytes, kwargs["input"]).decode("utf-8"))
        child_cache_key = payload["cache_key"]
        calls.append(child_cache_key)
        if len(calls) == 1:
            assert ExecutionState(child_cache_key).claim_running()
            return subprocess.CompletedProcess(
                args=args[0],
                returncode=0,
                stdout=b'{"status":"running","error":null}',
                stderr=b"",
            )
        _set_child_state(child_cache_key, status="succeeded", dag_id="dag-ssh-polled")
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=b'{"status":"succeeded","error":null}',
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)

    cache_key = "ck-ssh-running"
    argv_ptr = "s3://bucket/argv"
    ExecutionState.upsert(cache_key, argv_ptr)
    assert ExecutionState(cache_key).claim_running()

    executor = SshExecutor()
    record = ExecutionState(cache_key).get()
    assert record is not None
    executor.start(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        remote=_remote(),
        state=record,
    )

    # Running result remains running until a later poll/transport step finishes it.
    final = ExecutionState(cache_key).get()
    assert final is not None
    assert final["status"] == "running"

    polled = ExecutionState(cache_key).get()
    assert polled is not None
    executor.poll(cache_key=cache_key, state=polled)

    final = ExecutionState(cache_key).get()
    assert final is not None
    assert final["status"] == "succeeded"
    assert final["dag_id"] == "dag-ssh-polled"
    assert calls == ["ck-ssh-running:ssh-child", "ck-ssh-running:ssh-child"]


def test_ssh_executor_start_projects_child_failure(monkeypatch):
    runnable = Runnable(
        target=Uri("ssh"),
        adapter="dml-local-adapter",
        kwargs={"host": "worker.example"},
        sub=_sub_runnable(),
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: (
            _set_child_state(
                json.loads(cast(bytes, kwargs["input"]).decode("utf-8"))["cache_key"],
                status="failed",
                error="child boom",
            )
            or subprocess.CompletedProcess(
                args=args[0],
                returncode=0,
                stdout=b'{"status":"failed","error":"child boom"}',
                stderr=b"",
            )
        ),
    )

    cache_key = "ck-ssh-child-fail"
    argv_ptr = "s3://bucket/argv"
    ExecutionState.upsert(cache_key, argv_ptr)
    assert ExecutionState(cache_key).claim_running()

    executor = SshExecutor()
    record = ExecutionState(cache_key).get()
    assert record is not None
    executor.start(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        remote=_remote(),
        state=record,
    )

    final = ExecutionState(cache_key).get()
    assert final is not None
    assert final["status"] == "failed"
    assert final["error"] == "child boom"
