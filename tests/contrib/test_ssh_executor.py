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
from daggerml.contrib.executors import ScriptExecutor, SshExecutor


@pytest.fixture(autouse=True)
def _reset_registries(tmp_path, monkeypatch):
    areg._reset_for_tests()
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_TEST_FN_STATE_DIR", str(tmp_path / "state"))
    areg.register_adapter(LocalAdapter)
    ereg.register_executor(ScriptExecutor)
    ereg.register_executor(SshExecutor)
    yield
    areg._reset_for_tests()
    ereg._reset_for_tests()


def _remote() -> dict[str, str]:
    return {"root": os.environ["DML_REMOTE_URI"]}


def _sub_runnable() -> Runnable:
    return Runnable(target=Uri("script"), adapter="dml-local-adapter", kwargs={"x": 1}, sub=None)


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


def test_ssh_executor_handle_runs_nested_adapter_over_ssh(monkeypatch):
    runnable = Runnable(
        target=Uri("ssh"),
        adapter="dml-local-adapter",
        kwargs={"host": "worker.example", "flags": ["-p", "2222"], "env_files": ["/etc/dml.env"]},
        sub=_sub_runnable(),
    )
    seen: dict[str, Any] = {}

    dag_id = "d" * 64

    def _fake_run(cmd, input=None, capture_output=None, check=None):
        seen["cmd"] = cmd
        seen["payload"] = json.loads(cast(bytes, input).decode("utf-8"))
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=0,
            stdout=json.dumps({"status": "succeeded", "error": None, "dag_id": dag_id}).encode(),
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)

    cache_key = "ck-ssh-start"
    argv_ptr = "s3://bucket/argv"

    result = SshExecutor.handle(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        execution_id="exec-ssh-start",
        remote=_remote(),
        state=None,
    )

    assert result["status"] == "succeeded"
    assert result["dag_id"] == dag_id
    assert seen["cmd"][:4] == ["ssh", "-p", "2222", "worker.example"]
    assert seen["cmd"][4].startswith("set -e; . /etc/dml.env; exec dml-local-adapter")
    assert ". /etc/dml.env" in seen["cmd"][4]
    assert "DML_REMOTE_URI" not in seen["cmd"][4]
    assert "--poll" in seen["cmd"][4]
    assert seen["payload"]["runnable"]["target"] == "script"
    assert seen["payload"]["cache_key"] == cache_key
    assert seen["payload"]["argv_ptr"] == argv_ptr
    assert seen["payload"]["execution_id"] == "exec-ssh-start"
    assert seen["payload"]["state"] is None


def test_ssh_executor_handle_marks_failed_on_ssh_error(monkeypatch):
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

    result = SshExecutor.handle(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        execution_id="exec-ssh-fail",
        remote=_remote(),
        state=None,
    )

    assert result["status"] == "failed"
    assert result["error"] is not None
    assert "SSH command failed (255): permission denied" in result["error"]


def test_ssh_executor_handle_returns_running_result_directly(monkeypatch):
    """SSH executor passes through non-terminal results from the nested adapter."""
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
            returncode=0,
            stdout=b'{"status":"running","error":null,"state":{}}',
            stderr=b"",
        ),
    )

    cache_key = "ck-ssh-running"
    argv_ptr = "s3://bucket/argv"

    result = SshExecutor.handle(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        execution_id="exec-ssh-running",
        remote=_remote(),
        state=None,
    )

    assert result["status"] == "running"
    assert result["state"] == {}


def test_ssh_executor_handle_projects_child_failure(monkeypatch):
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
            returncode=0,
            stdout=b'{"status":"failed","error":"child boom"}',
            stderr=b"",
        ),
    )

    cache_key = "ck-ssh-child-fail"
    argv_ptr = "s3://bucket/argv"

    result = SshExecutor.handle(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        execution_id="exec-ssh-child-fail",
        remote=_remote(),
        state=None,
    )

    assert result["status"] == "failed"
    assert result["error"] == "child boom"


def test_ssh_executor_handle_passes_state_to_transport(monkeypatch):
    runnable = Runnable(
        target=Uri("ssh"),
        adapter="dml-local-adapter",
        kwargs={"host": "worker.example"},
        sub=_sub_runnable(),
    )

    def _fake_run(cmd, input=None, capture_output=None, check=None):
        del cmd, capture_output, check
        payload = json.loads(cast(bytes, input).decode("utf-8"))
        assert payload["state"] == {"job_id": "123"}
        return subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps({"status": "succeeded", "error": None, "dag_id": "d" * 64}).encode(),
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)

    result = SshExecutor.handle(
        cache_key="ck-ssh-handle",
        execution_id="exec-ssh-handle",
        state={"job_id": "123"},
        runnable=runnable,
        argv_ptr="s3://bucket/argv",
        remote=_remote(),
    )
    assert result["status"] == "succeeded"
