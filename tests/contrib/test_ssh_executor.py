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
from daggerml.contrib.executor_state import LocalState
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
    return {"root": os.environ["DML_REMOTE_ROOT"], "cache": os.environ["DML_REMOTE_CACHE"]}


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
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=0,
            stdout=b'{"status":"succeeded","error":null}',
            stderr=b"",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)

    cache_key = "ck-ssh-start"
    with SshExecutor.state_class(cache_key).lock() as state:
        assert state is not None
        result = SshExecutor.start(
            runnable=runnable,
            argv_ptr="s3://bucket/argv",
            cache_key=cache_key,
            remote=_remote(),
            state=state,
        )

    assert result == {"status": "succeeded", "error": None}
    assert seen["cmd"][:4] == ["ssh", "-p", "2222", "worker.example"]
    assert seen["cmd"][4].startswith("set -e; . /etc/dml.env; exec dml-local-adapter")
    assert ". /etc/dml.env" in seen["cmd"][4]
    assert "DML_REMOTE_ROOT" not in seen["cmd"][4]
    assert "--poll" not in seen["cmd"][4]
    assert seen["payload"]["runnable"]["target"] == "script"
    assert seen["payload"]["argv_ptr"] == "s3://bucket/argv"
    assert LocalState(cache_key).get() is None


def test_ssh_executor_start_returns_failed_result_on_ssh_error(monkeypatch):
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
    with SshExecutor.state_class(cache_key).lock() as state:
        assert state is not None
        result = SshExecutor.start(
            runnable=runnable,
            argv_ptr="s3://bucket/argv",
            cache_key=cache_key,
            remote=_remote(),
            state=state,
        )

    assert result == {"status": "failed", "error": "SSH command failed (255): permission denied"}
    assert LocalState(cache_key).get() is None


def test_ssh_executor_start_returns_non_terminal_nested_result(monkeypatch):
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
            stdout=b'{"status":"running","error":null}',
            stderr=b"",
        ),
    )

    with SshExecutor.state_class("ck-ssh-running").lock() as state:
        assert state is not None
        result = SshExecutor.start(
            runnable=runnable,
            argv_ptr="s3://bucket/argv",
            cache_key="ck-ssh-running",
            remote=_remote(),
            state=state,
        )

    assert result == {"status": "running", "error": None}
