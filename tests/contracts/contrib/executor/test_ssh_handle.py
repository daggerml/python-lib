from __future__ import annotations

import json
import os
import subprocess
from typing import Any, cast

import pytest

from daggerml._internal.types import Runnable, Uri
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.adapters import LocalAdapter
from daggerml.contrib.executors import ScriptExecutor, SshExecutor


@pytest.fixture(autouse=True)
def _reset_registries(tmp_path, monkeypatch):
    areg._reset_for_tests()
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_TEST_FN_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("DML_REMOTE_URI", "s3://test-bucket/test-prefix")
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


def _ssh_runnable() -> Runnable:
    return Runnable(
        target=Uri("ssh"),
        adapter="dml-local-adapter",
        kwargs={"host": "worker.example", "flags": ["-p", "2222"], "env_files": ["/etc/dml.env"]},
        sub=_sub_runnable(),
    )


@pytest.mark.parametrize(
    "contract_id,transport_stdout,transport_returncode,transport_stderr,stage,expected_status,expected_error",
    [
        pytest.param(
            "SSH-HDL-001",
            json.dumps({"status": "succeeded", "error": None, "dag_id": "d" * 64}).encode(),
            0,
            b"",
            "kickoff",
            "succeeded",
            None,
            id="SSH-HDL-001:kickoff-forwards-envelope-and-projects-success",
        ),
        pytest.param(
            "SSH-HDL-002",
            b"",
            255,
            b"permission denied",
            "terminal-failed",
            "failed",
            "SSH command failed (255): permission denied",
            id="SSH-HDL-002:transport-nonzero-projects-failed",
        ),
        pytest.param(
            "SSH-HDL-003",
            b'{"status":"running","error":null,"state":{}}',
            0,
            b"",
            "resume",
            "running",
            None,
            id="SSH-HDL-003:running-child-result-passes-through",
        ),
        pytest.param(
            "SSH-HDL-004",
            b'{"status":"failed","error":"child boom"}',
            0,
            b"",
            "terminal-failed",
            "failed",
            "child boom",
            id="SSH-HDL-004:child-failure-projects-unchanged",
        ),
    ],
)
def test_ssh_executor_handle_stage_matrix_SSH_HDL_001_to_SSH_HDL_004(
    monkeypatch,
    contract_id,
    transport_stdout,
    transport_returncode,
    transport_stderr,
    stage,
    expected_status,
    expected_error,
):
    del contract_id, stage
    runnable = _ssh_runnable()
    seen: dict[str, Any] = {}

    def _fake_run(cmd, input=None, capture_output=None, check=None):
        seen["cmd"] = cmd
        seen["payload"] = json.loads(cast(bytes, input).decode("utf-8"))
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=transport_returncode,
            stdout=transport_stdout,
            stderr=transport_stderr,
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)

    result = SshExecutor.handle(
        runnable=runnable,
        argv_ptr="s3://bucket/argv",
        cache_key="ck-ssh-handle",
        execution_id="exec-ssh-handle",
        remote=_remote(),
        state=None,
    )

    assert result["status"] == expected_status
    if expected_error is None:
        assert result.get("error") is None
    else:
        assert expected_error in result["error"]

    assert seen["cmd"][:4] == ["ssh", "-p", "2222", "worker.example"]
    assert seen["cmd"][4].startswith("set -e; . /etc/dml.env; exec dml-local-adapter")
    assert ". /etc/dml.env" in seen["cmd"][4]
    assert "DML_REMOTE_URI" not in seen["cmd"][4]
    assert "--poll" in seen["cmd"][4]
    assert seen["payload"]["runnable"]["target"] == "script"
    assert seen["payload"]["cache_key"] == "ck-ssh-handle"
    assert seen["payload"]["argv_ptr"] == "s3://bucket/argv"
    assert seen["payload"]["execution_id"] == "exec-ssh-handle"
    assert seen["payload"]["state"] is None


def test_ssh_executor_handle_SSH_HDL_005_forwards_runtime_state_to_transport(monkeypatch):
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
