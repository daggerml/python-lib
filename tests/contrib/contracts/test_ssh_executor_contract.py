from __future__ import annotations

import json
from types import SimpleNamespace

from daggerml.contrib.executors.ssh import SshExecutor


def _runnable() -> dict:
    return {
        "target": {"uri": "ssh"},
        "kwargs": {"host": "worker", "flags": [], "env_files": []},
        "adapter": "dml-local-adapter",
        "sub": {"target": {"uri": "script"}, "kwargs": {}, "adapter": "nested-adapter", "sub": None},
    }


def test_ssh_poll_forwards_adapter_state_on_nested_wire(monkeypatch) -> None:
    calls = []

    def run(command, *, input, capture_output, check, text):
        calls.append((command, json.loads(input)))
        response = {"status": "running", "error": None, "adapter_state": {"poll": 2}, "dag_id": None}
        return SimpleNamespace(returncode=0, stdout=json.dumps(response), stderr="")

    monkeypatch.setattr("daggerml.contrib.executors.ssh.subprocess.run", run)

    response = SshExecutor().poll(
        cache_key="ck",
        execution_id="exec",
        runnable=_runnable(),
        state={"poll": 1},
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/root/exec/io/exec/",
    )

    payload = calls[0][1]
    assert payload["adapter_state"] == {"poll": 1}
    assert "state" not in payload
    assert response["adapter_state"] == {"poll": 2}


def test_ssh_cancel_forwards_argv_ref_on_nested_wire(monkeypatch) -> None:
    calls = []

    def run(command, *, input, capture_output, check, text):
        calls.append((command, json.loads(input)))
        response = {"status": "cancelled", "error": None, "adapter_state": {"job": "stopped"}}
        return SimpleNamespace(returncode=0, stdout=json.dumps(response), stderr="")

    monkeypatch.setattr("daggerml.contrib.executors.ssh.subprocess.run", run)

    SshExecutor().cancel(
        cache_key="ck",
        execution_id="exec",
        runnable=_runnable(),
        state={"job": "running"},
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/root/exec/io/exec/",
        cancel_requested_by="user",
        argv_ptr="node-argv:abc",
    )

    payload = calls[0][1]
    assert payload["operation"] == "cancel"
    assert payload["adapter_state"] == {"job": "running"}
    assert payload["argv_ref"] == "node-argv:abc"
    assert "state" not in payload
    assert "argv_ptr" not in payload


def test_ssh_cancel_with_null_requester_remains_cancel_operation(monkeypatch) -> None:
    payloads = []

    def run(command, *, input, capture_output, check, text):
        payloads.append(json.loads(input))
        response = {"status": "cancelled", "error": None, "adapter_state": {}}
        return SimpleNamespace(returncode=0, stdout=json.dumps(response), stderr="")

    monkeypatch.setattr("daggerml.contrib.executors.ssh.subprocess.run", run)

    SshExecutor().cancel(
        cache_key="ck",
        execution_id="exec",
        runnable=_runnable(),
        state={},
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/root/exec/io/exec/",
        cancel_requested_by=None,
        argv_ptr="node-argv:abc",
    )

    assert payloads[0]["operation"] == "cancel"
    assert payloads[0]["requested_by"] is None
    assert payloads[0]["argv_ref"] == "node-argv:abc"


def test_ssh_cleanup_forwards_result_and_retry_state_across_fresh_calls(monkeypatch) -> None:
    payloads = []
    responses = iter(
        (
            {"status": "retry", "error": None, "adapter_state": {"cleanup": 1}},
            {"status": "success", "error": None, "adapter_state": {"cleanup": 1}},
        )
    )

    def run(command, *, input, capture_output, check, text):
        payloads.append(json.loads(input))
        return SimpleNamespace(returncode=0, stdout=json.dumps(next(responses)), stderr="")

    monkeypatch.setattr("daggerml.contrib.executors.ssh.subprocess.run", run)
    executor = SshExecutor()
    common = {
        "cache_key": "ck",
        "execution_id": "exec",
        "runnable": _runnable(),
        "remote": {"root": "s3://bucket/root"},
        "scratch_uri": "s3://bucket/root/exec/io/exec/",
        "result_ref": "dag:result",
    }

    first = executor.cleanup(state={"job": "done"}, **common)
    second = executor.cleanup(state=first["adapter_state"], **common)

    assert first["status"] == "retry"
    assert second["status"] == "success"
    assert [payload["operation"] for payload in payloads] == ["cleanup", "cleanup"]
    assert payloads[0]["result_ref"] == payloads[1]["result_ref"] == "dag:result"
    assert payloads[1]["adapter_state"] == {"cleanup": 1}
