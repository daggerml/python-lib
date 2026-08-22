from __future__ import annotations

from dataclasses import asdict

import pytest

from daggerml import Runnable, Uri
from daggerml.api import DmlRepoError
from daggerml.contrib.executors._base import ExecutorBase


def _runnable() -> Runnable:
    return Runnable(target=Uri("test"), kwargs={}, adapter="test-adapter")


def _remote() -> dict[str, str]:
    return {"root": "s3://test-bucket/test-prefix"}


def _adapter_request(
    *, operation: str, adapter_state: dict | None, requested_by: str | None = None, argv_ref: str | None = None
) -> dict:
    payload = {
        "operation": operation,
        "cache_key": "ck",
        "execution_id": "exec",
        "adapter_state": adapter_state,
        "runnable": asdict(_runnable()),
        "remote": _remote(),
        "scratch_uri": "s3://bucket/scratch",
    }
    if operation == "cancel":
        payload.update(requested_by=requested_by, argv_ref=argv_ref)
    return payload


class TrackingExecutor(ExecutorBase):
    calls: list[str] = []
    cancel_argv_ptr: str | None = None

    def start(self, **kwargs):
        TrackingExecutor.calls.append("start")
        return {"status": "retry", "error": None, "state": {"token": kwargs["execution_id"]}}

    def poll(self, **kwargs):
        TrackingExecutor.calls.append("poll")
        return {"status": "retry", "error": None, "state": kwargs["state"]}

    def cleanup(self, **kwargs):
        TrackingExecutor.calls.append("cleanup")
        return {"status": "success", "error": None, "state": kwargs["state"]}

    def cancel(self, **kwargs):
        TrackingExecutor.calls.append("cancel")
        TrackingExecutor.cancel_argv_ptr = kwargs["argv_ptr"]
        return {"status": "cancelled", "error": None}


def test_contrib_exec_base_001__handle_routes_missing_state_to_start():
    TrackingExecutor.calls = []
    result = TrackingExecutor.handle(**_adapter_request(operation="invoke", adapter_state=None))
    assert TrackingExecutor.calls == ["start"]
    assert result["status"] == "retry"


def test_contrib_exec_base_002__handle_routes_existing_state_to_poll():
    TrackingExecutor.calls = []
    result = TrackingExecutor.handle(**_adapter_request(operation="invoke", adapter_state={"existing": True}))
    assert TrackingExecutor.calls == ["poll"]
    assert result["adapter_state"] == {"existing": True}


def test_contrib_exec_base_003__cancel_operation_routes_to_cancel():
    TrackingExecutor.calls = []
    TrackingExecutor.cancel_argv_ptr = None
    result = TrackingExecutor.handle(
        **_adapter_request(
            operation="cancel",
            adapter_state={"existing": True},
            requested_by="alice@example.com",
            argv_ref="node-argv:target",
        )
    )
    assert TrackingExecutor.calls == ["cancel"]
    assert TrackingExecutor.cancel_argv_ptr == "node-argv:target"
    assert result == {"status": "cancelled", "error": None}


def test_contrib_exec_base_003__cancel_requires_exact_wire_fields():
    TrackingExecutor.calls = []
    payload = _adapter_request(operation="cancel", adapter_state=None, argv_ref="node-argv:target")
    del payload["adapter_state"]

    with pytest.raises(DmlRepoError, match="Invalid cancel adapter request fields"):
        TrackingExecutor.handle(**payload)
    assert TrackingExecutor.calls == []


def test_contrib_exec_base_004__unknown_operation_is_rejected_before_dispatch():
    TrackingExecutor.calls = []
    with pytest.raises(DmlRepoError, match="Unsupported adapter operation"):
        TrackingExecutor.handle(**_adapter_request(operation="unknown", adapter_state=None))
    assert TrackingExecutor.calls == []


def test_contrib_exec_base_005__cancel_requires_argv_ref():
    TrackingExecutor.calls = []
    with pytest.raises(DmlRepoError, match="requires a non-empty argv_ref"):
        TrackingExecutor.handle(**_adapter_request(operation="cancel", adapter_state={}))
    assert TrackingExecutor.calls == []


def test_contrib_exec_base_006__adapter_state_must_be_object_or_null():
    with pytest.raises(DmlRepoError, match="adapter_state must be an object or null"):
        TrackingExecutor.handle(**_adapter_request(operation="invoke", adapter_state="bad"))


def test_contrib_exec_base_007__cleanup_requires_result_and_routes_idempotently():
    TrackingExecutor.calls = []
    payload = _adapter_request(operation="cleanup", adapter_state={"job": "done"})
    payload["result_ref"] = "dag:" + "a" * 64
    result = TrackingExecutor.handle(**payload)
    assert TrackingExecutor.calls == ["cleanup"]
    assert result == {"status": "success", "error": None, "adapter_state": {"job": "done"}}


def test_contrib_exec_base_008__poll_operation_is_rejected():
    with pytest.raises(DmlRepoError, match="Unsupported adapter operation: poll"):
        TrackingExecutor.handle(**_adapter_request(operation="poll", adapter_state=None))
