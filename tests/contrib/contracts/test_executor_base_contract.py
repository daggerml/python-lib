from __future__ import annotations

from dataclasses import asdict

from daggerml import Runnable, Uri
from daggerml.contrib.executors._base import ExecutorBase


def _runnable() -> Runnable:
    return Runnable(target=Uri("test"), kwargs={}, adapter="test-adapter")


def _remote() -> dict[str, str]:
    return {"root": "s3://test-bucket/test-prefix"}


def _adapter_envelope(*, state: dict | None, cancel_requested_by: str | None) -> dict:
    return {
        "cache_key": "ck",
        "execution_id": "exec",
        "state": state,
        "cancel_requested_by": cancel_requested_by,
        "runnable": asdict(_runnable()),
        "remote": _remote(),
        "scratch_uri": "s3://bucket/scratch",
    }


class TrackingExecutor(ExecutorBase):
    calls: list[str] = []

    def start(self, **kwargs):
        TrackingExecutor.calls.append("start")
        return {"status": "running", "error": None, "state": {"token": kwargs["execution_id"]}}

    def poll(self, **kwargs):
        TrackingExecutor.calls.append("poll")
        return {"status": "running", "error": None, "state": kwargs["state"]}

    def cancel(self, **kwargs):
        TrackingExecutor.calls.append("cancel")
        return {"status": "cancelled", "error": None}


def test_contrib_exec_base_001__handle_routes_missing_state_to_start():
    TrackingExecutor.calls = []
    result = TrackingExecutor.handle(**_adapter_envelope(state=None, cancel_requested_by=None))
    assert TrackingExecutor.calls == ["start"]
    assert result["status"] == "running"


def test_contrib_exec_base_002__handle_routes_existing_state_to_poll():
    TrackingExecutor.calls = []
    result = TrackingExecutor.handle(**_adapter_envelope(state={"existing": True}, cancel_requested_by=None))
    assert TrackingExecutor.calls == ["poll"]
    assert result["state"] == {"existing": True}


def test_contrib_exec_base_003__cancel_pending_only_routes_to_cancel_when_state_exists():
    TrackingExecutor.calls = []
    result = TrackingExecutor.handle(
        **_adapter_envelope(state={"existing": True}, cancel_requested_by="alice@example.com")
    )
    assert TrackingExecutor.calls == ["cancel"]
    assert result == {"status": "cancelled", "error": None}
