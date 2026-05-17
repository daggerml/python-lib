from __future__ import annotations

import pytest

from daggerml._internal.types import Runnable, Uri
from daggerml.contrib.executors._base import ExecutorBase


def _runnable() -> Runnable:
    return Runnable(target=Uri("test"), kwargs={}, adapter="test-adapter")


def _remote() -> dict[str, str]:
    return {"root": "s3://test-bucket/test-prefix"}


class MockExecutor(ExecutorBase):
    name = "mock"
    adapter = "local"
    calls: list[str] = []

    def start(self, *, cache_key, execution_id, runnable, argv_ptr, remote):
        MockExecutor.calls.append("start")
        return {"status": "running", "error": None, "state": {"token": execution_id}}

    def poll(self, *, cache_key, execution_id, state, remote):
        MockExecutor.calls.append("poll")
        return {"status": "running", "error": None, "state": state}


class TerminalStartExecutor(MockExecutor):
    def start(self, *, cache_key, execution_id, runnable, argv_ptr, remote):
        MockExecutor.calls.append("start")
        return {"status": "succeeded", "error": None, "dag_id": "a" * 64}


class CancelExecutor(MockExecutor):
    def cancel(self, *, cache_key, execution_id, state, remote):
        MockExecutor.calls.append("cancel")
        return {"status": "cancel-detached", "error": None}


@pytest.fixture(autouse=True)
def reset_calls():
    MockExecutor.calls = []
    yield


@pytest.mark.parametrize(
    "contract_id,state,expected_start_calls,expected_poll_calls,stage",
    [
        pytest.param("EXB-HDL-001", None, 1, 0, "kickoff", id="EXB-HDL-001:kickoff-uses-start"),
        pytest.param("EXB-HDL-002", {"some": "state"}, 0, 1, "resume", id="EXB-HDL-002:resume-uses-poll"),
    ],
)
def test_executor_base_handle_lifecycle_stage_matrix_EXB_HDL_001_EXB_HDL_002(
    contract_id, state, expected_start_calls, expected_poll_calls, stage
):
    del contract_id, stage
    result = MockExecutor.handle(
        cache_key="ck-stage",
        execution_id="exec-stage",
        state=state,
        execution_status=None,
        cancel_requested_by=None,
        runnable=_runnable(),
        argv_ptr="ptr",
        remote=_remote(),
    )
    assert MockExecutor.calls.count("start") == expected_start_calls
    assert MockExecutor.calls.count("poll") == expected_poll_calls
    assert result["status"] == "running"


def test_executor_base_handle_EXB_HDL_003_returns_terminal_start_result_directly():
    result = TerminalStartExecutor.handle(
        cache_key="ck-terminal",
        execution_id="exec-terminal",
        state=None,
        execution_status=None,
        cancel_requested_by=None,
        runnable=_runnable(),
        argv_ptr="ptr",
        remote=_remote(),
    )
    assert MockExecutor.calls.count("start") == 1
    assert result["status"] == "succeeded"
    assert result.get("dag_id") == "a" * 64


def test_executor_base_handle_EXB_HDL_004_routes_mixed_state_invocations_correctly():
    class TrackingExecutor(ExecutorBase):
        name = "tracking"
        adapter = "local"

        def start(self, *, cache_key, execution_id, runnable, argv_ptr, remote):
            MockExecutor.calls.append("start")
            return {"status": "running", "error": None, "state": {"token": execution_id}}

        def poll(self, *, cache_key, execution_id, state, remote):
            MockExecutor.calls.append("poll")
            return {"status": "running", "error": None, "state": state}

    kickoff = TrackingExecutor.handle(
        cache_key="ck-mixed",
        execution_id="exec-mixed",
        state=None,
        execution_status=None,
        cancel_requested_by=None,
        runnable=_runnable(),
        argv_ptr="ptr",
        remote=_remote(),
    )
    resumed = TrackingExecutor.handle(
        cache_key="ck-mixed",
        execution_id="exec-mixed",
        state={"existing": "state"},
        execution_status=None,
        cancel_requested_by=None,
        runnable=_runnable(),
        argv_ptr="ptr",
        remote=_remote(),
    )
    assert MockExecutor.calls.count("start") == 1
    assert MockExecutor.calls.count("poll") == 1
    assert kickoff["status"] == "running"
    assert resumed["status"] == "running"


def test_executor_base_handle_routes_cancel_requested_updates_to_cancel():
    result = CancelExecutor.handle(
        cache_key="ck-cancel",
        execution_id="exec-cancel",
        state={"pid": 1},
        execution_status="cancel-pending",
        cancel_requested_by="alice@example.com",
        runnable=_runnable(),
        argv_ptr="ptr",
        remote=_remote(),
    )
    assert MockExecutor.calls == ["cancel"]
    assert result == {"status": "cancel-detached", "error": None}
