from __future__ import annotations

from unittest.mock import patch

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


class SlowStartExecutor(MockExecutor):
    import time as _time

    def start(self, *, cache_key, execution_id, runnable, argv_ptr, remote):
        MockExecutor.calls.append("start")
        import time
        time.sleep(0.05)
        return {"status": "running", "error": None, "state": {"token": execution_id}}


@pytest.fixture(autouse=True)
def reset_calls():
    MockExecutor.calls = []
    yield


class TestHandle:
    def test_start_called_when_no_job_state(self):
        result = MockExecutor.handle(
            cache_key="ck-1", execution_id="exec-1", state=None, runnable=_runnable(), argv_ptr="ptr", remote=_remote()
        )
        assert "start" in MockExecutor.calls
        assert result["status"] == "running"

    def test_poll_called_when_job_state_exists(self):
        job_state = {"some": "state"}
        result = MockExecutor.handle(
            cache_key="ck-2",
            execution_id="exec-2",
            state=job_state,
            runnable=_runnable(),
            argv_ptr="ptr",
            remote=_remote(),
        )
        assert "poll" in MockExecutor.calls
        assert result["status"] == "running"

    def test_start_returns_terminal_result_directly(self):
        result = TerminalStartExecutor.handle(
            cache_key="ck-terminal",
            execution_id="exec-terminal",
            state=None,
            runnable=_runnable(),
            argv_ptr="ptr",
            remote=_remote(),
        )
        assert "start" in MockExecutor.calls
        assert result["status"] == "succeeded"
        assert result.get("dag_id") == "a" * 64

    def test_concurrent_calls_both_get_result(self):
        """Two concurrent handle() calls: one hits start(), other hits poll()."""
        call_count = {"start": 0, "poll": 0}

        class TrackingExecutor(ExecutorBase):
            name = "tracking"
            adapter = "local"

            def start(self, *, cache_key, execution_id, runnable, argv_ptr, remote):
                call_count["start"] += 1
                return {"status": "running", "error": None, "state": {"token": execution_id}}

            def poll(self, *, cache_key, execution_id, state, remote):
                call_count["poll"] += 1
                return {"status": "running", "error": None, "state": state}

        results = [
            TrackingExecutor.handle(
                cache_key="ck-race",
                execution_id="exec-race",
                state=None,
                runnable=_runnable(),
                argv_ptr="ptr",
                remote=_remote(),
            ),
            TrackingExecutor.handle(
                cache_key="ck-race",
                execution_id="exec-race",
                state={"existing": "state"},
                runnable=_runnable(),
                argv_ptr="ptr",
                remote=_remote(),
            ),
        ]

        assert call_count["start"] == 1
        assert call_count["poll"] == 1
        assert all(r["status"] == "running" for r in results)
