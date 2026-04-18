from __future__ import annotations

from typing import Any, ClassVar

import pytest

from daggerml import Runnable, Uri
from daggerml.contrib.executor_state import ExecutionState
from daggerml.contrib.executors._lambda import LambdaExecutorBase


class _Executor(LambdaExecutorBase):
    name = "lambda-test"

    start_calls: ClassVar[list[dict[str, Any]]] = []
    cleanup_calls: ClassVar[int] = 0

    def start(self, *, cache_key, state, runnable, argv_ptr, remote):
        _Executor.start_calls.append(
            {"runnable": runnable, "argv_ptr": argv_ptr, "cache_key": cache_key, "remote": remote}
        )
        # Mark running in state
        es = ExecutionState(cache_key)
        assert es.lock()
        try:
            es.mark_running()
        finally:
            es.unlock()

    def poll(self, *, cache_key, state):
        # Mark succeeded
        es = ExecutionState(cache_key)
        if es.lock():
            try:
                es.mark_succeeded("dag-id")
            finally:
                es.unlock()

    def cleanup(self, *, cache_key, state):
        _Executor.cleanup_calls += 1

    @staticmethod
    def resolve_runnable(uri, kwargs, sub):
        return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-lambda-adapter")


class _FailingStartExecutor(LambdaExecutorBase):
    name = "lambda-test-failing-start"

    def start(self, *, cache_key, state, runnable, argv_ptr, remote):
        raise RuntimeError("boom")

    def cleanup(self, *, cache_key, state):
        return None

    @staticmethod
    def resolve_runnable(uri, kwargs, sub):
        return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-lambda-adapter")


@pytest.fixture(autouse=True)
def _reset():
    _Executor.start_calls = []
    _Executor.cleanup_calls = 0


def _payload(*, cache_key: str) -> dict[str, Any]:
    runnable = Runnable(target=Uri("lambda-test"), kwargs={"x": 1}, adapter="dml-lambda-adapter")
    return {
        "runnable": runnable,
        "argv_ptr": "argv://ptr",
        "cache_key": cache_key,
        "remote": {"root": "s3://bucket/root"},
    }


def test_lambda_executor_handler_starts_with_pending_state():
    cache_key = "lambda-start"
    ExecutionState.upsert(cache_key, "argv://ptr")
    event = _payload(cache_key=cache_key)

    result = _Executor.handler(event, None)

    assert result["status"] == "running"
    assert len(_Executor.start_calls) == 1
    assert _Executor.cleanup_calls == 0


def test_lambda_executor_handler_polls_running_state():
    cache_key = "lambda-poll"
    ExecutionState.upsert(cache_key, "argv://ptr")
    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.unlock()

    result = _Executor.handler(_payload(cache_key=cache_key), None)

    # poll marks succeeded, then handle does cleanup in the same call
    assert result["status"] == "succeeded"
    assert len(_Executor.start_calls) == 0
    assert _Executor.cleanup_calls == 1


def test_lambda_executor_handler_returns_failed_for_terminal_state():
    cache_key = "lambda-terminal"
    ExecutionState.upsert(cache_key, "argv://ptr")
    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.mark_failed("boom")
    es.unlock()

    result = _Executor.handler(_payload(cache_key=cache_key), None)

    # failed -> cleanup, but canonical failed response
    assert result["status"] == "failed"
    assert len(_Executor.start_calls) == 0
    assert _Executor.cleanup_calls == 1


def test_lambda_executor_handler_persists_failed_state_on_exception():
    cache_key = "lambda-handler-failure"
    ExecutionState.upsert(cache_key, "argv://ptr")

    result = _FailingStartExecutor.handler(_payload(cache_key=cache_key), None)

    state = ExecutionState(cache_key).get()
    assert result["status"] == "failed"
    assert state is not None
    assert state["status"] == "failed"
    assert state["error"] == result["error"]
