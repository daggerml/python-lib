from __future__ import annotations

from typing import Any, ClassVar

import pytest

from daggerml import Runnable, Uri
from daggerml.contrib.executors._lambda import LambdaExecutorBase

_REMOTE = {"root": "s3://bucket/root"}


class _Executor(LambdaExecutorBase):
    name = "lambda-test"

    start_calls: ClassVar[list[dict[str, Any]]] = []

    def start(self, *, cache_key, execution_id, runnable, argv_ptr, remote):
        _Executor.start_calls.append(
            {
                "runnable": runnable,
                "argv_ptr": argv_ptr,
                "cache_key": cache_key,
                "execution_id": execution_id,
                "remote": remote,
            }
        )
        return {"status": "running", "error": None, "state": {"token": execution_id}}

    def poll(self, *, cache_key, execution_id, state, remote):
        return {"status": "succeeded", "error": None, "dag_id": "d" * 64}

    @staticmethod
    def resolve_runnable(uri, kwargs, sub):
        return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-lambda-adapter")


class _FailingStartExecutor(LambdaExecutorBase):
    name = "lambda-test-failing-start"

    def start(self, *, cache_key, execution_id, runnable, argv_ptr, remote):
        raise RuntimeError("boom")

    @staticmethod
    def resolve_runnable(uri, kwargs, sub):
        return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-lambda-adapter")


@pytest.fixture(autouse=True)
def _reset():
    _Executor.start_calls = []


def _payload(*, cache_key: str) -> dict[str, Any]:
    runnable = Runnable(target=Uri("lambda-test"), kwargs={"x": 1}, adapter="dml-lambda-adapter")
    return {
        "runnable": runnable,
        "argv_ptr": "argv://ptr",
        "cache_key": cache_key,
        "execution_id": f"exec-{cache_key}",
        "remote": _REMOTE,
        "state": None,
    }


def test_lambda_executor_handler_starts_when_no_job_state(monkeypatch):
    cache_key = "lambda-start"
    result = _Executor.handler(_payload(cache_key=cache_key), None)

    assert result["status"] == "running"
    assert len(_Executor.start_calls) == 1


def test_lambda_executor_handler_polls_with_existing_job_state(monkeypatch):
    job_state = {"some": "state"}
    cache_key = "lambda-poll"
    payload = _payload(cache_key=cache_key)
    payload["state"] = job_state
    result = _Executor.handler(payload, None)

    assert result["status"] == "succeeded"
    assert len(_Executor.start_calls) == 0


def test_lambda_executor_handler_returns_failed_on_exception(monkeypatch):
    cache_key = "lambda-handler-failure"
    result = _FailingStartExecutor.handler(_payload(cache_key=cache_key), None)

    assert result["status"] == "failed"
    assert "boom" in result["error"]
