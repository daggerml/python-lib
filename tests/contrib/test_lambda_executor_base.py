from __future__ import annotations

from typing import Any, ClassVar

import pytest

from daggerml import Runnable, Uri
from daggerml.contrib.executor_state import LocalState
from daggerml.contrib.executors._lambda import LambdaExecutorBase


class _Executor(LambdaExecutorBase):
    name = "lambda-test"
    state_class = LocalState

    @staticmethod
    def resolve_runnable(uri, kwargs, sub):
        return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-lambda-adapter")

    start_calls: ClassVar[list[dict[str, Any]]] = []
    poll_calls: ClassVar[int] = 0
    gc_calls: ClassVar[int] = 0

    @classmethod
    def start(cls, *, runnable, argv_ptr, cache_key, remote, state=None):
        cls.start_calls.append({"runnable": runnable, "argv_ptr": argv_ptr, "cache_key": cache_key, "remote": remote})
        return {"status": "running", "error": None}

    @classmethod
    def poll(cls, *, state=None):
        cls.poll_calls += 1
        return {"status": "succeeded", "error": None}

    @classmethod
    def gc(cls, *, state=None):
        cls.gc_calls += 1
        return None


@pytest.fixture(autouse=True)
def _reset(monkeypatch, tmp_path):
    monkeypatch.setenv("DML_FN_CACHE_DIR", str(tmp_path))
    _Executor.start_calls = []
    _Executor.poll_calls = 0
    _Executor.gc_calls = 0


def _payload(*, cache_key: str) -> dict[str, Any]:
    runnable = Runnable(target=Uri("lambda-test"), kwargs={"x": 1}, adapter="dml-lambda-adapter")
    return {
        "runnable": runnable,
        "argv_ptr": "argv://ptr",
        "cache_key": cache_key,
        "remote": {"root": "s3://bucket/root", "cache": "cache"},
    }


def test_lambda_executor_handler_starts_with_empty_state():
    event = _payload(cache_key="lambda-start")

    result = _Executor.handler(event, None)

    assert result == {"status": "running", "error": None}
    assert len(_Executor.start_calls) == 1
    assert _Executor.poll_calls == 0
    assert _Executor.gc_calls == 0


def test_lambda_executor_handler_polls_existing_state():
    cache_key = "lambda-poll"
    with LocalState(cache_key).lock() as state:
        assert state is not None
        state.put_if_absent(state.init_record(status="running", error=None))

    result = _Executor.handler(_payload(cache_key=cache_key), None)

    assert result == {"status": "succeeded", "error": None}
    assert len(_Executor.start_calls) == 0
    assert _Executor.poll_calls == 1
    assert _Executor.gc_calls == 1


def test_lambda_executor_handler_returns_terminal_cached_state():
    cache_key = "lambda-terminal"
    with LocalState(cache_key).lock() as state:
        assert state is not None
        state.put_if_absent(state.init_record(status="failed", error="boom"))

    result = _Executor.handler(_payload(cache_key=cache_key), None)

    assert result == {"status": "failed", "error": "boom"}
    assert len(_Executor.start_calls) == 0
    assert _Executor.poll_calls == 0
    assert _Executor.gc_calls == 1
