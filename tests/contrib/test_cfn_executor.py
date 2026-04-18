from __future__ import annotations

from contextlib import contextmanager
from typing import cast

import pytest

from daggerml import Runnable, Uri
from daggerml.contrib.executor_state import ExecutionRecord
from daggerml.contrib.executors.cfn import CfnExecutor


class _FakeDag:
    pass


class _FakeRef:
    def __init__(self, dag_id):
        self._dag_id = dag_id

    def id(self):
        return self._dag_id


class _FakeDml:
    def __init__(self, dag, calls):
        self._dag = dag
        self._calls = calls

    @contextmanager
    def new(self, *, argv_ptr):
        self._calls.append(("new", argv_ptr))
        yield self._dag


def test_cfn_tmpdag_is_context_manager(monkeypatch):
    dag = _FakeDag()
    calls = []

    @contextmanager
    def _temporary():
        calls.append(("temporary", None))
        yield _FakeDml(dag, calls)

    class _FakeDmlApi:
        temporary = staticmethod(_temporary)

    monkeypatch.setattr("daggerml.contrib.executors.cfn.Dml", _FakeDmlApi)

    with CfnExecutor._tmpdag("argv://ptr") as result:
        assert result is dag

    assert calls == [("temporary", None), ("new", "argv://ptr")]


def test_cfn_tmpdag_propagates_setup_errors(monkeypatch):
    @contextmanager
    def _temporary():
        raise RuntimeError("boom")
        yield

    class _FakeDmlApi:
        temporary = staticmethod(_temporary)

    monkeypatch.setattr("daggerml.contrib.executors.cfn.Dml", _FakeDmlApi)

    with pytest.raises(RuntimeError, match="boom"):
        with CfnExecutor._tmpdag("argv://ptr"):
            pass


class _ArgvValue:
    def __init__(self, values):
        self._values = values

    def __getitem__(self, item):
        return _ArgvValue(self._values[item])

    def value(self):
        return self._values


class _ArgvDag:
    def __init__(self, values):
        self.argv = _ArgvValue(values)


class _StartDml:
    def __init__(self, dag):
        self._dag = dag

    @contextmanager
    def new(self, *, argv_ptr):
        yield self._dag


class _FakeExecutionState:
    record = {"status": "pending", "metadata": {}}
    instances = []

    def __init__(self, cache_key):
        self.cache_key = cache_key
        self.locked = False
        type(self).instances.append(self)

    @classmethod
    def reset(cls):
        cls.record = {"status": "pending", "metadata": {}}
        cls.instances = []

    def lock(self):
        self.locked = True
        return True

    def unlock(self):
        self.locked = False
        return True

    def claim_running(self):
        type(self).record["status"] = "running"
        return True

    def mark_succeeded(self, dag_id):
        type(self).record["status"] = "succeeded"
        type(self).record["dag_id"] = dag_id
        return True

    def mark_failed(self, error):
        type(self).record["status"] = "failed"
        type(self).record["error"] = error
        return True

    def update_metadata(self, data):
        type(self).record["metadata"].update(data)
        return True

    def get(self):
        return {"status": type(self).record["status"], "metadata": dict(type(self).record["metadata"])}


def test_cfn_start_uses_existing_stack_id_on_no_update(monkeypatch):
    dag = _ArgvDag((None, "stack-name", {"Resources": {}}, {"Param": "Value"}))

    @contextmanager
    def _temporary():
        yield _StartDml(dag)

    class _FakeDmlApi:
        temporary = staticmethod(_temporary)

    class _FakeClient:
        def describe_stacks(self, *, StackName):
            assert StackName == "stack-name"
            return {"Stacks": [{"StackId": "stack-123"}]}

        def update_stack(self, **kwargs):
            assert kwargs["StackName"] == "stack-name"
            raise Exception("No updates are to be performed")

    _FakeExecutionState.reset()
    _FakeExecutionState.claim_running(_FakeExecutionState("cache-key"))
    poll_calls = []

    def _poll(self, *, cache_key, state):
        poll_calls.append({"cache_key": cache_key, "state": state})

    monkeypatch.setattr("daggerml.contrib.executors.cfn.Dml", _FakeDmlApi)
    monkeypatch.setattr("daggerml.contrib.executors.cfn.ExecutionState", _FakeExecutionState)
    monkeypatch.setattr(CfnExecutor, "_client", staticmethod(lambda: _FakeClient()))
    monkeypatch.setattr(CfnExecutor, "poll", _poll)

    CfnExecutor().start(
        cache_key="cache-key",
        state=cast(
            ExecutionRecord,
            {
                "cache_key": "cache-key",
                "argv_ptr": "argv://ptr",
                "status": "running",
                "lock_token": None,
                "lock_expires_ts": None,
                "dag_id": None,
                "error": None,
                "heartbeat_ts": None,
                "metadata": {},
                "updated_ts": 0.0,
            },
        ),
        runnable=Runnable(target=Uri("cfn"), kwargs={}, adapter="local"),
        argv_ptr="argv://ptr",
        remote={},
    )

    assert _FakeExecutionState.record == {
        "status": "running",
        "metadata": {"cfn": {"stack_name": "stack-name", "stack_id": "stack-123", "argv_ptr": "argv://ptr"}},
    }
    assert poll_calls == [
        {
            "cache_key": "cache-key",
            "state": {
                "status": "running",
                "metadata": {"cfn": {"stack_name": "stack-name", "stack_id": "stack-123", "argv_ptr": "argv://ptr"}},
            },
        }
    ]


def test_cfn_commit_dag_returns_committed_dag_id(monkeypatch):
    class _CommitDag:
        def __init__(self):
            self.values = {}
            self.ref = _FakeRef("dag-cfn-123")
            self.stack_id = None
            self.stack_name = None
            self.outputs = None
            self.committed = None

        def __setitem__(self, key, value):
            self.values[key] = value

        def commit(self, value):
            self.committed = value

    dag = _CommitDag()

    @contextmanager
    def _tmpdag(_argv_ptr):
        yield dag

    monkeypatch.setattr(CfnExecutor, "_tmpdag", classmethod(lambda cls, argv_ptr: _tmpdag(argv_ptr)))

    dag_id = CfnExecutor._commit_dag(
        {"argv_ptr": "argv://ptr", "stack_name": "stack-name"},
        {"StackId": "stack-123"},
        {"OutputA": "value-a"},
    )

    assert dag_id == "dag-cfn-123"
    assert dag.values == {"OutputA": "value-a"}
    assert dag.stack_id == "stack-123"
    assert dag.stack_name == "stack-name"
    assert dag.outputs == {"OutputA": "value-a"}
    assert dag.committed == {"OutputA": "value-a"}


def test_cfn_poll_marks_success_with_committed_dag_id(monkeypatch):
    class _FakeClient:
        def describe_stacks(self, *, StackName):
            assert StackName == "stack-name"
            return {
                "Stacks": [
                    {
                        "StackId": "stack-123",
                        "StackStatus": "CREATE_COMPLETE",
                        "Outputs": [{"OutputKey": "OutputA", "OutputValue": "value-a"}],
                    }
                ]
            }

    _FakeExecutionState.reset()
    commit_calls = []

    def _commit_dag(cls, metadata, stack, outputs):
        commit_calls.append((metadata, stack, outputs))
        return "dag-cfn-success"

    monkeypatch.setattr("daggerml.contrib.executors.cfn.ExecutionState", _FakeExecutionState)
    monkeypatch.setattr(CfnExecutor, "_client", staticmethod(lambda: _FakeClient()))
    monkeypatch.setattr(CfnExecutor, "_commit_dag", classmethod(_commit_dag))

    CfnExecutor().poll(
        cache_key="cache-key",
        state=cast(
            ExecutionRecord,
            {
                "cache_key": "cache-key",
                "argv_ptr": "argv://ptr",
                "status": "running",
                "lock_token": None,
                "lock_expires_ts": None,
                "dag_id": None,
                "error": None,
                "heartbeat_ts": None,
                "metadata": {"cfn": {"stack_name": "stack-name", "argv_ptr": "argv://ptr"}},
                "updated_ts": 0.0,
            },
        ),
    )

    assert commit_calls == [
        (
            {"stack_name": "stack-name", "argv_ptr": "argv://ptr"},
            {
                "StackId": "stack-123",
                "StackStatus": "CREATE_COMPLETE",
                "Outputs": [{"OutputKey": "OutputA", "OutputValue": "value-a"}],
            },
            {"OutputA": "value-a"},
        )
    ]
    assert _FakeExecutionState.record["status"] == "succeeded"
    assert _FakeExecutionState.record["dag_id"] == "dag-cfn-success"


def test_cfn_poll_marks_failed_when_stack_is_missing(monkeypatch):
    class _FakeClient:
        def describe_stacks(self, *, StackName):
            assert StackName == "stack-name"
            return {"Stacks": []}

    _FakeExecutionState.reset()

    monkeypatch.setattr("daggerml.contrib.executors.cfn.ExecutionState", _FakeExecutionState)
    monkeypatch.setattr(CfnExecutor, "_client", staticmethod(lambda: _FakeClient()))

    CfnExecutor().poll(
        cache_key="cache-key",
        state=cast(
            ExecutionRecord,
            {
                "cache_key": "cache-key",
                "argv_ptr": "argv://ptr",
                "status": "running",
                "lock_token": None,
                "lock_expires_ts": None,
                "dag_id": None,
                "error": None,
                "heartbeat_ts": None,
                "metadata": {"cfn": {"stack_name": "stack-name", "argv_ptr": "argv://ptr"}},
                "updated_ts": 0.0,
            },
        ),
    )

    assert _FakeExecutionState.record["status"] == "failed"
    assert _FakeExecutionState.record["error"] == "Stack not found: stack-name"


def test_cfn_poll_marks_failed_with_stack_event_reasons(monkeypatch):
    class _FakeClient:
        def describe_stacks(self, *, StackName):
            assert StackName == "stack-name"
            return {"Stacks": [{"StackId": "stack-123", "StackStatus": "ROLLBACK_COMPLETE"}]}

        def describe_stack_events(self, *, StackName):
            assert StackName == "stack-name"
            return {
                "StackEvents": [
                    {"ResourceStatusReason": "First failure"},
                    {"LogicalResourceId": "IgnoredWithoutReason"},
                    {"ResourceStatusReason": "Second failure"},
                ]
            }

    _FakeExecutionState.reset()

    monkeypatch.setattr("daggerml.contrib.executors.cfn.ExecutionState", _FakeExecutionState)
    monkeypatch.setattr(CfnExecutor, "_client", staticmethod(lambda: _FakeClient()))

    CfnExecutor().poll(
        cache_key="cache-key",
        state=cast(
            ExecutionRecord,
            {
                "cache_key": "cache-key",
                "argv_ptr": "argv://ptr",
                "status": "running",
                "lock_token": None,
                "lock_expires_ts": None,
                "dag_id": None,
                "error": None,
                "heartbeat_ts": None,
                "metadata": {"cfn": {"stack_name": "stack-name", "argv_ptr": "argv://ptr"}},
                "updated_ts": 0.0,
            },
        ),
    )

    assert _FakeExecutionState.record["status"] == "failed"
    assert (
        _FakeExecutionState.record["error"]
        == "Stack stack-name failed: ROLLBACK_COMPLETE\nFirst failure\nSecond failure"
    )
