from __future__ import annotations

from contextlib import contextmanager

import pytest

from daggerml import Runnable, Uri
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


_REMOTE = {"root": "s3://bucket/root"}


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

    poll_calls = []

    def _poll(self, *, cache_key, execution_id, state, remote):
        poll_calls.append({"cache_key": cache_key, "execution_id": execution_id, "state": state})
        return {"status": "running", "error": None, "state": state}

    monkeypatch.setattr("daggerml.contrib.executors.cfn.Dml", _FakeDmlApi)
    monkeypatch.setattr(CfnExecutor, "_client", staticmethod(lambda: _FakeClient()))
    monkeypatch.setattr(CfnExecutor, "poll", _poll)

    CfnExecutor().start(
        cache_key="cache-key",
        execution_id="exec-cfn-start",
        runnable=Runnable(target=Uri("cfn"), kwargs={}, adapter="local"),
        argv_ptr="argv://ptr",
        remote=_REMOTE,
    )

    assert len(poll_calls) == 1
    assert poll_calls[0]["cache_key"] == "cache-key"
    assert poll_calls[0]["execution_id"] == "exec-cfn-start"
    assert poll_calls[0]["state"] == {"stack_name": "stack-name", "stack_id": "stack-123", "argv_ptr": "argv://ptr"}


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

    commit_calls = []

    def _commit_dag(cls, metadata, stack, outputs):
        commit_calls.append((metadata, stack, outputs))
        return "dag-cfn-success"

    monkeypatch.setattr(CfnExecutor, "_client", staticmethod(lambda: _FakeClient()))
    monkeypatch.setattr(CfnExecutor, "_commit_dag", classmethod(_commit_dag))
    result = CfnExecutor().poll(
        cache_key="cache-key",
        execution_id="exec-cfn-success",
        state={"stack_name": "stack-name", "argv_ptr": "argv://ptr"},
        remote=_REMOTE,
    )

    assert result["status"] == "succeeded"
    assert result["dag_id"] == "dag-cfn-success"
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


def test_cfn_poll_marks_failed_when_stack_is_missing(monkeypatch):
    class _FakeClient:
        def describe_stacks(self, *, StackName):
            assert StackName == "stack-name"
            return {"Stacks": []}

    monkeypatch.setattr(CfnExecutor, "_client", staticmethod(lambda: _FakeClient()))
    result = CfnExecutor().poll(
        cache_key="cache-key",
        execution_id="exec-cfn-missing",
        state={"stack_name": "stack-name", "argv_ptr": "argv://ptr"},
        remote=_REMOTE,
    )

    assert result["status"] == "failed"
    assert result["error"] == "Stack not found: stack-name"


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

    monkeypatch.setattr(CfnExecutor, "_client", staticmethod(lambda: _FakeClient()))
    result = CfnExecutor().poll(
        cache_key="cache-key",
        execution_id="exec-cfn-failed",
        state={"stack_name": "stack-name", "argv_ptr": "argv://ptr"},
        remote=_REMOTE,
    )

    assert result["status"] == "failed"
    assert result["error"] == "Stack stack-name failed: ROLLBACK_COMPLETE\nFirst failure\nSecond failure"
