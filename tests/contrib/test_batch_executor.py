from __future__ import annotations

from typing import Any, cast

import pytest

from daggerml import Uri
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.executor_state import LocalState
from daggerml.contrib.executors.batch import BatchExecutor


class _FakeStore:
    def __init__(self):
        self.writes: dict[str, bytes] = {}

    def put(self, data=None, filepath=None, *, suffix=""):
        assert filepath is None
        assert isinstance(data, bytes)
        uri = f"s3://bucket/input{suffix}"
        self.writes[uri] = data
        return Uri(uri)

    def _name2uri(self, name):
        return Uri(f"s3://bucket/{name}")


class _FakeBatchClient:
    def __init__(self, *, jobs=None):
        self.jobs = jobs or [{"status": "RUNNING"}]
        self.registered = []
        self.submitted = []
        self.terminated = []
        self.canceled = []
        self.deregistered = []

    def register_job_definition(self, **kwargs):
        self.registered.append(kwargs)
        return {"jobDefinitionArn": "arn:batch:def/123"}

    def submit_job(self, **kwargs):
        self.submitted.append(kwargs)
        return {"jobId": "job-123"}

    def describe_jobs(self, **kwargs):
        return {"jobs": self.jobs}

    def terminate_job(self, **kwargs):
        self.terminated.append(kwargs)

    def cancel_job(self, **kwargs):
        self.canceled.append(kwargs)

    def deregister_job_definition(self, **kwargs):
        self.deregistered.append(kwargs)


@pytest.fixture(autouse=True)
def _setup(monkeypatch, tmp_path):
    monkeypatch.setenv("DML_FN_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("CPU_QUEUE", "cpu-q")
    monkeypatch.setenv("GPU_QUEUE", "gpu-q")
    monkeypatch.setenv("BATCH_TASK_ROLE_ARN", "arn:role/batch")
    monkeypatch.setattr(BatchExecutor, "state_class", LocalState)


def _sub() -> Runnable:
    return Runnable(target=Uri("script"), adapter="dml-local-adapter", kwargs={"x": 1})


def test_batch_executor_resolve_runnable_shape():
    runnable = BatchExecutor.resolve_runnable(
        "batch",
        {"lambda_uri": "lambda-fn", "image": Uri("repo/image:tag"), "cpu": 2, "memory": 2048, "gpu": 1},
        _sub(),
    )

    assert runnable.target.uri == "lambda-fn"
    assert runnable.adapter == "dml-lambda-adapter"
    assert runnable.kwargs == {"image": Uri("repo/image:tag"), "cpu": 2, "memory": 2048, "gpu": 1}
    assert runnable.sub is not None


def test_batch_executor_resolve_runnable_rejects_bad_input():
    with pytest.raises(DmlRepoError, match="requires sub runnable"):
        BatchExecutor.resolve_runnable("batch", {"lambda_uri": "lambda-fn", "image": Uri("img")}, None)
    with pytest.raises(DmlRepoError, match="Unknown batch executor kwargs"):
        BatchExecutor.resolve_runnable("batch", {"lambda_uri": "lambda-fn", "image": Uri("img"), "oops": 1}, _sub())
    with pytest.raises(DmlRepoError, match="image must be a Uri"):
        BatchExecutor.resolve_runnable("batch", {"lambda_uri": "lambda-fn", "image": "img"}, _sub())


def test_batch_executor_start_submits_job_and_records_state(monkeypatch):
    fake_store = _FakeStore()
    fake_client = _FakeBatchClient()
    monkeypatch.setattr(BatchExecutor, "_store", staticmethod(lambda remote: fake_store))
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))
    monkeypatch.setattr(
        BatchExecutor,
        "_child_comms",
        staticmethod(lambda state: {"kind": "dynamo", "spec": {"table_name": "test-table"}}),
    )
    runnable = BatchExecutor.resolve_runnable(
        "batch", {"lambda_uri": "lambda-fn", "image": Uri("repo/image:tag")}, _sub()
    )

    with LocalState("batch-start").lock() as state:
        assert state is not None
        result = BatchExecutor.start(
            runnable=runnable,
            argv_ptr="argv://ptr",
            cache_key="batch-start",
            remote={"root": "s3://bucket/root", "cache": "cache-key"},
            state=state,
        )
        record = cast(dict[str, Any], state.get())

    assert result == {"status": "pending", "error": None}
    assert fake_client.registered
    assert fake_client.submitted == [
        {"jobName": "dml-batch-batch-start", "jobQueue": "cpu-q", "jobDefinition": "arn:batch:def/123"}
    ]
    assert record["status"] == "pending"
    assert record["metadata"]["batch"]["job_id"] == "job-123"


def test_batch_executor_start_rejects_non_dynamo_state(monkeypatch):
    fake_store = _FakeStore()
    fake_client = _FakeBatchClient()
    monkeypatch.setattr(BatchExecutor, "_store", staticmethod(lambda remote: fake_store))
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))
    runnable = BatchExecutor.resolve_runnable(
        "batch", {"lambda_uri": "lambda-fn", "image": Uri("repo/image:tag")}, _sub()
    )

    with LocalState("batch-start-bad").lock() as state:
        assert state is not None
        with pytest.raises(DmlRepoError, match="requires dynamo state backend"):
            BatchExecutor.start(
                runnable=runnable,
                argv_ptr="argv://ptr",
                cache_key="batch-start-bad",
                remote={"root": "s3://bucket/root", "cache": "cache-key"},
                state=state,
            )


def test_batch_executor_poll_marks_succeeded_from_batch_status(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "SUCCEEDED"}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    with LocalState("batch-poll").lock() as state:
        assert state is not None
        state.put_if_absent(state.init_record(status="pending", error=None))
        state.update(
            state.set_executor_metadata(
                executor_id="batch",
                data={
                    "job_id": "job-123",
                    "job_definition": "arn:batch:def/123",
                },
            )
        )
        result = BatchExecutor.poll(state=state)
        record = cast(dict[str, Any], state.get())

    assert result == {"status": "succeeded", "error": None}
    assert record["metadata"]["batch"]["batch_status"] == "succeeded"


def test_batch_executor_poll_reads_batch_failure_reason(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "FAILED", "statusReason": "boom", "attempts": []}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    with LocalState("batch-fail").lock() as state:
        assert state is not None
        state.put_if_absent(state.init_record(status="pending", error=None))
        state.update(
            state.set_executor_metadata(
                executor_id="batch",
                data={
                    "job_id": "job-123",
                    "job_definition": "arn:batch:def/123",
                },
            )
        )
        result = BatchExecutor.poll(state=state)

    assert result == {"status": "failed", "error": "Batch job job-123 failed: boom"}


def test_batch_executor_gc_terminates_job(monkeypatch):
    fake_client = _FakeBatchClient()
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    with LocalState("batch-gc").lock() as state:
        assert state is not None
        state.put_if_absent(state.init_record(status="running", error=None))
        state.update(
            state.set_executor_metadata(
                executor_id="batch", data={"job_id": "job-123", "job_definition": "arn:batch:def/123"}
            )
        )
        BatchExecutor.gc(state=state)

    assert fake_client.terminated == [{"jobId": "job-123", "reason": "killed"}]
