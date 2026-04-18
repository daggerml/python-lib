from __future__ import annotations

import pytest

from daggerml import Uri
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.executor_state import ExecutionState
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


def _sub() -> Runnable:
    return Runnable(target=Uri("script"), adapter="dml-local-adapter", kwargs={"x": 1})


def _set_child_state(cache_key: str, *, status: str, dag_id: str | None = None, error: str | None = None) -> None:
    state = ExecutionState(cache_key)
    record = state.get()
    assert record is not None
    if record["status"] == "pending":
        assert state.claim_running()
    assert state.lock()
    try:
        if status == "succeeded":
            assert dag_id is not None
            assert state.mark_succeeded(dag_id)
            return
        if status == "failed":
            assert error is not None
            assert state.mark_failed(error)
            return
        raise AssertionError(f"unsupported child status: {status}")
    finally:
        state.unlock()


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
    runnable = BatchExecutor.resolve_runnable(
        "batch", {"lambda_uri": "lambda-fn", "image": Uri("repo/image:tag")}, _sub()
    )

    cache_key = "batch-start"
    argv_ptr = "argv://ptr"
    ExecutionState.upsert(cache_key, argv_ptr)
    assert ExecutionState(cache_key).claim_running()

    executor = BatchExecutor()
    record = ExecutionState(cache_key).get()
    assert record is not None
    executor.start(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        remote={"root": "s3://bucket/root"},
        state=record,
    )

    final = ExecutionState(cache_key).get()
    assert final is not None
    assert fake_client.registered
    assert fake_client.submitted == [
        {"jobName": "dml-batch-batch-start", "jobQueue": "cpu-q", "jobDefinition": "arn:batch:def/123"}
    ]
    assert final["status"] == "running"
    assert final["metadata"]["batch"]["child_cache_key"] == "batch-start:batch-child"
    assert final["metadata"]["batch"]["job_id"] == "job-123"


def test_batch_executor_poll_projects_child_success(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "SUCCEEDED"}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    cache_key = "batch-poll"
    ExecutionState.upsert(cache_key, "argv://ptr")
    ExecutionState.upsert("batch-poll:batch-child", "argv://ptr")
    _set_child_state("batch-poll:batch-child", status="succeeded", dag_id="a" * 64)
    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.update_metadata(
        {
            "batch": {
                "child_cache_key": "batch-poll:batch-child",
                "job_id": "job-123",
                "job_definition": "arn:batch:def/123",
            }
        }
    )
    es.unlock()

    record = es.get()
    assert record is not None
    executor = BatchExecutor()
    executor.poll(cache_key=cache_key, state=record)

    final = es.get()
    assert final is not None
    assert final["status"] == "succeeded"
    assert final["dag_id"] == "a" * 64


def test_batch_executor_poll_marks_failed_when_success_has_no_child_dag(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "SUCCEEDED"}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    cache_key = "batch-missing-dag"
    ExecutionState.upsert(cache_key, "argv://ptr")
    ExecutionState.upsert("batch-missing-dag:batch-child", "argv://ptr")
    _set_child_state("batch-missing-dag:batch-child", status="succeeded", dag_id="")
    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.update_metadata(
        {
            "batch": {
                "child_cache_key": "batch-missing-dag:batch-child",
                "job_id": "job-123",
                "job_definition": "arn:batch:def/123",
            }
        }
    )
    es.unlock()

    record = es.get()
    assert record is not None
    executor = BatchExecutor()
    executor.poll(cache_key=cache_key, state=record)

    final = es.get()
    assert final is not None
    assert final["status"] == "failed"
    assert final["error"] == "Batch nested execution succeeded without dag_id"


def test_batch_executor_poll_reads_batch_failure_reason(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "FAILED", "statusReason": "boom", "attempts": []}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    cache_key = "batch-fail"
    ExecutionState.upsert(cache_key, "argv://ptr")
    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.update_metadata(
        {
            "batch": {
                "child_cache_key": "batch-fail:batch-child",
                "job_id": "job-123",
                "job_definition": "arn:batch:def/123",
            }
        }
    )
    es.unlock()

    record = es.get()
    assert record is not None
    executor = BatchExecutor()
    executor.poll(cache_key=cache_key, state=record)

    final = es.get()
    assert final is not None
    assert final["status"] == "failed"
    assert final["error"] is not None
    assert "Batch job job-123 failed: boom" in final["error"]


def test_batch_executor_poll_projects_child_failure(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "FAILED", "statusReason": "boom", "attempts": []}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    cache_key = "batch-child-fail"
    ExecutionState.upsert(cache_key, "argv://ptr")
    ExecutionState.upsert("batch-child-fail:batch-child", "argv://ptr")
    _set_child_state("batch-child-fail:batch-child", status="failed", error="child boom")
    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.update_metadata(
        {
            "batch": {
                "child_cache_key": "batch-child-fail:batch-child",
                "job_id": "job-123",
                "job_definition": "arn:batch:def/123",
            }
        }
    )
    es.unlock()

    record = es.get()
    assert record is not None
    executor = BatchExecutor()
    executor.poll(cache_key=cache_key, state=record)

    final = es.get()
    assert final is not None
    assert final["status"] == "failed"
    assert final["error"] == "child boom"


def test_batch_executor_cleanup_terminates_job(monkeypatch):
    fake_client = _FakeBatchClient()
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    cache_key = "batch-gc"
    ExecutionState.upsert(cache_key, "argv://ptr")
    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.update_metadata(
        {
            "batch": {
                "child_cache_key": "batch-gc:batch-child",
                "job_id": "job-123",
                "job_definition": "arn:batch:def/123",
            }
        }
    )
    es.unlock()

    record = es.get()
    assert record is not None
    executor = BatchExecutor()
    executor.cleanup(cache_key=cache_key, state=record)

    assert fake_client.terminated == [{"jobId": "job-123", "reason": "killed"}]
