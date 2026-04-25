from __future__ import annotations

import pytest

from daggerml import Uri
from daggerml._internal.types import DmlRepoError, Runnable
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
    monkeypatch.setenv("DML_TEST_FN_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("CPU_QUEUE", "cpu-q")
    monkeypatch.setenv("GPU_QUEUE", "gpu-q")
    monkeypatch.setenv("BATCH_TASK_ROLE_ARN", "arn:role/batch")


def _sub() -> Runnable:
    return Runnable(target=Uri("script"), adapter="dml-local-adapter", kwargs={"x": 1})


_REMOTE = {"root": "s3://bucket/root"}


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


def test_batch_executor_start_submits_job_and_writes_state(monkeypatch):
    fake_store = _FakeStore()
    fake_client = _FakeBatchClient()
    monkeypatch.setattr(BatchExecutor, "_store", staticmethod(lambda remote: fake_store))
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    runnable = BatchExecutor.resolve_runnable(
        "batch", {"lambda_uri": "lambda-fn", "image": Uri("repo/image:tag")}, _sub()
    )

    cache_key = "batch-start"
    argv_ptr = "argv://ptr"

    executor = BatchExecutor()
    result = executor.start(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        execution_id="exec-batch-start",
        remote=_REMOTE,
    )

    assert result["status"] == "running"
    written_state = result["state"]
    assert fake_client.registered
    assert fake_client.submitted == [
        {"jobName": "dml-batch-batch-start", "jobQueue": "cpu-q", "jobDefinition": "arn:batch:def/123"}
    ]
    assert written_state["job_id"] == "job-123"
    assert written_state["job_definition"] == "arn:batch:def/123"


def test_batch_executor_poll_returns_running_while_batch_running(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "RUNNING"}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    executor = BatchExecutor()
    result = executor.poll(
        cache_key="batch-poll",
        execution_id="exec-batch-poll",
        state={"job_id": "job-123", "output_uri": "s3://bucket/out.json"},
        remote=_REMOTE,
    )

    assert result == {
        "status": "running",
        "error": None,
        "state": {"job_id": "job-123", "output_uri": "s3://bucket/out.json"},
    }


def test_batch_executor_poll_returns_succeeded_when_batch_succeeded(monkeypatch):
    import json as _json

    fake_client = _FakeBatchClient(jobs=[{"status": "SUCCEEDED"}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))
    dag_id = "a" * 64
    sub_result = {"status": "succeeded", "error": None, "dag_id": dag_id}

    class _FakeS3Store:
        def get(self, uri):
            return _json.dumps(sub_result).encode()

    from daggerml.contrib import s3 as s3_mod

    monkeypatch.setattr(s3_mod.S3Store, "from_remote_root", staticmethod(lambda root: _FakeS3Store()))

    executor = BatchExecutor()
    result = executor.poll(
        cache_key="batch-poll-ok",
        execution_id="exec-batch-ok",
        state={"job_id": "job-123", "output_uri": "s3://bucket/out.json"},
        remote=_REMOTE,
    )

    assert result["status"] == "succeeded"
    assert result["dag_id"] == dag_id


def test_batch_executor_poll_reads_batch_failure_reason(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "FAILED", "statusReason": "boom", "attempts": []}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))
    executor = BatchExecutor()
    result = executor.poll(
        cache_key="batch-fail",
        execution_id="exec-batch-fail",
        state={"job_id": "job-123", "output_uri": "s3://bucket/out.json"},
        remote=_REMOTE,
    )

    assert result["status"] == "failed"
    assert result["error"] is not None
    assert "Batch job job-123 failed: boom" in result["error"]


def test_batch_executor_poll_returns_failed_for_missing_job_id():
    executor = BatchExecutor()
    result = executor.poll(
        cache_key="batch-no-id",
        execution_id="exec-batch-no-id",
        state={},
        remote=_REMOTE,
    )

    assert result["status"] == "failed"
    assert "job_id" in result["error"]
