from __future__ import annotations

import json

import pytest

from daggerml import Uri
from daggerml._internal.exec_state import ExecutionState
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.executors.batch import _ADAPTER_IO_NAME, BatchExecutor


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


_REMOTE = {"root": "s3://test-bucket/test-prefix"}


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
    fake_client = _FakeBatchClient()
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    runnable = BatchExecutor.resolve_runnable(
        "batch", {"lambda_uri": "lambda-fn", "image": Uri("repo/image:tag")}, _sub()
    )

    cache_key = "batch-start"
    execution_id = "exec-batch-start"
    argv_ptr = "argv://ptr"

    executor = BatchExecutor()
    result = executor.start(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        execution_id=execution_id,
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
    # input_uri and output_uri must NOT be in state (derived from AdapterIO)
    assert "input_uri" not in written_state
    assert "output_uri" not in written_state


def test_batch_executor_start_writes_input_payload_to_s3(monkeypatch):
    fake_client = _FakeBatchClient()
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    runnable = BatchExecutor.resolve_runnable(
        "batch", {"lambda_uri": "lambda-fn", "image": Uri("repo/image:tag")}, _sub()
    )

    cache_key = "batch-payload"
    execution_id = "exec-payload"
    remote = _REMOTE

    BatchExecutor().start(
        runnable=runnable,
        argv_ptr="argv://ptr",
        cache_key=cache_key,
        execution_id=execution_id,
        remote=remote,
    )

    exec_state = ExecutionState(cache_key, remote_root=remote["root"])
    io = exec_state.adapter_io(execution_id, _ADAPTER_IO_NAME)
    raw = exec_state._get_object_bytes(io._input_key)
    assert raw is not None
    payload = json.loads(raw[0])
    assert payload["cache_key"] == cache_key
    assert payload["execution_id"] == execution_id


def test_batch_executor_start_passes_s3_uris_to_container_command(monkeypatch):
    fake_client = _FakeBatchClient()
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    runnable = BatchExecutor.resolve_runnable(
        "batch", {"lambda_uri": "lambda-fn", "image": Uri("repo/image:tag")}, _sub()
    )

    BatchExecutor().start(
        runnable=runnable,
        argv_ptr="argv://ptr",
        cache_key="batch-cmd",
        execution_id="exec-cmd",
        remote=_REMOTE,
    )

    container_props = fake_client.registered[0]["containerProperties"]
    cmd = container_props["command"]
    # Command must include --poll, -i <s3://...>, -o <s3://...>
    assert "--poll" in cmd
    i_idx = cmd.index("-i")
    o_idx = cmd.index("-o")
    assert cmd[i_idx + 1].startswith("s3://")
    assert cmd[o_idx + 1].startswith("s3://")


def test_batch_executor_poll_returns_running_while_batch_running(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "RUNNING"}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    executor = BatchExecutor()
    result = executor.poll(
        cache_key="batch-poll",
        execution_id="exec-batch-poll",
        state={"job_id": "job-123"},
        remote=_REMOTE,
    )

    assert result["status"] == "running"
    assert result["state"]["job_id"] == "job-123"


def test_batch_executor_poll_returns_succeeded_when_batch_succeeded(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "SUCCEEDED"}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    cache_key = "batch-poll-ok"
    execution_id = "exec-batch-ok"
    remote = _REMOTE
    dag_id = "a" * 64
    sub_result = {"status": "succeeded", "error": None, "dag_id": dag_id}

    # Pre-write result to S3 via AdapterIO
    exec_state = ExecutionState(cache_key, remote_root=remote["root"])
    io = exec_state.adapter_io(execution_id, _ADAPTER_IO_NAME)
    exec_state._put_object(io._output_key, json.dumps(sub_result).encode())

    executor = BatchExecutor()
    result = executor.poll(
        cache_key=cache_key,
        execution_id=execution_id,
        state={"job_id": "job-123"},
        remote=remote,
    )

    assert result["status"] == "succeeded"
    assert result["dag_id"] == dag_id


def test_batch_executor_poll_returns_failed_when_output_absent(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "SUCCEEDED"}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    result = BatchExecutor().poll(
        cache_key="batch-no-out",
        execution_id="exec-no-out",
        state={"job_id": "job-123"},
        remote=_REMOTE,
    )

    assert result["status"] == "failed"
    assert "output not yet written" in result["error"]


def test_batch_executor_poll_reads_batch_failure_reason(monkeypatch):
    fake_client = _FakeBatchClient(jobs=[{"status": "FAILED", "statusReason": "boom", "attempts": []}])
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))
    executor = BatchExecutor()
    result = executor.poll(
        cache_key="batch-fail",
        execution_id="exec-batch-fail",
        state={"job_id": "job-123"},
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


def test_batch_executor_cancel_cleans_up_backend_resources(monkeypatch):
    fake_client = _FakeBatchClient()
    monkeypatch.setattr(BatchExecutor, "_client", staticmethod(lambda: fake_client))

    result = BatchExecutor().cancel(
        cache_key="batch-cancel",
        execution_id="exec-batch-cancel",
        state={"job_id": "job-123", "job_definition": "arn:batch:def/123"},
        remote=_REMOTE,
    )

    assert result == {"status": "cancel-detached", "error": None}
    assert fake_client.canceled == [{"jobId": "job-123", "reason": "daggerml cancellation requested"}]
    assert fake_client.deregistered == [{"jobDefinition": "arn:batch:def/123"}]
