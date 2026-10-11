import json

import pytest
from botocore.exceptions import ClientError

from daggerml import Uri
from daggerml._core.aws import _get_region_from_metadata
from daggerml.api import DmlRepoError
from daggerml.contrib.executors.batch import BatchExecutor


@pytest.mark.parametrize("option", ["cpu", "memory", "gpu"])
@pytest.mark.parametrize("value", [True, False, -1, "1", 1.5])
def test_invalid_resources_fail_before_scratch_writes(monkeypatch, option, value):
    monkeypatch.setenv("CPU_QUEUE", "queue")
    monkeypatch.setenv("GPU_QUEUE", "queue")
    monkeypatch.setattr(
        "daggerml.contrib.executors.batch._write_scratch_json", lambda *a, **k: pytest.fail("unexpected write")
    )
    with pytest.raises(DmlRepoError, match=f"{option} must be an int"):
        BatchExecutor().start(
            cache_key="cache",
            execution_id="execution",
            remote={"root": "s3://bucket/root"},
            scratch_uri="s3://bucket/scratch",
            runnable={
                "sub": {"adapter": "dml-local-adapter"},
                "kwargs": {"image": Uri("image"), option: value},
            },
        )


@pytest.mark.parametrize("stage", ["terminate", "describe", "deregister"])
def test_cancellation_throttling_retains_state_and_backoff(monkeypatch, stage):
    error = ClientError(
        {
            "Error": {"Code": "ThrottlingException"},
            "ResponseMetadata": {"HTTPHeaders": {"retry-after": "2"}},
        },
        "operation",
    )

    class Client:
        def terminate_job(self, **kwargs):
            if stage == "terminate":
                raise error

        def describe_jobs(self, **kwargs):
            if stage == "describe":
                raise error
            return {"jobs": [{"status": "FAILED"}]}

        def deregister_job_definition(self, **kwargs):
            if stage == "deregister":
                raise error

    monkeypatch.setattr(BatchExecutor, "_client", lambda self: Client())
    state = {"job_id": "job", "job_definition": "definition"}
    result = BatchExecutor().cancel(
        cache_key="cache",
        execution_id="execution",
        runnable={},
        state=state,
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/scratch",
        cancel_requested_by=None,
    )
    assert result == {"status": "retry", "error": None, "state": state, "retry_after_ms": 2000}


@pytest.mark.parametrize("disabled", ["true", "TRUE", "True"])
def test_disabled_imds_region_lookup_makes_no_ec2_request(monkeypatch, disabled):
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", disabled)
    monkeypatch.delenv("ECS_CONTAINER_METADATA_URI_V4", raising=False)
    monkeypatch.delenv("ECS_CONTAINER_METADATA_URI", raising=False)
    monkeypatch.setattr("daggerml._core.aws.urllib.request.urlopen", lambda *a, **k: pytest.fail("IMDS request"))
    assert _get_region_from_metadata() is None


def test_disabled_ec2_metadata_keeps_ecs_metadata_available(monkeypatch):
    from io import BytesIO

    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", "http://ecs/task")
    calls = []

    def response(url, **kwargs):
        calls.append(url)
        return BytesIO(json.dumps({"Labels": {"com.amazonaws.ecs.cluster": "default"}}).encode())

    monkeypatch.setattr("daggerml._core.aws.urllib.request.urlopen", response)
    assert _get_region_from_metadata() is None
    assert calls == ["http://ecs/task"]


def test_missing_job_cancellation_retries_without_deregistering(monkeypatch):
    class Client:
        def terminate_job(self, **kwargs):
            pass

        def describe_jobs(self, **kwargs):
            return {"jobs": []}

        def deregister_job_definition(self, **kwargs):
            pytest.fail("deregistered without terminal observation")

    monkeypatch.setattr(BatchExecutor, "_client", lambda self: Client())
    state = {"job_id": "job", "job_definition": "definition"}
    result = BatchExecutor().cancel(
        cache_key="cache",
        execution_id="execution",
        runnable={},
        state=state,
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/scratch",
        cancel_requested_by=None,
    )
    assert result == {"status": "retry", "error": None, "state": state}


@pytest.mark.parametrize("image", [{}, {"uri": "", "extra": 1}, {"uri": ""}, {"uri": None}, {"uri": 2}, "image"])
def test_invalid_wire_image_is_rejected_before_side_effects(monkeypatch, image):
    monkeypatch.setenv("CPU_QUEUE", "queue")
    monkeypatch.setattr(
        "daggerml.contrib.executors.batch._write_scratch_json", lambda *a, **k: pytest.fail("unexpected write")
    )
    monkeypatch.setattr(BatchExecutor, "_client", lambda *a, **k: pytest.fail("unexpected client"))
    with pytest.raises(DmlRepoError, match="image"):
        BatchExecutor().start(
            cache_key="cache",
            execution_id="execution",
            remote={"root": "s3://bucket/root"},
            scratch_uri="s3://bucket/scratch",
            runnable={
                "sub": {"adapter": "dml-local-adapter"},
                "kwargs": {"image": image},
            },
        )


@pytest.mark.parametrize(
    "field,value", [("adapter", None), ("adapter", ""), ("adapter", 2), ("role", None), ("role", "")]
)
def test_invalid_launch_configuration_is_rejected_before_side_effects(monkeypatch, field, value):
    monkeypatch.setenv("CPU_QUEUE", "queue")
    monkeypatch.setenv("BATCH_TASK_ROLE_ARN", "role" if field != "role" else (str(value) if value is not None else ""))
    monkeypatch.setattr(
        "daggerml.contrib.executors.batch._write_scratch_json", lambda *a, **k: pytest.fail("unexpected write")
    )

    with pytest.raises(DmlRepoError, match="sub adapter|BATCH_TASK_ROLE_ARN"):
        BatchExecutor().start(
            cache_key="cache",
            execution_id="execution",
            remote={"root": "s3://bucket/root"},
            scratch_uri="s3://bucket/scratch",
            runnable={
                "sub": {"adapter": value if field == "adapter" else "dml-local-adapter"},
                "kwargs": {"image": {"uri": "image"}},
            },
        )
