"""Moto control-plane/S3 simulation; no containers or production isolation proof."""

import json
from uuid import uuid4

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from moto.batch.models import batch_backends

from daggerml.contrib.executors.batch import BatchExecutor, _scratch_uri

pytestmark = [pytest.mark.slow, pytest.mark.serial]


@pytest.fixture(params=["us-east-1", "us-west-2"])
def batch_world(monkeypatch, request):
    region = request.param
    monkeypatch.setenv("AWS_REGION", region)
    monkeypatch.setenv("AWS_DEFAULT_REGION", region)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.delenv("MOTO_SIMPLE_BATCH_FAIL_AFTER", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    with mock_aws(config={"batch": {"use_docker": False}}):
        iam = boto3.client("iam", region_name="us-east-1")
        role = iam.create_role(
            RoleName="batch-service",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "batch.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
        )["Role"]["Arn"]
        client = boto3.client("batch", region_name=region)
        environment = client.create_compute_environment(
            computeEnvironmentName="test",
            type="UNMANAGED",
            serviceRole=role,
        )["computeEnvironmentArn"]
        queue = client.create_job_queue(
            jobQueueName="test",
            priority=1,
            state="ENABLED",
            computeEnvironmentOrder=[{"order": 1, "computeEnvironment": environment}],
        )["jobQueueArn"]
        gpu_queue = client.create_job_queue(
            jobQueueName="gpu",
            priority=1,
            state="ENABLED",
            computeEnvironmentOrder=[{"order": 1, "computeEnvironment": environment}],
        )["jobQueueArn"]
        monkeypatch.setenv("CPU_QUEUE", queue)
        monkeypatch.setenv("GPU_QUEUE", gpu_queue)
        monkeypatch.setenv("BATCH_TASK_ROLE_ARN", "arn:aws:iam::123456789012:role/task")
        s3 = boto3.client("s3", region_name=region)
        config = {} if region == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": region}}
        s3.create_bucket(Bucket="batch-test", **config)
        request = {
            "operation": "invoke",
            "cache_key": "shared-cache",
            "execution_id": uuid4().hex,
            "runnable": {
                "target": {"uri": "lambda:test"},
                "kwargs": {"image": {"uri": "example/image:tag"}},
                "sub": {"target": {"uri": "script"}, "adapter": "dml-local-adapter"},
            },
            "remote": {"root": "s3://batch-test/root"},
            "scratch_uri": f"s3://batch-test/scratch/{uuid4().hex}",
            "adapter_state": None,
        }
        yield client, s3, request


def put_output(s3, request, output):
    uri = _scratch_uri(request["scratch_uri"], "output.json")
    s3.put_object(Bucket="batch-test", Key=uri.split("/", 3)[3], Body=json.dumps(output).encode())


def job_model(state):
    # Moto's no-Docker backend immediately succeeds. Explicitly simulate scheduler
    # transitions; do not imply that a container actually executed.
    region = state["job_definition"].split(":")[3]
    return batch_backends["123456789012"][region].get_job_by_id(state["job_id"])


@pytest.mark.parametrize("gpu", [0, 1])
def test_launch_wire_payload_security_and_repeatable_cleanup(batch_world, gpu):
    client, s3, request = batch_world
    request["runnable"]["kwargs"].update(cpu=2, memory=2048, gpu=gpu)
    launch = BatchExecutor.handle(**request)
    assert launch["status"] == "retry"
    state = launch["adapter_state"]
    definition = client.describe_job_definitions(jobDefinitions=[state["job_definition"]])["jobDefinitions"][0]
    properties = definition["containerProperties"]
    assert properties["image"] == "example/image:tag"
    assert properties["privileged"] is False
    assert properties["environment"] == [
        {"name": "AWS_EC2_METADATA_DISABLED", "value": "true"},
        {"name": "AWS_REGION", "value": client.meta.region_name},
        {"name": "AWS_DEFAULT_REGION", "value": client.meta.region_name},
    ]
    assert properties["jobRoleArn"] == "arn:aws:iam::123456789012:role/task"
    job = client.describe_jobs(jobs=[state["job_id"]])["jobs"][0]
    assert job["jobQueue"].endswith("/gpu" if gpu else "/test")
    assert job["jobDefinition"] == state["job_definition"]
    assert properties["resourceRequirements"] == [
        {"type": "MEMORY", "value": "2048"},
        {"type": "VCPU", "value": "2"},
        *([{"type": "GPU", "value": "1"}] if gpu else []),
    ]
    command = properties["command"]
    assert command == [
        "dml-local-adapter",
        "--poll",
        "-i",
        _scratch_uri(request["scratch_uri"], "input.json"),
        "-o",
        _scratch_uri(request["scratch_uri"], "output.json"),
    ]
    nested = json.loads(s3.get_object(Bucket="batch-test", Key=command[3].split("/", 3)[3])["Body"].read())
    assert nested == {**request, "runnable": request["runnable"]["sub"]}
    put_output(s3, request, {"status": "success", "error": None})
    resumed = {**request, "adapter_state": state}
    assert BatchExecutor.handle(**resumed)["status"] == "success"
    cleanup = {**resumed, "operation": "cleanup", "result_ref": "dag:result"}
    for _ in range(2):
        assert BatchExecutor.handle(**cleanup)["status"] == "success"
        assert (
            client.describe_job_definitions(jobDefinitions=[state["job_definition"]])["jobDefinitions"][0]["status"]
            == "INACTIVE"
        )


@pytest.mark.parametrize("status", ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"])
def test_active_poll_cleanup_and_cancel_wait_for_terminal_job(batch_world, status):
    client, _, request = batch_world
    state = BatchExecutor.handle(**request)["adapter_state"]
    job = job_model(state)
    job.status = status
    job.job_stopped = False
    resumed = {**request, "adapter_state": state}
    assert BatchExecutor.handle(**resumed)["status"] == "retry"
    assert BatchExecutor.handle(**{**resumed, "operation": "cleanup", "result_ref": "dag:result"})["status"] == "retry"
    cancel = {**resumed, "operation": "cancel", "argv_ref": "node-argv:test", "requested_by": "user"}
    assert BatchExecutor.handle(**cancel)["status"] == "retry"
    assert job.stop is True
    assert (
        client.describe_job_definitions(jobDefinitions=[state["job_definition"]])["jobDefinitions"][0]["status"]
        == "ACTIVE"
    )
    job._mark_stopped(success=False)
    for _ in range(2):
        assert BatchExecutor.handle(**cancel)["status"] == "cancelled"
        assert (
            client.describe_job_definitions(jobDefinitions=[state["job_definition"]])["jobDefinitions"][0]["status"]
            == "INACTIVE"
        )


def test_failed_job_preserves_diagnostics(batch_world, monkeypatch):
    _, _, request = batch_world
    monkeypatch.setenv("MOTO_SIMPLE_BATCH_FAIL_AFTER", "0")
    state = BatchExecutor.handle(**request)["adapter_state"]
    result = BatchExecutor.handle(**{**request, "adapter_state": state})
    assert result["status"] == "failure"
    assert state["job_id"] in result["error"]


@pytest.mark.parametrize("output", [None, {"status": "failure", "error": "nested failed"}, {"status": "failure"}])
def test_terminal_output_protocol(batch_world, output):
    _, s3, request = batch_world
    state = BatchExecutor.handle(**request)["adapter_state"]
    if output is not None:
        put_output(s3, request, output)
    result = BatchExecutor.handler({**request, "adapter_state": state}, None)
    assert result["status"] == "failure"
    assert result["error"]
    if output and output.get("error"):
        assert result["error"] == "nested failed"


def test_failed_submission_rolls_back_definition(batch_world, monkeypatch):
    client, _, request = batch_world
    monkeypatch.setenv("CPU_QUEUE", "missing-queue")
    result = BatchExecutor.handler(request, None)
    assert result["status"] == "failure"
    assert "missing-queue" in result["error"]
    assert client.describe_job_definitions(status="ACTIVE")["jobDefinitions"] == []
    assert len(client.describe_job_definitions(status="INACTIVE")["jobDefinitions"]) == 1


def test_attempts_sharing_cache_have_separate_definitions_and_scratch(batch_world):
    client, s3, first = batch_world
    second = {**first, "execution_id": uuid4().hex, "scratch_uri": f"s3://batch-test/scratch/{uuid4().hex}"}
    one = BatchExecutor.handle(**first)["adapter_state"]
    two = BatchExecutor.handle(**second)["adapter_state"]
    assert one["job_definition"] != two["job_definition"]
    put_output(s3, first, {"status": "success", "error": None})
    assert BatchExecutor.handle(**{**first, "adapter_state": one})["status"] == "success"
    assert BatchExecutor.handle(**{**second, "adapter_state": two})["status"] == "failure"
    BatchExecutor.handle(**{**first, "adapter_state": one, "operation": "cleanup", "result_ref": "dag:result"})
    assert (
        client.describe_job_definitions(jobDefinitions=[two["job_definition"]])["jobDefinitions"][0]["status"]
        == "ACTIVE"
    )


@pytest.mark.parametrize("code", ["ThrottlingException", "AccessDeniedException"])
def test_failed_rollback_preserves_definition_for_cleanup(batch_world, monkeypatch, code):
    client, _, request = batch_world
    monkeypatch.setenv("CPU_QUEUE", "missing-queue")
    deregister = client.deregister_job_definition

    def fail(**kwargs):
        raise ClientError({"Error": {"Code": code, "Message": "rollback unavailable"}}, "DeregisterJobDefinition")

    monkeypatch.setattr(client, "deregister_job_definition", fail)
    monkeypatch.setattr(BatchExecutor, "_client", lambda self, **kwargs: client)
    result = BatchExecutor.handler(request, None)
    assert result["status"] == "failure"
    assert "missing-queue" in result["error"]
    assert code in result["error"]
    state = result["adapter_state"]
    definition = state["job_definition"]
    assert client.describe_job_definitions(jobDefinitions=[definition])["jobDefinitions"][0]["status"] == "ACTIVE"
    monkeypatch.setattr(client, "deregister_job_definition", deregister)
    assert (
        BatchExecutor.handle(
            **{
                **request,
                "adapter_state": state,
                "operation": "cleanup",
                "result_ref": "dag:failure",
            }
        )["status"]
        == "success"
    )
    assert client.describe_job_definitions(jobDefinitions=[definition])["jobDefinitions"][0]["status"] == "INACTIVE"


@pytest.mark.parametrize("stage", ["scratch", "register", "submit"])
@pytest.mark.parametrize(
    "code", ["Throttling", "ThrottlingException", "TooManyRequestsException", "RequestLimitExceeded"]
)
def test_throttled_launch_resumes_without_leaking_definitions(batch_world, monkeypatch, stage, code):
    from daggerml.contrib.executors import batch

    client, _, request = batch_world
    monkeypatch.setattr(BatchExecutor, "_client", lambda self, **kwargs: client)
    error = ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPHeaders": {"retry-after": "2"}}},
        "launch",
    )
    target, method = {
        "scratch": (batch, "_write_scratch_json"),
        "register": (client, "register_job_definition"),
        "submit": (client, "submit_job"),
    }[stage]
    original = getattr(target, method)

    def throttle(*args, **kwargs):
        raise error

    monkeypatch.setattr(target, method, throttle)
    result = BatchExecutor.handler(request, None)
    assert result["status"] == "retry"
    assert result["error"] is None
    assert result["retry_after_ms"] == 2000
    state = result["adapter_state"]
    assert set(state) == ({"job_definition"} if stage == "submit" else set())
    resumed = {**request, "adapter_state": state}
    assert BatchExecutor.handler(resumed, None) == result
    assert len(client.describe_job_definitions(status="ACTIVE")["jobDefinitions"]) == (stage == "submit")
    monkeypatch.setattr(target, method, original)
    launched = BatchExecutor.handler(resumed, None)
    assert launched["status"] == "retry"
    assert launched["adapter_state"]["job_id"]
    definitions = client.describe_job_definitions(status="ACTIVE")["jobDefinitions"]
    assert len(definitions) == 1
    assert client.describe_job_definitions(status="INACTIVE")["jobDefinitions"] == []
    if stage == "submit":
        assert launched["adapter_state"]["job_definition"] == state["job_definition"]
    put_output(batch_world[1], request, {"status": "success", "error": None})
    resumed["adapter_state"] = launched["adapter_state"]
    assert BatchExecutor.handle(**resumed)["status"] == "success"


def test_ambiguous_submission_is_not_retried_as_throttling(batch_world, monkeypatch):
    client, _, request = batch_world
    monkeypatch.setattr(BatchExecutor, "_client", lambda self, **kwargs: client)

    def timeout(**kwargs):
        raise TimeoutError("submission response lost")

    monkeypatch.setattr(client, "submit_job", timeout)
    result = BatchExecutor.handler(request, None)
    assert result["status"] == "failure"
    assert "submission response lost" in result["error"]
    assert client.describe_job_definitions(status="ACTIVE")["jobDefinitions"] == []
