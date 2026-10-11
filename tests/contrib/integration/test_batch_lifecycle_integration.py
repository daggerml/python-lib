"""Moto control-plane/S3 simulation; no containers or production isolation proof."""

import json
import os
import subprocess
import sys
from uuid import uuid4

import boto3
import pytest
from botocore.exceptions import ClientError, ConnectionClosedError, ReadTimeoutError
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
    assert set(state) == ({"job_definition", "identity"} if stage == "submit" else set())
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


@pytest.mark.parametrize("lost_continuation", [False, True])
@pytest.mark.parametrize("fault", ["read-timeout", "connection-closed", "crash", "save"])
def test_accepted_submission_recovers_without_resubmission(batch_world, monkeypatch, lost_continuation, fault):
    client, s3, request = batch_world
    monkeypatch.setattr(BatchExecutor, "_client", lambda self, **kwargs: client)
    submit = client.submit_job
    accepted = []

    def lose_response(**kwargs):
        record = json.loads(s3.get_object(
            Bucket="batch-test", Key=_scratch_uri(request["scratch_uri"], "submission.json").split("/", 3)[3],
        )["Body"].read())
        assert record["state"]["phase"] == "unknown"
        accepted.append(submit(**kwargs)["jobId"])
        if fault == "crash":
            raise KeyboardInterrupt("handler process died")
        if fault == "read-timeout":
            raise ReadTimeoutError(endpoint_url=client.meta.endpoint_url)
        if fault == "connection-closed":
            raise ConnectionClosedError(endpoint_url=client.meta.endpoint_url)
        return {"jobId": accepted[-1]}

    monkeypatch.setattr(client, "submit_job", lose_response)
    save = BatchExecutor._save_submission

    def fail_accepted_save(self, state, *args):
        if state.get("phase") == "submitted":
            raise TimeoutError("continuation write lost")
        return save(self, state, *args)

    if fault == "save":
        monkeypatch.setattr(BatchExecutor, "_save_submission", fail_accepted_save)
    if fault == "crash":
        with pytest.raises(KeyboardInterrupt):
            BatchExecutor.handler(request, None)
        state = None
    else:
        result = BatchExecutor.handler(request, None)
        assert result["status"] == "retry"
        state = result["adapter_state"]
    monkeypatch.setattr(BatchExecutor, "_save_submission", save)
    assert client.describe_job_definitions(status="INACTIVE")["jobDefinitions"] == []
    job = job_model({
        "job_id": accepted[0], "job_definition": client.describe_jobs(jobs=accepted)["jobs"][0]["jobDefinition"],
    })
    job.status = "RUNNING"
    job.job_stopped = False
    resumed = {**request, "adapter_state": None if lost_continuation else state}
    recovered = BatchExecutor.handler(json.loads(json.dumps(resumed)), None)
    assert recovered["status"] == "retry"
    assert recovered["adapter_state"]["job_id"] == accepted[0]
    assert len(accepted) == 1
    job._mark_stopped(success=True)
    put_output(s3, request, {"status": "success", "error": None})
    assert BatchExecutor.handler({**request, "adapter_state": recovered["adapter_state"]}, None)["status"] == "success"


def test_fresh_process_recovers_crash_after_accepted_submission(batch_world):
    from moto.server import ThreadedMotoServer

    client, s3, request = batch_world
    # The HTTP server shares Moto's backend, but the handler processes share no
    # Python objects or continuation: only the durable S3 intent remains.
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    env = {**os.environ, "AWS_ENDPOINT_URL": f"http://{host}:{port}"}
    crash = """
import json, os, sys
from daggerml.contrib.executors.batch import BatchExecutor
client = BatchExecutor._client(max_attempts=0)
submit = client.submit_job

def accepted_then_crash(**kwargs):
    submit(**kwargs)
    os._exit(77)

client.submit_job = accepted_then_crash
BatchExecutor._client = staticmethod(lambda **kwargs: client)
BatchExecutor.handler(json.load(sys.stdin), None)
"""
    recover = """
import json, sys
from daggerml.contrib.executors.batch import BatchExecutor
print(json.dumps(BatchExecutor.handler(json.load(sys.stdin), None)))
"""
    try:
        died = subprocess.run(
            [sys.executable, "-c", crash], input=json.dumps(request), text=True,
            capture_output=True, env=env, timeout=30,
        )
        assert died.returncode == 77, died.stderr + died.stdout
        definitions = client.describe_job_definitions(status="ACTIVE")["jobDefinitions"]
        assert len(definitions) == 1
        returned = subprocess.run(
            [sys.executable, "-c", recover], input=json.dumps(request), text=True,
            capture_output=True, env=env, timeout=30, check=True,
        )
        result = json.loads(returned.stdout)
        assert result["status"] == "retry", result
        state = result["adapter_state"]
        assert state["job_definition"] == definitions[0]["jobDefinitionArn"]
        assert state["job_id"]
        jobs = client.list_jobs(jobQueue=state["job_queue"], filters=[{
            "name": "JOB_NAME", "values": [state["job_name"]],
        }])["jobSummaryList"]
        assert [job["jobId"] for job in jobs] == [state["job_id"]]
        put_output(s3, request, {"status": "success", "error": None})
        resumed = {**request, "adapter_state": state}
        returned = subprocess.run(
            [sys.executable, "-c", recover], input=json.dumps(resumed), text=True,
            capture_output=True, env=env, timeout=30, check=True,
        )
        assert json.loads(returned.stdout)["status"] == "success"
    finally:
        server.stop()


@pytest.mark.parametrize("fault", ["disconnect", "server", "throttle"])
def test_submit_job_sdk_sends_only_one_http_attempt(monkeypatch, fault):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    from botocore.exceptions import BotoCoreError

    attempts = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            attempts.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            if fault == "disconnect":
                self.close_connection = True
                return
            body = json.dumps({
                "__type": "ThrottlingException" if fault == "throttle" else "ServerException",
                "message": "fault injection",
            }).encode()
            self.send_response(400 if fault == "throttle" else 500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        monkeypatch.setenv("AWS_ENDPOINT_URL", f"http://127.0.0.1:{server.server_port}")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        try:
            client = BatchExecutor._client(max_attempts=0)
            assert client.meta.config.retries["total_max_attempts"] == 1
            with pytest.raises((BotoCoreError, ClientError)):
                client.submit_job(jobName="test", jobQueue="queue", jobDefinition="definition")
            assert len(attempts) == 1
        finally:
            server.shutdown()
            thread.join(timeout=5)
            assert not thread.is_alive()


@pytest.mark.parametrize(
    "code,confirmed", [("ClientException", True), ("ServerException", False), ("RequestTimeout", False)],
)
def test_submission_rejection_is_explicit_not_any_client_exception(batch_world, monkeypatch, code, confirmed):
    client, _, request = batch_world
    monkeypatch.setattr(BatchExecutor, "_client", lambda self, **kwargs: client)

    def fail(**kwargs):
        raise ClientError({"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": 400}}, "SubmitJob")

    monkeypatch.setattr(client, "submit_job", fail)
    result = BatchExecutor.handler(request, None)
    assert result["status"] == ("failure" if confirmed else "retry")
    if confirmed:
        assert client.describe_job_definitions(status="ACTIVE")["jobDefinitions"] == []
        assert code in result["error"]
    else:
        assert result["adapter_state"]["phase"] == "unknown"
        assert len(client.describe_job_definitions(status="ACTIVE")["jobDefinitions"]) == 1


def unknown_submission(client, request, monkeypatch, *, count=1):
    monkeypatch.setattr(BatchExecutor, "_client", lambda self, **kwargs: client)
    submit = client.submit_job
    ids = []

    def lose_response(**kwargs):
        ids.extend(submit(**kwargs)["jobId"] for _ in range(count))
        raise TimeoutError("submission response lost")

    monkeypatch.setattr(client, "submit_job", lose_response)
    result = BatchExecutor.handler(request, None)
    assert result["status"] == "retry"
    assert result["adapter_state"]["phase"] == "unknown"
    monkeypatch.setattr(client, "submit_job", lambda **kwargs: pytest.fail("resubmitted ambiguous intent"))
    return result["adapter_state"], ids


@pytest.mark.parametrize("operation", ["invoke", "cleanup", "cancel"])
def test_crash_before_submission_stays_unresolved_without_resubmitting(batch_world, monkeypatch, operation):
    client, _, request = batch_world
    monkeypatch.setattr(BatchExecutor, "_client", lambda self, **kwargs: client)

    def die_before_request(**kwargs):
        raise KeyboardInterrupt("died before sending SubmitJob")

    monkeypatch.setattr(client, "submit_job", die_before_request)
    with pytest.raises(KeyboardInterrupt):
        BatchExecutor.handler(request, None)
    monkeypatch.setattr(client, "submit_job", lambda **kwargs: pytest.fail("unsafe resubmission"))
    event = {**request, "operation": operation}
    if operation == "cleanup":
        event["result_ref"] = "dag:result"
    if operation == "cancel":
        event.update(argv_ref="node-argv:test", requested_by="user")
    for _ in range(2):
        result = BatchExecutor.handler(event, None)
        assert result["status"] == "retry"
        assert result["adapter_state"]["phase"] == "unknown"
        event["adapter_state"] = result["adapter_state"]
    assert client.describe_job_definitions(status="INACTIVE")["jobDefinitions"] == []


def test_incomplete_candidate_description_replays_discovery_page(batch_world, monkeypatch):
    client, _, request = batch_world
    state, ids = unknown_submission(client, request, monkeypatch)
    describe = client.describe_jobs
    monkeypatch.setattr(client, "describe_jobs", lambda **kwargs: {"jobs": []})
    result = BatchExecutor.handler({**request, "adapter_state": state}, None)
    assert result["status"] == "retry"
    assert result["adapter_state"] == state
    monkeypatch.setattr(client, "describe_jobs", describe)
    recovered = BatchExecutor.handler({**request, "adapter_state": result["adapter_state"]}, None)
    assert recovered["adapter_state"]["job_id"] == ids[0]


@pytest.mark.parametrize("saved_state", ["unknown", "none", "definition"])
@pytest.mark.parametrize("operation", ["invoke", "cleanup", "cancel"])
def test_unknown_submission_waits_for_visibility_before_poll_or_teardown(
    batch_world, monkeypatch, operation, saved_state,
):
    client, _, request = batch_world
    state, ids = unknown_submission(client, request, monkeypatch)
    listing = client.list_jobs
    monkeypatch.setattr(client, "list_jobs", lambda **kwargs: {"jobSummaryList": []})
    continuation = {"unknown": state, "none": None, "definition": {"job_definition": state["job_definition"]}}
    event = {**request, "adapter_state": continuation[saved_state], "operation": operation}
    if operation == "cleanup":
        event["result_ref"] = "dag:result"
    if operation == "cancel":
        event.update(argv_ref="node-argv:test", requested_by="user")
    for _ in range(2):
        result = BatchExecutor.handler(event, None)
        assert result["status"] == "retry"
        assert result["retry_after_ms"] == 2000
        event["adapter_state"] = result["adapter_state"]
        assert event["adapter_state"]["phase"] == "unknown"
        assert client.describe_job_definitions(status="INACTIVE")["jobDefinitions"] == []
    monkeypatch.setattr(client, "list_jobs", listing)
    result = BatchExecutor.handler(event, None)
    assert result["adapter_state"]["job_id"] == ids[0]
    if operation != "invoke":
        event["adapter_state"] = result["adapter_state"]
        assert BatchExecutor.handler(event, None)["status"] == ("cancelled" if operation == "cancel" else "success")


@pytest.mark.parametrize("stage", ["list", "describe"])
def test_reconciliation_throttling_keeps_identity_and_retry_timing(batch_world, monkeypatch, stage):
    client, _, request = batch_world
    state, ids = unknown_submission(client, request, monkeypatch)
    method = "list_jobs" if stage == "list" else "describe_jobs"
    original = getattr(client, method)

    def throttle(**kwargs):
        raise ClientError({
            "Error": {"Code": "ThrottlingException"},
            "ResponseMetadata": {"HTTPHeaders": {"retry-after": "3"}},
        }, method)

    monkeypatch.setattr(client, method, throttle)
    result = BatchExecutor.handler({**request, "adapter_state": state}, None)
    assert result["status"] == "retry"
    assert result["adapter_state"] == state
    assert result["retry_after_ms"] == 3000
    monkeypatch.setattr(client, method, original)
    assert BatchExecutor.handler({**request, "adapter_state": state}, None)["adapter_state"]["job_id"] == ids[0]


@pytest.mark.parametrize("operation", ["cleanup", "cancel"])
def test_paginated_duplicates_are_all_retained_and_torn_down(batch_world, monkeypatch, operation):
    client, _, request = batch_world
    state, ids = unknown_submission(client, request, monkeypatch, count=2)
    calls = []

    def paginated(**kwargs):
        calls.append(kwargs)
        assert "jobStatus" not in kwargs
        assert kwargs["jobQueue"] == state["job_queue"]
        assert kwargs["filters"] == [{"name": "JOB_NAME", "values": [state["job_name"]]}]
        if kwargs.get("nextToken") == "second":
            return {"jobSummaryList": [{"jobId": ids[1]}]}
        return {"jobSummaryList": [{"jobId": ids[0]}], "nextToken": "second"}

    monkeypatch.setattr(client, "list_jobs", paginated)
    first = BatchExecutor.handler({**request, "adapter_state": state}, None)
    assert len(calls) == 1
    assert first["adapter_state"]["next_token"] == "second"
    assert first["adapter_state"]["phase"] == "unknown"
    second = BatchExecutor.handler({**request, "adapter_state": first["adapter_state"]}, None)
    state = second["adapter_state"]
    assert state["phase"] == "duplicates"
    assert state["job_ids"] == sorted(ids)
    assert "job_id" not in state
    failed = BatchExecutor.handler({**request, "adapter_state": state}, None)
    assert failed["status"] == "failure"
    assert all(job_id in failed["error"] for job_id in ids)
    for job_id in ids:
        job = job_model({"job_id": job_id, "job_definition": state["job_definition"]})
        job.status = "RUNNING"
        job.job_stopped = False
    event = {**request, "adapter_state": state, "operation": operation}
    event.update({"result_ref": "dag:result"} if operation == "cleanup" else {
        "argv_ref": "node-argv:test", "requested_by": "user",
    })
    assert BatchExecutor.handler(event, None)["status"] == "retry"
    assert client.describe_job_definitions(status="INACTIVE")["jobDefinitions"] == []
    for job_id in ids:
        job = job_model({"job_id": job_id, "job_definition": state["job_definition"]})
        if operation == "cancel":
            assert job.stop is True
        job._mark_stopped(success=False)
    assert BatchExecutor.handler(event, None)["status"] == ("cancelled" if operation == "cancel" else "success")


@pytest.mark.parametrize("field", ["jobDefinition", "jobQueue", "jobName", "tags", "createdAt"])
def test_reconciliation_rejects_foreign_candidates(batch_world, monkeypatch, field):
    client, _, request = batch_world
    state, _ = unknown_submission(client, request, monkeypatch)
    describe = client.describe_jobs

    def foreign(**kwargs):
        result = describe(**kwargs)
        result["jobs"][0][field] = {} if field == "tags" else (0 if field == "createdAt" else "foreign")
        return result

    monkeypatch.setattr(client, "describe_jobs", foreign)
    result = BatchExecutor.handler({**request, "adapter_state": state}, None)
    assert result["status"] == "retry"
    assert result["adapter_state"]["phase"] == "unknown"
    assert result["adapter_state"]["job_ids"] == []


@pytest.mark.parametrize("phase", ["definition", "unknown", "submitted", "duplicates"])
@pytest.mark.parametrize("operation", ["invoke", "cleanup", "cancel"])
@pytest.mark.parametrize("change", ["execution", "remote"])
def test_continuation_cannot_cross_execution_or_remote(batch_world, monkeypatch, change, operation, phase):
    client, _, request = batch_world
    if phase == "definition":
        monkeypatch.setattr(BatchExecutor, "_client", lambda self, **kwargs: client)

        def throttle(**kwargs):
            raise ClientError({"Error": {"Code": "ThrottlingException"}}, "SubmitJob")

        monkeypatch.setattr(client, "submit_job", throttle)
        state = BatchExecutor.handler(request, None)["adapter_state"]
        assert set(state) == {"job_definition", "identity"}
    else:
        state, _ = unknown_submission(client, request, monkeypatch, count=2 if phase == "duplicates" else 1)
    if phase not in {"definition", "unknown"}:
        state = BatchExecutor.handler({**request, "adapter_state": state}, None)["adapter_state"]
        assert state["phase"] == phase
    monkeypatch.setattr(client, "terminate_job", lambda **kwargs: pytest.fail("terminated another execution"))
    monkeypatch.setattr(
        client, "deregister_job_definition", lambda **kwargs: pytest.fail("deregistered another execution"),
    )
    other = {**request, "adapter_state": state, "operation": operation}
    if operation == "cleanup":
        other["result_ref"] = "dag:result"
    if operation == "cancel":
        other.update(argv_ref="node-argv:test", requested_by="user")
    if change == "execution":
        other["execution_id"] = uuid4().hex
    else:
        other["remote"] = {"root": "s3://batch-test/other-root"}
    assert BatchExecutor.handler(other, None)["status"] == "failure"
    other["adapter_state"] = None
    assert BatchExecutor.handler(other, None)["status"] == "failure"


@pytest.mark.parametrize("stored", [False, True])
@pytest.mark.parametrize("stage", ["intent", "rejection"])
def test_submission_record_write_failure_retains_definition_for_teardown(batch_world, monkeypatch, stage, stored):
    client, _, request = batch_world
    monkeypatch.setattr(BatchExecutor, "_client", lambda self, **kwargs: client)
    save = BatchExecutor._save_submission
    submits = []

    def reject(**kwargs):
        submits.append(kwargs)
        raise ClientError({"Error": {"Code": "ClientException", "Message": "confirmed rejection"}}, "SubmitJob")

    monkeypatch.setattr(client, "submit_job", reject)

    def fail_write(self, state, *args):
        if (stage == "intent") == (state.get("phase") == "unknown"):
            if stored:
                save(self, state, *args)
            raise ReadTimeoutError(endpoint_url="s3")
        return save(self, state, *args)

    monkeypatch.setattr(BatchExecutor, "_save_submission", fail_write)
    result = BatchExecutor.handler(request, None)
    assert result["status"] == "failure"
    state = result["adapter_state"]
    assert state["job_definition"]
    assert len(submits) == (stage == "rejection")
    monkeypatch.setattr(BatchExecutor, "_save_submission", save)
    cleaned = BatchExecutor.handler({
        **request, "adapter_state": state, "operation": "cleanup", "result_ref": "dag:failure",
    }, None)
    assert cleaned["status"] == "success", cleaned
    assert client.describe_job_definitions(status="ACTIVE")["jobDefinitions"] == []
