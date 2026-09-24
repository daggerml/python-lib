"""Real AWS acceptance is opt-in; Moto never satisfies these assertions."""

import os
from concurrent.futures import ThreadPoolExecutor
from time import monotonic, sleep
from urllib.parse import urlparse
from uuid import uuid4

import boto3
import pytest

import daggerml.api as api
from daggerml import CancellationError, Dml
from daggerml.contrib.api import funkify

pytestmark = [pytest.mark.slow, pytest.mark.external, pytest.mark.serial]


@funkify(uri="script")
def batch_body(dag, value):
    print("batch-acceptance-marker", flush=True)
    return value.value() + 1


@funkify(uri="script")
def batch_wait(dag, marker, release):
    from time import sleep
    from urllib.parse import urlparse

    import boto3
    from botocore.exceptions import ClientError

    started = urlparse(marker.value())
    released = urlparse(release.value())
    client = boto3.client("s3")
    client.put_object(Bucket=started.netloc, Key=started.path.lstrip("/"), Body=b"started")
    while True:
        try:
            client.head_object(Bucket=released.netloc, Key=released.path.lstrip("/"))
            return "released"
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in {"404", "NoSuchKey"}:
                raise
            sleep(0.2)


def _batch_environment():
    required = (
        "ACCEPTANCE_LAMBDA_URI", "ACCEPTANCE_BATCH_IMAGE",
        "ACCEPTANCE_CPU_QUEUE", "ACCEPTANCE_BATCH_TASK_ROLE_ARN",
    )
    if any(not os.getenv(key) for key in required):
        pytest.skip("Lambda and Batch infrastructure not provisioned")


def _batch_runnable(fn):
    return funkify(
        fn, adapter="lambda", uri="batch",
        lambda_uri=os.environ["ACCEPTANCE_LAMBDA_URI"], image=os.environ["ACCEPTANCE_BATCH_IMAGE"],
    )


@pytest.fixture
def real_aws(tmp_path, monkeypatch):
    required = ("BUCKET", "REGION", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY")
    if any(not os.getenv(f"ACCEPTANCE_AWS_{key}") for key in required):
        pytest.skip("real AWS bucket, region and credentials must be provisioned")
    for key in ("REGION", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "SESSION_TOKEN"):
        if value := os.getenv(f"ACCEPTANCE_AWS_{key}"):
            monkeypatch.setenv(f"AWS_{key}", value)
    monkeypatch.setenv("AWS_DEFAULT_REGION", os.environ["ACCEPTANCE_AWS_REGION"])
    for key, target in (
        ("ACCEPTANCE_CPU_QUEUE", "CPU_QUEUE"),
        ("ACCEPTANCE_BATCH_TASK_ROLE_ARN", "BATCH_TASK_ROLE_ARN"),
    ):
        if value := os.getenv(key):
            monkeypatch.setenv(target, value)
    root = f"s3://{os.environ['ACCEPTANCE_AWS_BUCKET']}/acceptance-{uuid4().hex}"
    monkeypatch.setenv("DML_REMOTE_ROOT", root)
    try:
        yield Dml.init(str(tmp_path), remote_root=root, user="acceptance")
    finally:
        parsed = urlparse(root)
        client = boto3.client("s3")
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=parsed.netloc, Prefix=parsed.path.lstrip("/") + "/"
        )
        for page in pages:
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if objects:
                client.delete_objects(Bucket=parsed.netloc, Delete={"Objects": objects})


def test_batch_lambda_execution_result(real_aws):
    _batch_environment()
    wrapped = _batch_runnable(batch_body)
    with api.new("batch-result", dml=real_aws) as dag:
        result = dag.put(wrapped)(41, timeout=180_000)
        dag.commit(result)
    assert api.load("batch-result", dml=real_aws).result.value() == 42
    cache_key = real_aws.dag.describe(result.context().ref)["cache_key"]
    execution = real_aws.cache.describe(cache_key)["execution"]
    record = real_aws.runtime.read_execution_record(execution)
    assert record["state"]["lifecycle"] == "succeeded"
    assert record["driver"]["cleanup"] is not None
    job_id = record["driver"]["adapter_state"]["job_id"]
    jobs = boto3.client("batch").describe_jobs(jobs=[job_id])["jobs"]
    assert jobs[0]["status"] == "SUCCEEDED"
    stream = jobs[0]["container"]["logStreamName"]
    logs = boto3.client("logs").get_log_events(logGroupName="/aws/batch/job", logStreamName=stream)
    assert any("batch-acceptance-marker" in event["message"] for event in logs["events"])


def test_batch_lambda_cancels_running_job(real_aws):
    _batch_environment()
    root = real_aws.config.get("remote.root")
    marker, release = f"{root}/batch-started", f"{root}/batch-release"
    dag = api.new("batch-cancel", dml=real_aws)
    caller = dag.token
    fn = dag.put(_batch_runnable(batch_wait))

    def invoke():
        try:
            return fn(marker, release, timeout=180_000)
        except BaseException as exc:
            return exc

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(invoke)
        client = boto3.client("s3")
        parsed = urlparse(marker)
        deadline = monotonic() + 150
        while monotonic() < deadline:
            try:
                client.head_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
                break
            except client.exceptions.ClientError:
                sleep(0.5)
        else:
            raise AssertionError("Batch worker did not publish readiness marker")
        try:
            with pytest.raises(CancellationError):
                dag.cancel()
        finally:
            released = urlparse(release)
            client.put_object(Bucket=released.netloc, Key=released.path.lstrip("/"), Body=b"release")
        assert isinstance(future.result(timeout=30), BaseException)
    assert real_aws.runtime.read_execution_record(caller)["state"]["lifecycle"] == "canceled"
