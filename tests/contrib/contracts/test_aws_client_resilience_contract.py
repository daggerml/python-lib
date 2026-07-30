from __future__ import annotations

import json

from daggerml import Uri
from daggerml.contrib.executors.batch import BatchExecutor
from daggerml.util import get_client


def test_contrib_aws_client_001__get_client_uses_default_resilience_policy(monkeypatch):
    captured = {}
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setattr(
        "daggerml.util.boto3.client",
        lambda name, config, **kwargs: captured.update(name=name, config=config, kwargs=kwargs) or object(),
    )

    get_client("s3")

    assert captured["name"] == "s3"
    assert captured["config"].connect_timeout == 5
    assert captured["config"].read_timeout == 60
    assert captured["config"].retries["mode"] == "adaptive"
    assert captured["config"].retries["max_attempts"] == 5
    assert captured["config"].max_pool_connections == 20
    assert captured["kwargs"] == {}


def test_contrib_aws_client_002__get_client_passes_resilience_overrides(monkeypatch):
    captured = {}
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setattr(
        "daggerml.util.boto3.client",
        lambda name, config, **kwargs: captured.update(name=name, config=config, kwargs=kwargs) or object(),
    )

    get_client(
        "logs",
        connection_timeout=12,
        read_timeout=34,
        max_attempts=7,
        retry_mode="standard",
        max_pool_connections=56,
    )

    assert captured["name"] == "logs"
    assert captured["config"].connect_timeout == 12
    assert captured["config"].read_timeout == 34
    assert captured["config"].retries == {"mode": "standard", "max_attempts": 7}
    assert captured["config"].max_pool_connections == 56
    assert captured["kwargs"] == {}


def test_contrib_aws_client_003__batch_launch_and_poll_use_high_resilience_clients(monkeypatch):
    clients = []
    writes = []

    class S3Client:
        def put_object(self, **kwargs):
            writes.append(kwargs)

        def get_object(self, **kwargs):
            return {"Body": type("Body", (), {"read": lambda self: json.dumps({"status": "succeeded"}).encode()})()}

    class BatchClient:
        def register_job_definition(self, **kwargs):
            return {"jobDefinitionArn": "arn:job-definition"}

        def submit_job(self, **kwargs):
            return {"jobId": "job-1"}

        def describe_jobs(self, **kwargs):
            return {"jobs": [{"status": "SUCCEEDED"}]}

    def fake_get_client(name, **kwargs):
        clients.append((name, kwargs))
        return S3Client() if name == "s3" else BatchClient()

    monkeypatch.setattr("daggerml.contrib.executors.batch.get_client", fake_get_client)
    monkeypatch.setenv("CPU_QUEUE", "queue")
    monkeypatch.setenv("BATCH_TASK_ROLE_ARN", "arn:role")
    executor = BatchExecutor()
    start = executor.start(
        cache_key="cache-key",
        execution_id="execution-id",
        runnable={"sub": {"adapter": "dml-lambda-adapter"}, "kwargs": {"image": Uri("example:image")}},
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/scratch",
    )
    result = executor.poll(
        cache_key="cache-key",
        execution_id="execution-id",
        runnable={},
        state=start["state"],
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/scratch",
    )

    assert writes
    assert result == {"status": "succeeded"}
    expected_start_policy = {
        "connection_timeout": 60,
        "read_timeout": 60,
        "max_attempts": 100,
        "retry_mode": "adaptive",
        "max_pool_connections": 100,
    }
    expected_poll_policy = {**expected_start_policy, "max_attempts": 25}
    assert [name for name, _ in clients] == ["s3", "batch", "batch", "s3"]
    assert [kwargs for _, kwargs in clients] == [
        expected_start_policy,
        expected_start_policy,
        expected_poll_policy,
        expected_poll_policy,
    ]
