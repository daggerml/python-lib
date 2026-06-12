from __future__ import annotations

import os
from unittest.mock import patch

import boto3
import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        if "/tests/_core/" in str(item.fspath):
            item.add_marker(pytest.mark.core)


@pytest.fixture(autouse=True)
def clear_envvars():
    with patch.dict(os.environ):
        for key in list(os.environ):
            if key.startswith("AWS_") or key.startswith("DML_"):
                del os.environ[key]
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
        yield


@pytest.fixture(scope="session")
def aws_server():
    with patch.dict(os.environ):
        for key in list(os.environ):
            if key.startswith("AWS_"):
                del os.environ[key]
        from moto.server import ThreadedMotoServer

        server = ThreadedMotoServer(port=0, verbose=False)
        server.start()
        host, port = server.get_host_and_port()
        try:
            yield {
                "endpoint": f"http://{host}:{port}",
                "envvars": {
                    "AWS_ACCESS_KEY_ID": "test",
                    "AWS_SECRET_ACCESS_KEY": "test",
                    "AWS_REGION": "us-east-1",
                    "AWS_DEFAULT_REGION": "us-east-1",
                    "AWS_ENDPOINT_URL": f"http://{host}:{port}",
                },
            }
        finally:
            server.stop()


@pytest.fixture
def remote_env(clear_envvars, aws_server):
    os.environ.update(aws_server["envvars"])
    os.environ["DML_REMOTE_ROOT"] = "s3://test-bucket/test-prefix"
    boto3.setup_default_session()
    yield aws_server["endpoint"]


@pytest.fixture
def s3_client(remote_env):
    return boto3.client("s3", endpoint_url=remote_env)


@pytest.fixture
def s3_bucket(s3_client):
    s3_client.create_bucket(Bucket="test-bucket")
    return "test-bucket"


@pytest.fixture
def fake_dml():
    with patch("daggerml._core.Dml", autospec=True) as mock_dml:
        yield mock_dml
