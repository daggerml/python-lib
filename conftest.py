from __future__ import annotations

import os
from unittest.mock import patch

import boto3
import pytest

import daggerml as dml
from daggerml._core.remote import Remote


def pytest_collection_modifyitems(items):
    for item in items:
        if "/tests/_core/" in str(item.fspath):
            item.add_marker(pytest.mark.core)
        elif "tests/contrib/" in str(item.fspath):
            item.add_marker(pytest.mark.contrib)
        if "/integration/" in str(item.fspath):
            item.add_marker(pytest.mark.slow)


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
    Remote(os.environ["DML_REMOTE_ROOT"], n_workers=1, client=s3_client)
    return "test-bucket"


@pytest.fixture(autouse=True)
def setup_doctests(request, doctest_namespace, remote_env, s3_bucket):
    """Creates a temporary Dml and api.Dag with mocked s3 for doctests"""
    if request.node.__class__.__name__.startswith("Doctest"):
        with dml.temporary() as tmp_dml:
            doctest_namespace["dml"] = tmp_dml
            doctest_namespace["dag"] = dml.new("temp-dag", dml=tmp_dml)
            yield
    else:
        yield
