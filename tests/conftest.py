"""Common test fixtures for dml-util tests."""

import logging
import os
from unittest.mock import patch

import pytest

from daggerml import Dml


@pytest.fixture(scope="module")
def _aws_server():
    with patch.dict(os.environ):
        for key in list(os.environ.keys()):
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


@pytest.fixture(autouse=True)
def clear_envvars():
    with patch.dict(os.environ):
        # Clear AWS environment variables before any tests run
        for k in list(os.environ.keys()):
            if k.startswith("AWS_") or k.startswith("DML_"):
                del os.environ[k]
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
        yield


@pytest.fixture(autouse=True)
def remote_env(clear_envvars, _aws_server):
    import boto3

    os.environ.update(_aws_server["envvars"])
    os.environ["DML_REMOTE_ROOT"] = "s3://test-bucket/test-prefix"
    os.environ["DML_REMOTE_CACHE"] = "test-cache"
    boto3.setup_default_session()
    s3 = boto3.client("s3", endpoint_url=_aws_server["endpoint"])
    try:
        s3.create_bucket(Bucket="test-bucket")
    except Exception:
        pass
    yield


@pytest.fixture(autouse=True)
def debug(clear_envvars):
    """Fixture to set debug mode for tests."""
    with patch.dict(os.environ, {"DML_DEBUG": "1"}):
        logging.basicConfig(level=logging.DEBUG)
        yield


@pytest.fixture
def dml():
    with Dml.temporary() as _dml:
        # Set function cache dir to config_dir so tests can find debug files
        with patch.dict(os.environ, DML_FN_CACHE_DIR=_dml.config_dir):
            yield _dml


@pytest.fixture
def fake_dml():
    # patches Dml and Dag so that neither does anything
    with patch("daggerml.api.Dml", autospec=True) as mock_dml:
        with patch("daggerml.api.Dag", autospec=True) as mock_dag:
            yield mock_dml, mock_dag
