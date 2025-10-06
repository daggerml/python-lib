"""Common test fixtures for dml-util tests."""

import logging
import os
from unittest.mock import patch

import pytest

from daggerml import Dml


@pytest.fixture(autouse=True)
def clear_envvars():
    with patch.dict(os.environ):
        # Clear AWS environment variables before any tests run
        for k in os.environ:
            if k.startswith("AWS_") or k.startswith("DML_"):
                del os.environ[k]
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
        yield


@pytest.fixture(autouse=True)
def debug(clear_envvars):
    """Fixture to set debug mode for tests."""
    with patch.dict(os.environ, {"DML_DEBUG": "1"}):
        logging.basicConfig(level=logging.DEBUG)
        yield


@pytest.fixture
def dml(tmpdir):
    with Dml.temporary(cache_path=str(tmpdir)) as _dml:
        with patch.dict(os.environ, DML_FN_CACHE_DIR=_dml.kwargs["config_dir"], **_dml.envvars):
            yield _dml


@pytest.fixture
def fake_dml():
    # patches Dml and Dag so that neither does anything
    with patch("daggerml.core.Dml", autospec=True) as mock_dml:
        with patch("daggerml.core.Dag", autospec=True) as mock_dag:
            yield mock_dml, mock_dag
