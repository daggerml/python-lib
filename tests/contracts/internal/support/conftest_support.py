"""Common test fixtures for dml-util tests."""

import base64
import hashlib
import os
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest

from daggerml._internal._db import DmlDbEnv, DmlDbMapFullError, Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.types import NAMESPACES


def split_remote_root(remote_root: str) -> tuple[str, str]:
    """Parse `s3://bucket/prefix` into (bucket, project_prefix)."""
    if not remote_root.startswith("s3://"):
        raise ValueError(f"Invalid remote root URI: {remote_root!r}")
    rest = remote_root[5:]
    if not rest:
        raise ValueError(f"Invalid remote root URI: {remote_root!r}")
    if "/" not in rest:
        return rest, ""
    bucket, prefix = rest.split("/", 1)
    return bucket, prefix.strip("/")


def remote_bucket_and_prefix_from_env() -> tuple[str, str]:
    return split_remote_root(os.environ["DML_REMOTE_ROOT"])


def remote_protocol_prefix_from_env() -> str:
    """Return protocol prefix rooted under `<project-prefix>/dml`."""
    _bucket, project_prefix = remote_bucket_and_prefix_from_env()
    return f"{project_prefix}/dml" if project_prefix else "dml"


@pytest.fixture(scope="module")
def _aws_server():
    """Module fixture providing a moto S3 server."""
    with patch.dict(os.environ):
        # IMPORTANT: clear out env variables for safety **BEFORE** importing moto
        for k in os.environ:
            if k.startswith("AWS_"):
                del os.environ[k]
        from moto.server import ThreadedMotoServer

        server = ThreadedMotoServer(port=0, verbose=False)
        server.start()
        host, port = server.get_host_and_port()
        yield {
            "server": server,
            "endpoint": f"http://{host}:{port}",
            "port": port,
            "envvars": {
                "AWS_ACCESS_KEY_ID": "test",
                "AWS_SECRET_ACCESS_KEY": "test",
                "AWS_REGION": "us-east-1",
                "AWS_DEFAULT_REGION": "us-east-1",
                "AWS_ENDPOINT_URL": f"http://{host}:{port}",
            },
        }
        server.stop()


@pytest.fixture(autouse=True)
def clear_envvars():
    """Autouse fixture to clear AWS/DML env vars and set test values."""
    with patch.dict(os.environ):
        # Clear existing AWS and DML environment variables
        for k in list(os.environ.keys()):
            if k.startswith("AWS_") or k.startswith("DML_"):
                del os.environ[k]

        # Set test-specific environment variables
        os.environ["DML_REMOTE_ROOT"] = "s3://test-bucket/test-prefix"
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
        os.environ["PYTHONPATH"] = "."  # ensure `tests` is in PYTHONPATH
        yield


@pytest.fixture
def aws_server(_aws_server, clear_envvars):
    """Fixture that sets up AWS environment and returns server info."""
    import boto3

    # Set environment variables from _aws_server
    os.environ.update(_aws_server["envvars"])
    # Call boto3.setup_default_session() after env vars are set
    boto3.setup_default_session()
    yield _aws_server


@pytest.fixture
def s3(aws_server):
    """Fixture providing a boto3 S3 client and ensuring bucket exists."""
    import boto3

    s3_client = boto3.client("s3", endpoint_url=aws_server["endpoint"])
    bucket, _prefix = remote_bucket_and_prefix_from_env()
    try:
        s3_client.create_bucket(Bucket=bucket)
    except s3_client.exceptions.BucketAlreadyExists:
        pass  # Bucket already exists, which is fine
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass  # Bucket already owned by us, which is fine
    yield s3_client


@pytest.fixture
def db():
    """Fixture providing a FakeDb instance for testing."""
    return FakeDb()


@pytest.fixture
def remote_ops(db, s3):
    """Fixture providing RemoteOps instance using plain boto3.client('s3')."""
    from daggerml._internal.ops.remote import RemoteOps

    bucket, prefix = remote_bucket_and_prefix_from_env()
    yield RemoteOps(
        _db=db,
        client=s3,
        bucket=bucket,
        prefix=prefix,
    )


@pytest.fixture(scope="class")
def integration_remote_ops(temp_bo, aws_server):
    """Fixture providing RemoteOps instance with real database for integration tests."""
    import boto3

    from daggerml._internal.ops.remote import RemoteOps

    # Create S3 client for integration tests
    s3_client = boto3.client("s3", endpoint_url=aws_server["endpoint"])

    # Ensure bucket exists
    bucket, prefix = remote_bucket_and_prefix_from_env()
    try:
        s3_client.create_bucket(Bucket=bucket)
    except s3_client.exceptions.BucketAlreadyExists:
        pass  # Bucket already exists, which is fine
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass  # Bucket already owned by us, which is fine

    yield RemoteOps(
        _db=temp_bo,
        client=s3_client,
        bucket=bucket,
        prefix=prefix,
    )


@pytest.fixture
def temp_db_fn():
    """Function-scoped fixture providing a temporary DmlDbEnv for integration tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_env = None
        try:
            db_path = Path(temp_dir) / ".dml" / "db"
            db_path.mkdir(parents=True, exist_ok=True)
            db_env = TmpEnv.create(str(db_path), namespaces=sorted(NAMESPACES))
            yield db_env
        finally:
            if db_env is not None:
                db_env.clear_all()
                db_env.close()


@pytest.fixture
def temp_bo_fn(temp_db_fn):
    """Function-scoped fixture providing a BaseOps instance with a temporary database."""
    yield BaseOps(temp_db_fn)


@pytest.fixture
def integration_remote_ops_fn(temp_bo_fn, aws_server):
    """Function-scoped fixture providing RemoteOps instance with real database for integration tests."""
    import boto3

    from daggerml._internal.ops.remote import RemoteOps

    # Create S3 client for integration tests
    s3_client = boto3.client("s3", endpoint_url=aws_server["endpoint"])

    # Ensure bucket exists
    bucket, prefix = remote_bucket_and_prefix_from_env()
    try:
        s3_client.create_bucket(Bucket=bucket)
    except s3_client.exceptions.BucketAlreadyExists:
        pass  # Bucket already exists, which is fine
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        pass  # Bucket already owned by us, which is fine

    yield RemoteOps(
        _db=temp_bo_fn._db,
        client=s3_client,
        bucket=bucket,
        prefix=prefix,
    )


@dataclass
class FakeTxn:
    """Fake transaction implementation matching required interface."""

    kv: Dict[str, Any]
    readonly: bool

    def get(self, ref, raw=False):
        """Get value by ref from fake storage."""
        return self.kv.get(ref.to)

    def exists(self, ref):
        """Check whether a ref exists in fake storage."""
        return ref.to in self.kv

    def put(self, value, *, to=None, **kwargs):
        """Put value at ref in fake storage."""
        if self.readonly:
            raise ValueError("Cannot put in readonly transaction")
        if to is None:
            ns = kwargs.get("ns")
            if ns is None:
                raise ValueError("FakeTxn.put requires either to or ns")
            if kwargs.get("raw"):
                decoded = base64.b64decode(value)
                to = Ref(f"{ns}:{hashlib.sha256(decoded).hexdigest()}")
            else:
                raise ValueError("FakeTxn.put without 'to' only supports raw=True")
        self.kv[to.to] = value
        return to


@dataclass
class FakeDb:
    """Fake database implementation matching required interface."""

    kv: Dict[str, Any] = field(default_factory=dict)
    namespaces: list = field(default_factory=lambda: sorted(NAMESPACES))
    path: str = "/tmp/daggerml-fake/.dml/db"

    @contextmanager
    def tx(self, readonly=False):
        """Transaction context manager returning a raw fake transaction."""
        yield FakeTxn(self.kv, readonly)


@dataclass
class TmpEnv(DmlDbEnv):
    def clear_all(self):
        while True:
            try:
                with self.tx(readonly=False) as txn:
                    for ns in NAMESPACES:
                        for obj, _ in txn.iter(ns):
                            txn.delete(obj)
                db_path = Path(self.path)
                repo_root = db_path.parent.parent if db_path.name == "db" and db_path.parent.name == ".dml" else db_path
                shutil.rmtree(repo_root / ".dml", ignore_errors=True)
                db_path.mkdir(parents=True, exist_ok=True)
                return
            except DmlDbMapFullError:
                self.resize(self.get_size() * 2)


@pytest.fixture(scope="class")
def temp_db():
    """Provides a temporary DmlDbEnv for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_env = None
        try:
            db_path = Path(temp_dir) / ".dml" / "db"
            db_path.mkdir(parents=True, exist_ok=True)
            db_env = TmpEnv.create(str(db_path), namespaces=sorted(NAMESPACES))
            yield db_env
        finally:
            if db_env is not None:
                db_env.clear_all()
                db_env.close()


@pytest.fixture(scope="class")
def temp_bo(temp_db):
    """Provides a BaseOps instance with a temporary database."""
    yield BaseOps(temp_db)
