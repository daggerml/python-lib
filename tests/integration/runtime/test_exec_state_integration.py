"""Tests for daggerml._internal.exec_state (S3-backed ExecutionState)."""

from __future__ import annotations

import os
import time
from unittest.mock import patch

import boto3
import pytest

from daggerml._internal.exec_state import LOCK_TTL, AdapterIO, ExecutionState
from daggerml._internal.types import DmlRepoError

pytestmark = pytest.mark.slow

BUCKET = "test-exec-state-bucket"
REMOTE_ROOT = f"s3://{BUCKET}/test-prefix"


# ---------------------------------------------------------------------------
# Module-scoped moto S3 server (reuse pattern from tests/conftest.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _s3_server():
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
def s3_env(_s3_server):
    """Set up S3 bucket for each test (clean state)."""
    with patch.dict(os.environ, _s3_server["envvars"]):
        boto3.setup_default_session()
        s3 = boto3.client("s3", endpoint_url=_s3_server["endpoint"])
        try:
            s3.create_bucket(Bucket=BUCKET)
        except Exception:
            # Bucket exists — delete all objects to start clean
            resp = s3.list_objects_v2(Bucket=BUCKET)
            for obj in resp.get("Contents", []):
                s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
        yield


def _es(cache_key: str) -> ExecutionState:
    return ExecutionState(cache_key, remote_root=REMOTE_ROOT)


# ---------------------------------------------------------------------------
# 2.8 Constructor — missing / invalid remote_root raises DmlRepoError
# ---------------------------------------------------------------------------


class TestConstructor:
    def test_missing_remote_root_raises(self):
        with pytest.raises(DmlRepoError, match="s3://"):
            ExecutionState("ck", remote_root="not-s3://foo")

    def test_empty_bucket_raises(self):
        with pytest.raises(DmlRepoError):
            ExecutionState("ck", remote_root="s3:///prefix")

    def test_non_s3_scheme_raises(self):
        with pytest.raises(DmlRepoError):
            ExecutionState("ck", remote_root="gs://bucket/prefix")

    def test_valid_construction(self):
        es = _es("ck-valid")
        assert es.cache_key == "ck-valid"
        assert es._bucket == BUCKET
        assert "dml/locks/ck-valid.json" in es._lock_key

    def test_key_derived_from_prefix(self):
        es = ExecutionState("ck", remote_root="s3://mybucket/my/prefix")
        assert es._lock_key == "my/prefix/dml/locks/ck.json"

    def test_key_no_prefix(self):
        es = ExecutionState("ck", remote_root="s3://mybucket")
        assert es._lock_key == "dml/locks/ck.json"


# ---------------------------------------------------------------------------
# 2.2 lock() creates file when absent, returns True
# ---------------------------------------------------------------------------


class TestLockAbsent:
    def test_lock_creates_file_returns_true(self):
        es = _es("lock-absent-1")
        assert es.lock() is True
        assert es._lock_token is not None

    def test_lock_file_exists_after_lock(self):
        es = _es("lock-absent-2")
        assert es.lock() is True
        record = es._get_object()
        assert record is not None
        assert "lock_token" in record
        assert "lock_expires_ts" in record


# ---------------------------------------------------------------------------
# 2.3 lock() returns False when non-expired lock exists
# ---------------------------------------------------------------------------


class TestLockHeld:
    def test_lock_returns_false_when_held(self):
        es1 = _es("lock-held-1")
        es2 = _es("lock-held-1")
        assert es1.lock() is True
        assert es2.lock() is False

    def test_lock_token_unchanged_on_failure(self):
        es1 = _es("lock-held-2")
        es2 = _es("lock-held-2")
        assert es1.lock() is True
        assert es2.lock() is False
        assert es2._lock_token is None


# ---------------------------------------------------------------------------
# 2.4 lock() steals expired lock (DELETE + re-PUT), returns True
# ---------------------------------------------------------------------------


class TestLockExpired:
    def test_lock_steals_expired(self):
        es1 = _es("lock-exp-1")
        assert es1.lock(ttl=0.01) is True
        time.sleep(0.05)
        es2 = _es("lock-exp-1")
        assert es2.lock() is True
        assert es2._lock_token is not None

    def test_stolen_lock_has_new_token(self):
        es1 = _es("lock-exp-2")
        assert es1.lock(ttl=0.01) is True
        old_record = es1._get_object()
        time.sleep(0.05)
        es2 = _es("lock-exp-2")
        assert es2.lock() is True
        new_record = es2._get_object()
        assert old_record is not None and new_record is not None
        assert new_record["lock_token"] != old_record["lock_token"]


# ---------------------------------------------------------------------------
# 2.5 lock() returns False on 412 concurrent conflict
# This is exercised indirectly via moto; we simulate by monkeypatching.
# ---------------------------------------------------------------------------


class TestLock412:
    def test_lock_returns_false_on_412(self, monkeypatch):
        """Simulate a 412 PreconditionFailed from S3."""
        import botocore.exceptions

        def _fake_put(*args, **kwargs):
            error_response = {"Error": {"Code": "PreconditionFailed", "Message": "precondition failed"}}
            raise botocore.exceptions.ClientError(error_response, "PutObject")

        es = _es("lock-412-1")
        monkeypatch.setattr(es, "_put_object_if_absent", lambda _: False)
        assert es.lock() is False


# ---------------------------------------------------------------------------
# 2.6 unlock() deletes the file
# ---------------------------------------------------------------------------


class TestUnlock:
    def test_unlock_deletes_file(self):
        es = _es("unlock-1")
        assert es.lock() is True
        es.unlock()
        assert es._get_object() is None

    def test_unlock_clears_token(self):
        es = _es("unlock-2")
        assert es.lock() is True
        es.unlock()
        assert es._lock_token is None


# ---------------------------------------------------------------------------
# 2.7 unlock() is idempotent when file absent
# ---------------------------------------------------------------------------


class TestUnlockIdempotent:
    def test_unlock_no_op_when_absent(self):
        es = _es("unlock-idem-1")
        # Never locked — should not raise
        es.unlock()

    def test_double_unlock_no_error(self):
        es = _es("unlock-idem-2")
        assert es.lock() is True
        es.unlock()
        es.unlock()  # second call is a no-op


# ---------------------------------------------------------------------------
# LOCK_TTL constant
# ---------------------------------------------------------------------------


def test_lock_ttl_is_positive():
    assert LOCK_TTL > 0


def test_active_execution_pointer_round_trip():
    es = _es("active-1")
    assert es.read_active_execution_id() is None
    assert es.create_active_execution("exec-3") is True
    assert es.read_active_execution_id() == "exec-3"
    es.delete_active_execution()
    assert es.read_active_execution_id() is None


def test_execution_record_is_create_only():
    es = _es("record-1")
    record = {
        "execution_id": "exec-1",
        "cache_key": "record-1",
        "lifecycle": "running",
        "updated_at": 1,
        "spawned_execution_ids": [],
        "cancellation_requested_by": None,
    }
    assert es.create_execution_record(record) is True
    assert es.read_execution_record("exec-1") == record
    assert es.create_execution_record(record) is False
    assert es._key_for_execution("exec-1") == "test-prefix/dml/exec/state/exec-1.json"


def test_execution_record_updates_merge_monotonically():
    es = _es("record-2")
    created = {
        "execution_id": "exec-0",
        "cache_key": "record-2",
        "lifecycle": "running",
        "updated_at": 10,
        "spawned_execution_ids": [],
        "cancellation_requested_by": None,
    }
    assert es.create_execution_record(created)
    merged = es.update_execution_record(
        {
            "execution_id": "exec-0",
            "cache_key": "record-2",
            "lifecycle": "cancel-pending",
            "updated_at": 11,
            "spawned_execution_ids": ["exec-2"],
            "cancellation_requested_by": "user@example.com",
        }
    )
    assert merged["spawned_execution_ids"] == ["exec-2"]
    assert merged["lifecycle"] == "cancel-pending"
    assert merged["cancellation_requested_by"] == "user@example.com"


def test_launch_state_round_trip():
    es = _es("launch-1")
    launch_state = {
        "execution_id": "exec-launch-1",
        "cache_key": "launch-1",
        "resume_state": {"pid": 1},
        "created_at": 1,
    }
    assert es.create_launch_state(launch_state) is True
    assert es.read_launch_state("exec-launch-1") == launch_state


def test_call_edge_records_are_canonical_and_idempotent():
    es = _es("callee")
    es.record_execution_dependency(caller_execution_id="caller-a", callee_execution_id="callee")
    es.record_execution_dependency(caller_execution_id="caller-a", callee_execution_id="callee")
    edge, _ = es._read_json(es._key_for_edge("callee", "caller-a"))
    assert edge == {"caller_execution_id": "caller-a", "callee_execution_id": "callee"}


def test_invalidation_record_is_create_only():
    es = _es("invalidate")
    assert es.create_invalidation_record(
        execution_id="exec-9",
        cache_key="invalidate",
        requested_by="user@example.com",
        requested_at=123,
    )
    assert not es.create_invalidation_record(
        execution_id="exec-9",
        cache_key="invalidate",
        requested_by="user@example.com",
        requested_at=123,
    )


# ---------------------------------------------------------------------------
# AdapterIO
# ---------------------------------------------------------------------------


class TestAdapterIO:
    def test_input_uri_derived_correctly(self):
        es = _es("io-ck")
        io = es.adapter_io("exec-uuid", "local:docker")
        assert io.input_uri == f"s3://{BUCKET}/test-prefix/dml/io/io-ck/exec-uuid/local:docker/input.json"

    def test_output_uri_derived_correctly(self):
        es = _es("io-ck")
        io = es.adapter_io("exec-uuid", "local:docker")
        assert io.output_uri == f"s3://{BUCKET}/test-prefix/dml/io/io-ck/exec-uuid/local:docker/output.json"

    def test_uri_properties_make_no_s3_call(self, monkeypatch):
        calls = []
        es = _es("io-no-s3")
        monkeypatch.setattr(es, "_put_object", lambda *a, **kw: calls.append(("put", a, kw)))
        monkeypatch.setattr(es, "_get_object_bytes", lambda *a, **kw: calls.append(("get", a, kw)) or None)
        io = es.adapter_io("exec-uuid", "local:docker")
        _ = io.input_uri
        _ = io.output_uri
        assert calls == []

    def test_write_input_stores_data_and_returns_input_uri(self):
        es = _es("io-write")
        io = es.adapter_io("exec-id-write", "lambda:batch")
        uri = io.write_input(b'{"payload": 1}')
        assert uri == io.input_uri
        # Read back via raw S3 to confirm
        result = es._get_object_bytes(io._input_key)
        assert result is not None
        assert result[0] == b'{"payload": 1}'

    def test_read_output_returns_none_when_absent(self):
        es = _es("io-read-absent")
        io = es.adapter_io("exec-id-absent", "lambda:batch")
        assert io.read_output() is None

    def test_read_output_returns_bytes_when_present(self):
        es = _es("io-read-present")
        io = es.adapter_io("exec-id-present", "lambda:batch")
        es._put_object(io._output_key, b'{"status":"succeeded"}')
        assert io.read_output() == b'{"status":"succeeded"}'

    def test_adapter_io_factory_returns_adapter_io_instance(self):
        es = _es("io-factory")
        io = es.adapter_io("exec-x", "local:docker")
        assert isinstance(io, AdapterIO)

    def test_paths_scoped_within_fn_exec_io(self):
        es = _es("io-scope")
        io = es.adapter_io("exec-y", "local:docker")
        assert "dml/io/" in io.input_uri
        assert "dml/io/" in io.output_uri

    def test_different_names_produce_different_paths(self):
        es = _es("io-names")
        io1 = es.adapter_io("exec-z", "local:docker")
        io2 = es.adapter_io("exec-z", "lambda:batch")
        assert io1.input_uri != io2.input_uri
        assert io1.output_uri != io2.output_uri

    def test_no_prefix_remote_root(self):
        es = ExecutionState("io-np", remote_root=f"s3://{BUCKET}")
        io = es.adapter_io("exec-np", "local:docker")
        assert io.input_uri == f"s3://{BUCKET}/dml/io/io-np/exec-np/local:docker/input.json"
