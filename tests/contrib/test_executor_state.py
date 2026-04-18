from __future__ import annotations

import os
import time
from unittest.mock import patch

import boto3
import pytest

from daggerml.contrib.executor_state import ExecutionState

TABLE_NAME = "test-execution-state"


@pytest.fixture(scope="module")
def _dynamo_server():
    """Module fixture providing a moto DynamoDB server."""
    with patch.dict(os.environ):
        for key in list(os.environ.keys()):
            if key.startswith("AWS_"):
                del os.environ[key]
        from moto.server import ThreadedMotoServer

        server = ThreadedMotoServer(port=0, verbose=False)
        server.start()
        host, port = server.get_host_and_port()
        try:
            yield f"http://{host}:{port}"
        finally:
            server.stop()


@pytest.fixture(autouse=True)
def dynamo_env(_dynamo_server):
    """Set up DynamoDB table for each test."""
    with patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_ENDPOINT_URL": _dynamo_server,
            "DML_DYNAMODB_TABLE": TABLE_NAME,
        },
    ):
        boto3.setup_default_session()
        client = boto3.client("dynamodb", endpoint_url=_dynamo_server)
        try:
            client.create_table(
                TableName=TABLE_NAME,
                KeySchema=[{"AttributeName": "cache_key", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "cache_key", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
        except client.exceptions.ResourceInUseException:
            # Table exists, clear it
            scan = client.scan(TableName=TABLE_NAME)
            for item in scan.get("Items", []):
                client.delete_item(TableName=TABLE_NAME, Key={"cache_key": item["cache_key"]})
        yield


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------


class TestConstructor:
    def test_requires_nonempty_cache_key(self):
        with pytest.raises(Exception, match="non-empty string"):
            ExecutionState("")

    def test_requires_table_name(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DML_DYNAMODB_TABLE", None)
            with pytest.raises(Exception, match="table_name"):
                ExecutionState("ck")

    def test_uses_env_var(self):
        es = ExecutionState("ck")
        assert es.table_name == TABLE_NAME


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------


class TestUpsert:
    def test_creates_pending_record(self):
        rec = ExecutionState.upsert("ck-1", "argv-ptr-1")
        assert rec["cache_key"] == "ck-1"
        assert rec["argv_ptr"] == "argv-ptr-1"
        assert rec["status"] == "pending"
        assert rec["lock_token"] is None
        assert rec["dag_id"] is None
        assert rec["error"] is None
        assert rec["metadata"] == {}

    def test_second_call_returns_existing(self):
        ExecutionState.upsert("ck-2", "ptr-a")
        second = ExecutionState.upsert("ck-2", "ptr-b")  # different argv_ptr
        assert second["argv_ptr"] == "ptr-a"  # not overwritten
        assert second["status"] == "pending"

    def test_get_returns_same_record(self):
        ExecutionState.upsert("ck-3", "ptr-3")
        es = ExecutionState("ck-3")
        rec = es.get()
        assert rec is not None
        assert rec["cache_key"] == "ck-3"


# ---------------------------------------------------------------------------
# Lock / Unlock
# ---------------------------------------------------------------------------


class TestLock:
    def test_lock_succeeds_on_unlocked(self):
        ExecutionState.upsert("lk-1", "ptr")
        es = ExecutionState("lk-1")
        assert es.lock() is True
        assert es.lock_token is not None

    def test_lock_fails_on_locked(self):
        ExecutionState.upsert("lk-2", "ptr")
        es1 = ExecutionState("lk-2")
        es2 = ExecutionState("lk-2")
        assert es1.lock() is True
        assert es2.lock() is False

    def test_lock_succeeds_on_expired(self):
        ExecutionState.upsert("lk-3", "ptr")
        es1 = ExecutionState("lk-3")
        assert es1.lock(ttl=0.01) is True
        time.sleep(0.05)
        es2 = ExecutionState("lk-3")
        assert es2.lock() is True

    def test_lock_fails_on_nonexistent_record(self):
        es = ExecutionState("lk-nonexist")
        assert es.lock() is False

    def test_unlock_succeeds_with_matching_token(self):
        ExecutionState.upsert("lk-4", "ptr")
        es = ExecutionState("lk-4")
        assert es.lock() is True
        assert es.unlock() is True
        assert es.lock_token is None

    def test_unlock_fails_without_token(self):
        ExecutionState.upsert("lk-5", "ptr")
        es = ExecutionState("lk-5")
        assert es.unlock() is False

    def test_unlock_allows_relock(self):
        ExecutionState.upsert("lk-6", "ptr")
        es1 = ExecutionState("lk-6")
        assert es1.lock() is True
        assert es1.unlock() is True
        es2 = ExecutionState("lk-6")
        assert es2.lock() is True


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------


class TestHeartbeat:
    def test_heartbeat_extends_lock(self):
        ExecutionState.upsert("hb-1", "ptr")
        es = ExecutionState("hb-1")
        assert es.lock() is True
        assert es.heartbeat() is True
        rec = es.get()
        assert rec is not None
        assert rec["heartbeat_ts"] is not None

    def test_heartbeat_fails_without_lock(self):
        ExecutionState.upsert("hb-2", "ptr")
        es = ExecutionState("hb-2")
        assert es.heartbeat() is False

    def test_heartbeat_fails_with_expired_lock(self):
        ExecutionState.upsert("hb-3", "ptr")
        es = ExecutionState("hb-3")
        assert es.lock(ttl=0.01) is True
        time.sleep(0.05)
        assert es.heartbeat() is False


# ---------------------------------------------------------------------------
# update_metadata
# ---------------------------------------------------------------------------


class TestUpdateMetadata:
    def test_merges_metadata(self):
        ExecutionState.upsert("md-1", "ptr")
        es = ExecutionState("md-1")
        assert es.lock() is True
        assert es.update_metadata({"key1": "val1"}) is True
        assert es.update_metadata({"key2": "val2"}) is True
        rec = es.get()
        assert rec is not None
        assert rec["metadata"] == {"key1": "val1", "key2": "val2"}

    def test_fails_without_lock(self):
        ExecutionState.upsert("md-2", "ptr")
        es = ExecutionState("md-2")
        assert es.update_metadata({"x": 1}) is False


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------


class TestTransitions:
    def test_claim_running_from_pending(self):
        ExecutionState.upsert("tr-claim-1", "ptr")
        es = ExecutionState("tr-claim-1")
        assert es.claim_running() is True
        rec = es.get()
        assert rec is not None
        assert rec["status"] == "running"

    def test_claim_running_fails_after_already_claimed(self):
        ExecutionState.upsert("tr-claim-2", "ptr")
        es1 = ExecutionState("tr-claim-2")
        es2 = ExecutionState("tr-claim-2")
        assert es1.claim_running() is True
        assert es2.claim_running() is False

    def test_mark_running_from_pending(self):
        ExecutionState.upsert("tr-1", "ptr")
        es = ExecutionState("tr-1")
        assert es.lock() is True
        assert es.mark_running() is True
        rec = es.get()
        assert rec is not None
        assert rec["status"] == "running"

    def test_mark_running_fails_from_running(self):
        ExecutionState.upsert("tr-2", "ptr")
        es = ExecutionState("tr-2")
        assert es.lock() is True
        assert es.mark_running() is True
        assert es.mark_running() is False  # already running

    def test_mark_succeeded_from_running(self):
        ExecutionState.upsert("tr-3", "ptr")
        es = ExecutionState("tr-3")
        assert es.lock() is True
        assert es.mark_running() is True
        assert es.mark_succeeded("dag-123") is True
        rec = es.get()
        assert rec is not None
        assert rec["status"] == "succeeded"
        assert rec["dag_id"] == "dag-123"

    def test_mark_succeeded_fails_from_pending(self):
        ExecutionState.upsert("tr-4", "ptr")
        es = ExecutionState("tr-4")
        assert es.lock() is True
        assert es.mark_succeeded("dag") is False

    def test_mark_failed_from_running(self):
        ExecutionState.upsert("tr-5", "ptr")
        es = ExecutionState("tr-5")
        assert es.lock() is True
        assert es.mark_running() is True
        assert es.mark_failed("something broke") is True
        rec = es.get()
        assert rec is not None
        assert rec["status"] == "failed"
        assert rec["error"] == "something broke"

    def test_mark_failed_fails_from_pending(self):
        ExecutionState.upsert("tr-6", "ptr")
        es = ExecutionState("tr-6")
        assert es.lock() is True
        assert es.mark_failed("err") is False

    def test_all_mutations_fail_without_lock(self):
        ExecutionState.upsert("tr-7", "ptr")
        es = ExecutionState("tr-7")
        assert es.mark_running() is False
        assert es.mark_succeeded("dag") is False
        assert es.mark_failed("err") is False

    def test_full_lifecycle(self):
        """pending -> running -> succeeded with lock/unlock."""
        ExecutionState.upsert("tr-8", "ptr")
        es = ExecutionState("tr-8")

        # Lock, mark running, unlock
        assert es.lock() is True
        assert es.mark_running() is True
        assert es.unlock() is True

        # Re-lock, mark succeeded, unlock
        assert es.lock() is True
        assert es.mark_succeeded("dag-final") is True
        assert es.unlock() is True

        rec = es.get()
        assert rec is not None
        assert rec["status"] == "succeeded"
        assert rec["dag_id"] == "dag-final"
