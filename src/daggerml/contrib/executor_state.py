from __future__ import annotations

import json
import os
import time
from typing import Any, Literal, TypedDict, cast
from uuid import uuid4

from daggerml._internal.types import DmlRepoError
from daggerml.util import get_client

# ---------------------------------------------------------------------------
# Execution state model
# ---------------------------------------------------------------------------

Status = Literal["pending", "running", "succeeded", "failed", "done"]
LOCK_TTL = 15.0


class ExecutionRecord(TypedDict):
    cache_key: str
    argv_ptr: str
    status: Status
    lock_token: str | None
    lock_expires_ts: float | None
    dag_id: str | None
    error: str | None
    heartbeat_ts: float | None
    metadata: dict[str, Any]
    updated_ts: float


def _check_condition_failure(exc: Exception) -> bool:
    """Return True if the exception is a DynamoDB ConditionalCheckFailedException."""
    code = getattr(exc, "response", {}).get("Error", {}).get("Code")
    return code == "ConditionalCheckFailedException"


def _record_from_item(item: dict[str, Any]) -> ExecutionRecord:
    """Deserialize a DynamoDB item into an ExecutionRecord."""
    raw = item.get("state", {}).get("S")
    if not raw:
        raise DmlRepoError("Execution state item missing 'state' field")
    return cast(ExecutionRecord, json.loads(raw))


def _serialize_record(record: ExecutionRecord) -> str:
    return json.dumps(record, separators=(",", ":"), sort_keys=True)


class ExecutionState:
    """DynamoDB-backed execution state with advisory locking."""

    def __init__(self, cache_key: str, *, table_name: str | None = None) -> None:
        if not isinstance(cache_key, str) or not cache_key:
            raise DmlRepoError("ExecutionState cache_key must be a non-empty string")
        self.cache_key = cache_key
        self.table_name = table_name or os.getenv("DML_DYNAMODB_TABLE")
        if not self.table_name:
            raise DmlRepoError("ExecutionState requires table_name or DML_DYNAMODB_TABLE env var")
        self.lock_token: str | None = None
        self._client = get_client("dynamodb")

    def _item_key(self) -> dict[str, Any]:
        return {"cache_key": {"S": self.cache_key}}

    # -- upsert (classmethod) -----------------------------------------------

    @classmethod
    def upsert(
        cls,
        cache_key: str,
        argv_ptr: str,
        *,
        table_name: str | None = None,
    ) -> ExecutionRecord:
        """Create a pending record if absent; return the current record either way."""
        inst = cls(cache_key, table_name=table_name)
        now = time.time()
        record: ExecutionRecord = {
            "cache_key": cache_key,
            "argv_ptr": argv_ptr,
            "status": "pending",
            "lock_token": None,
            "lock_expires_ts": None,
            "dag_id": None,
            "error": None,
            "heartbeat_ts": None,
            "metadata": {},
            "updated_ts": now,
        }
        try:
            inst._client.put_item(
                TableName=inst.table_name,
                Item={
                    "cache_key": {"S": cache_key},
                    "state": {"S": _serialize_record(record)},
                    "updated_ts": {"N": str(now)},
                },
                ConditionExpression="attribute_not_exists(cache_key)",
            )
            return record
        except Exception as e:
            if _check_condition_failure(e):
                existing = inst.get()
                if existing is None:
                    raise DmlRepoError("ExecutionState upsert race: item vanished after conflict") from e
                return existing
            raise

    # -- get (unlocked read) ------------------------------------------------

    def get(self) -> ExecutionRecord | None:
        resp = self._client.get_item(
            TableName=self.table_name,
            Key=self._item_key(),
            ConsistentRead=True,
        )
        item = resp.get("Item")
        if item is None:
            return None
        return _record_from_item(item)

    # -- lock / unlock ------------------------------------------------------

    def lock(self, ttl: float = LOCK_TTL) -> bool:
        """Acquire the advisory lock.  Returns True on success."""
        token = str(uuid4())
        now = time.time()
        # Read current state so we can update the lock fields inside the JSON blob.
        # Safe because the conditional expression below guarantees no other writer
        # holds the lock; if the lock is stolen between read and write, the
        # condition fails and we return False.
        record = self.get()
        if record is None:
            return False
        patched = dict(record)
        patched["lock_token"] = token
        patched["lock_expires_ts"] = now + ttl
        patched["updated_ts"] = now
        try:
            self._client.update_item(
                TableName=self.table_name,
                Key=self._item_key(),
                UpdateExpression="SET #st = :st, #ts = :ts, #lt = :lt, #le = :le",
                ConditionExpression=(
                    "attribute_exists(cache_key) AND (  attribute_not_exists(#lt) OR #lt = :null OR #le <= :now)"
                ),
                ExpressionAttributeNames={
                    "#st": "state",
                    "#ts": "updated_ts",
                    "#lt": "lock_token",
                    "#le": "lock_expires_ts",
                },
                ExpressionAttributeValues={
                    ":st": {"S": _serialize_record(cast(ExecutionRecord, patched))},
                    ":ts": {"N": str(now)},
                    ":lt": {"S": token},
                    ":le": {"N": str(now + ttl)},
                    ":null": {"NULL": True},
                    ":now": {"N": str(now)},
                },
            )
            self.lock_token = token
            return True
        except Exception as e:
            if _check_condition_failure(e):
                return False
            raise

    def unlock(self) -> bool:
        """Release the advisory lock.  Returns True on success."""
        if self.lock_token is None:
            return False
        now = time.time()
        record = self.get()
        if record is None:
            return False
        patched = dict(record)
        patched["lock_token"] = None
        patched["lock_expires_ts"] = None
        patched["updated_ts"] = now
        try:
            self._client.update_item(
                TableName=self.table_name,
                Key=self._item_key(),
                UpdateExpression="SET #st = :st, #ts = :ts REMOVE #lt, #le",
                ConditionExpression="#lt = :lt AND #le > :now",
                ExpressionAttributeNames={
                    "#st": "state",
                    "#ts": "updated_ts",
                    "#lt": "lock_token",
                    "#le": "lock_expires_ts",
                },
                ExpressionAttributeValues={
                    ":st": {"S": _serialize_record(cast(ExecutionRecord, patched))},
                    ":ts": {"N": str(now)},
                    ":lt": {"S": self.lock_token},
                    ":now": {"N": str(now)},
                },
            )
            self.lock_token = None
            return True
        except Exception as e:
            if _check_condition_failure(e):
                return False
            raise

    # -- heartbeat ----------------------------------------------------------

    def heartbeat(self, duration: float = LOCK_TTL) -> bool:
        """Extend lock and refresh heartbeat_ts.  Requires valid lock."""
        if self.lock_token is None:
            return False
        now = time.time()
        record = self.get()
        if record is None:
            return False
        patched = dict(record)
        patched["heartbeat_ts"] = now
        patched["lock_expires_ts"] = now + duration
        patched["updated_ts"] = now
        try:
            self._client.update_item(
                TableName=self.table_name,
                Key=self._item_key(),
                UpdateExpression="SET #st = :st, #ts = :ts, #le = :le",
                ConditionExpression="#lt = :lt AND #le > :now",
                ExpressionAttributeNames={
                    "#st": "state",
                    "#ts": "updated_ts",
                    "#lt": "lock_token",
                    "#le": "lock_expires_ts",
                },
                ExpressionAttributeValues={
                    ":st": {"S": _serialize_record(cast(ExecutionRecord, patched))},
                    ":ts": {"N": str(now)},
                    ":lt": {"S": self.lock_token},
                    ":le": {"N": str(now + duration)},
                    ":now": {"N": str(now)},
                },
            )
            return True
        except Exception as e:
            if _check_condition_failure(e):
                return False
            raise

    # -- update_metadata ----------------------------------------------------

    def update_metadata(self, data: dict[str, Any]) -> bool:
        """Merge data into metadata dict.  Requires valid lock."""
        if self.lock_token is None:
            return False
        now = time.time()
        record = self.get()
        if record is None:
            return False
        patched: dict[str, Any] = dict(record)
        metadata: dict[str, Any] = dict(patched.get("metadata") or {})
        metadata.update(data)
        patched["metadata"] = metadata
        patched["updated_ts"] = now
        try:
            self._client.update_item(
                TableName=self.table_name,
                Key=self._item_key(),
                UpdateExpression="SET #st = :st, #ts = :ts",
                ConditionExpression="#lt = :lt AND #le > :now",
                ExpressionAttributeNames={
                    "#st": "state",
                    "#ts": "updated_ts",
                    "#lt": "lock_token",
                    "#le": "lock_expires_ts",
                },
                ExpressionAttributeValues={
                    ":st": {"S": _serialize_record(cast(ExecutionRecord, patched))},
                    ":ts": {"N": str(now)},
                    ":lt": {"S": self.lock_token},
                    ":now": {"N": str(now)},
                },
            )
            return True
        except Exception as e:
            if _check_condition_failure(e):
                return False
            raise

    # -- state transitions --------------------------------------------------

    def claim_running(self) -> bool:
        """Atomically claim pending work by transitioning ``pending -> running``."""
        now = time.time()
        record = self.get()
        if record is None or record["status"] != "pending":
            return False
        patched = dict(record)
        patched["status"] = "running"
        patched["heartbeat_ts"] = now
        patched["updated_ts"] = now
        try:
            self._client.update_item(
                TableName=self.table_name,
                Key=self._item_key(),
                UpdateExpression="SET #st = :st, #ts = :ts",
                ConditionExpression="#st = :old_state AND #ts = :old_updated_ts",
                ExpressionAttributeNames={
                    "#st": "state",
                    "#ts": "updated_ts",
                },
                ExpressionAttributeValues={
                    ":st": {"S": _serialize_record(cast(ExecutionRecord, patched))},
                    ":ts": {"N": str(now)},
                    ":old_state": {"S": _serialize_record(record)},
                    ":old_updated_ts": {"N": str(record["updated_ts"])},
                },
            )
            return True
        except Exception as e:
            if _check_condition_failure(e):
                return False
            raise

    def mark_running(self) -> bool:
        """pending -> running.  Requires valid lock."""
        return self._transition(from_status="pending", to_status="running")

    def mark_succeeded(self, dag_id: str) -> bool:
        """running -> succeeded.  Requires valid lock."""
        return self._transition(from_status="running", to_status="succeeded", dag_id=dag_id)

    def mark_failed(self, error: str) -> bool:
        """running -> failed.  Requires valid lock."""
        return self._transition(from_status="running", to_status="failed", error=error)

    def mark_done(self) -> bool:
        """succeeded|failed -> done. Requires valid lock."""
        record = self.get()
        if record is None or record["status"] not in {"succeeded", "failed"}:
            return False
        return self._transition(from_status=record["status"], to_status="done")

    # -- internal helpers ---------------------------------------------------

    def _transition(
        self,
        *,
        from_status: str,
        to_status: str,
        dag_id: str | None = None,
        error: str | None = None,
    ) -> bool:
        if self.lock_token is None:
            return False
        now = time.time()
        record = self.get()
        if record is None:
            return False
        if record["status"] != from_status:
            return False
        patched = dict(record)
        patched["status"] = to_status
        patched["heartbeat_ts"] = now
        patched["updated_ts"] = now
        if dag_id is not None:
            patched["dag_id"] = dag_id
        if error is not None:
            patched["error"] = error
        try:
            self._client.update_item(
                TableName=self.table_name,
                Key=self._item_key(),
                UpdateExpression="SET #st = :st, #ts = :ts",
                ConditionExpression="#lt = :lt AND #le > :now",
                ExpressionAttributeNames={
                    "#st": "state",
                    "#ts": "updated_ts",
                    "#lt": "lock_token",
                    "#le": "lock_expires_ts",
                },
                ExpressionAttributeValues={
                    ":st": {"S": _serialize_record(cast(ExecutionRecord, patched))},
                    ":ts": {"N": str(now)},
                    ":lt": {"S": self.lock_token},
                    ":now": {"N": str(now)},
                },
            )
            return True
        except Exception as e:
            if _check_condition_failure(e):
                return False
            raise
