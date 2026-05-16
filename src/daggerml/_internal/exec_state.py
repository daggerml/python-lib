"""S3-backed execution coordination and lineage helpers.

Public API:
    AdapterIO       - Scoped S3 stdin/stdout surrogate for fire-and-monitor executors
    ExecutionState  - S3-backed lock + execution metadata helper
    ExecutionRecord - TypedDict for mutable execution state objects
    LockRecord      - TypedDict for the lock file contents
    LOCK_TTL        - Default lock time-to-live in seconds
"""

from __future__ import annotations

import json
import time
from typing import Any, Literal, TypedDict, cast
from urllib.parse import urlparse
from uuid import uuid4

import boto3
from botocore.config import Config

from daggerml._internal.types import DmlRepoError

LOCK_TTL: float = 300.0
S3_MAX_POOL_CONNECTIONS = 20


class LockRecord(TypedDict):
    lock_token: str
    lock_expires_ts: float


class ExecutionRecord(TypedDict):
    execution_id: str
    cache_key: str
    created_at: int
    status: Literal["running", "cancel-requested", "cancelled", "succeeded", "failed"]
    state: dict[str, Any] | None
    dependencies: list[str]
    updated_at: int
    cancel_requested_by: str | None


class AdapterIO:
    """Scoped S3 stdin/stdout surrogate for fire-and-monitor executors.

    Used by executors that launch a sub-adapter as a detached process (e.g.
    Docker container, AWS Batch job) where direct stdin/stdout piping is not
    possible.  Paths are derived deterministically from ``(cache_key, exec_id,
    name)`` so both ``start()`` and ``poll()`` can access the same objects
    without storing URIs in executor state.

    All keys live under ``{protocol-prefix}/io/{cache_key}/{exec_id}/{name}/``.

    Obtain via ``ExecutionState.adapter_io(exec_id, name)`` — do not construct
    directly.

    Parameters
    ----------
    exec_id:
        UUID identifying the current execution attempt.
    name:
        Caller-chosen identifier, conventionally ``"{adapter}:{executor}"``
        (e.g. ``"local:docker"``, ``"lambda:batch"``).
    """

    def __init__(self, state: "ExecutionState", exec_id: str, name: str) -> None:
        prefix = f"{state._exec_prefix}/io/{state.cache_key}/{exec_id}/{name}"
        self._state = state
        self._input_key = f"{prefix}/input.json"
        self._output_key = f"{prefix}/output.json"

    @property
    def input_uri(self) -> str:
        """S3 URI for the sub-adapter input payload (no S3 call made)."""
        return f"s3://{self._state._bucket}/{self._input_key}"

    @property
    def output_uri(self) -> str:
        """S3 URI for the sub-adapter output result (no S3 call made)."""
        return f"s3://{self._state._bucket}/{self._output_key}"

    def write_input(self, data: bytes) -> str:
        """Write ``data`` to the input S3 key and return ``input_uri``."""
        self._state._put_object(self._input_key, data)
        return self.input_uri

    def read_output(self) -> bytes | None:
        """Read the output S3 key.  Returns ``None`` if not yet written."""
        result = self._state._get_object_bytes(self._output_key)
        return result[0] if result is not None else None



class ExecutionState:
    """S3-backed advisory mutex for function execution.

    Function-execution coordination lives under ``{prefix}/dml/`` with:

    - ``locks/{cache_key}.json`` for the advisory mutex,
    - ``active/{cache_key}`` for the active execution id,
    - ``exec/state/{execution_id}.json`` for mutable execution state,
    - ``exec/edges/<callee_execution_id>/<caller_execution_id>.json`` for canonical lineage,
    - ``exec/invalidate/{execution_id}.json`` for invalidation tombstones,
    - ``io/{cache_key}/{exec_id}/{name}/`` for adapter I/O (see :class:`AdapterIO`).

    Lock lifecycle is create-only (``If-None-Match: *``) then delete — no
    updates.

    Parameters
    ----------
    cache_key:
        Unique identifier for this execution (typically the argv_ref id).
    remote_root:
        S3 URI of the form ``s3://bucket[/prefix]``.  Raises ``DmlRepoError``
        if absent or malformed.
    """

    def __init__(self, cache_key: str, *, remote_root: str) -> None:
        if not isinstance(cache_key, str) or not cache_key:
            raise DmlRepoError("ExecutionState cache_key must be a non-empty string")
        parsed = urlparse(remote_root)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise DmlRepoError(
                f"ExecutionState remote_root must be a valid s3:// URI, got: {remote_root!r}"
            )
        bucket = parsed.netloc
        prefix = parsed.path.strip("/")
        exec_prefix = f"{prefix}/dml" if prefix else "dml"
        self.cache_key = cache_key
        self._bucket = bucket
        self._exec_prefix = exec_prefix
        self._lock_key = f"{exec_prefix}/locks/{cache_key}.json"
        self._active_key = f"{exec_prefix}/active/{cache_key}"
        self._lock_token: str | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _s3():
        return boto3.client("s3", config=Config(max_pool_connections=S3_MAX_POOL_CONNECTIONS))

    def _key_for_execution(self, execution_id: str) -> str:
        return f"{self._exec_prefix}/exec/state/{execution_id}.json"

    def _key_for_edge(self, callee_execution_id: str, caller_execution_id: str) -> str:
        return f"{self._exec_prefix}/exec/edges/{callee_execution_id}/{caller_execution_id}.json"

    def _key_for_edge_prefix(self, callee_execution_id: str) -> str:
        return f"{self._exec_prefix}/exec/edges/{callee_execution_id}/"

    def _key_for_invalidation(self, execution_id: str) -> str:
        return f"{self._exec_prefix}/exec/invalidate/{execution_id}.json"

    def _get_object_bytes(self, key: str) -> tuple[bytes, str] | None:
        """Return object bytes and ETag, or None if the file does not exist."""
        try:
            resp = self._s3().get_object(Bucket=self._bucket, Key=key)
            return resp["Body"].read(), resp["ETag"].strip('"')
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404"):
                return None
            raise

    def _get_object(self) -> LockRecord | None:
        payload = self._get_object_bytes(self._lock_key)
        return None if payload is None else json.loads(payload[0])

    def _put_object(self, key: str, body: bytes, *, if_match: str | None = None, if_none_match: bool = False) -> bool:
        """Conditional PUT. Returns False on precondition failure."""
        try:
            kwargs: dict[str, Any] = {
                "Bucket": self._bucket,
                "Key": key,
                "Body": body,
            }
            if key.endswith(".json"):
                kwargs["ContentType"] = "application/json"
            if if_match is not None:
                kwargs["IfMatch"] = if_match
            if if_none_match:
                kwargs["IfNoneMatch"] = "*"
            self._s3().put_object(**kwargs)
            return True
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if code in ("PreconditionFailed", "412"):
                return False
            raise

    def _put_object_if_absent(self, record: LockRecord) -> bool:
        """PUT with ``If-None-Match: *``.  Returns True on success, False on 412."""
        return self._put_object(
            self._lock_key,
            json.dumps(record, separators=(",", ":"), sort_keys=True).encode(),
            if_none_match=True,
        )

    def _delete_object(self, key: str) -> None:
        """DELETE the lock file; no-op if already absent."""
        try:
            self._s3().delete_object(Bucket=self._bucket, Key=key)
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404"):
                return
            raise

    def _read_json(self, key: str) -> tuple[Any, str] | tuple[None, None]:
        payload = self._get_object_bytes(key)
        if payload is None:
            return None, None
        return json.loads(payload[0]), payload[1]

    def _write_json_if_absent(self, key: str, value: Any) -> bool:
        return self._put_object(
            key,
            json.dumps(value, separators=(",", ":"), sort_keys=True).encode(),
            if_none_match=True,
        )

    def _write_json_if_match(self, key: str, value: Any, etag: str) -> bool:
        return self._put_object(
            key,
            json.dumps(value, separators=(",", ":"), sort_keys=True).encode(),
            if_match=etag,
        )

    @staticmethod
    def _status_rank(status: str) -> int:
        ranks = {
            "running": 0,
            "cancel-requested": 1,
            "cancelled": 2,
            "succeeded": 3,
            "failed": 3,
        }
        if status not in ranks:
            raise DmlRepoError(f"Invalid execution status: {status}")
        return ranks[status]

    def _merge_execution_record(self, current: ExecutionRecord, incoming: ExecutionRecord) -> ExecutionRecord:
        state = current["state"] if current["state"] is not None else incoming["state"]
        status = current["status"]
        if self._status_rank(incoming["status"]) > self._status_rank(current["status"]):
            status = incoming["status"]
        created_at = current["created_at"]
        dependencies = sorted({*current.get("dependencies", []), *incoming.get("dependencies", [])})
        cancel_requested_by = current["cancel_requested_by"] or incoming["cancel_requested_by"]
        merged: ExecutionRecord = {
            "execution_id": current["execution_id"],
            "cache_key": current["cache_key"],
            "created_at": created_at,
            "status": status,
            "state": state,
            "dependencies": dependencies,
            "updated_at": max(current["updated_at"], incoming["updated_at"]),
            "cancel_requested_by": cancel_requested_by,
        }
        return merged

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def adapter_io(self, exec_id: str, name: str) -> AdapterIO:
        """Return a scoped :class:`AdapterIO` for the given execution attempt.

        Parameters
        ----------
        exec_id:
            UUID identifying the current execution attempt.
        name:
            Caller-chosen identifier, conventionally ``"{adapter}:{executor}"``
            (e.g. ``"local:docker"``, ``"lambda:batch"``).
        """
        return AdapterIO(self, exec_id, name)

    def lock(self, ttl: float = LOCK_TTL) -> bool:
        """Acquire the advisory lock.

        Algorithm:
        1. GET existing file.
        2. If absent: PUT with ``If-None-Match: *``.
        3. If present and **expired**: DELETE then PUT.
        4. If present and **held**: return ``False``.
        5. A 412 from S3 on step 2 means a concurrent writer won — return ``False``.

        Returns True on success, False if the lock is currently held.
        """
        now = time.time()
        existing = self._get_object()

        if existing is not None:
            if existing["lock_expires_ts"] > now:
                # Lock is currently held by someone else
                return False
            # Lock is expired — steal it
            self._delete_object(self._lock_key)

        token = str(uuid4())
        record: LockRecord = {
            "lock_token": token,
            "lock_expires_ts": now + ttl,
        }
        if not self._put_object_if_absent(record):
            # 412 — concurrent writer grabbed it first
            return False

        self._lock_token = token
        return True

    def unlock(self) -> None:
        """Release the advisory lock by deleting the lock file.

        This is a best-effort delete; if the file is already absent (e.g.
        expired and stolen), the call is a no-op.
        """
        self._lock_token = None
        self._delete_object(self._lock_key)

    def read_active_execution_id(self) -> str | None:
        payload = self._get_object_bytes(self._active_key)
        if payload is None:
            return None
        raw = payload[0].decode().strip()
        if not raw:
            return None
        return raw

    def create_active_execution(self, execution_id: str) -> bool:
        return self._put_object(self._active_key, execution_id.encode(), if_none_match=True)

    def delete_active_execution(self) -> None:
        self._delete_object(self._active_key)

    def read_execution_record(self, execution_id: str) -> ExecutionRecord | None:
        payload = self._get_object_bytes(self._key_for_execution(execution_id))
        if payload is None:
            return None
        return json.loads(payload[0])

    def create_execution_record(self, record: ExecutionRecord) -> bool:
        return self._write_json_if_absent(self._key_for_execution(record["execution_id"]), record)

    def update_execution_record(self, record: ExecutionRecord, *, retries: int = 8) -> ExecutionRecord:
        key = self._key_for_execution(record["execution_id"])
        for _ in range(retries):
            current, etag = self._read_json(key)
            if current is None:
                if self._write_json_if_absent(key, record):
                    return record
            else:
                merged = self._merge_execution_record(cast(ExecutionRecord, current), record)
                if self._write_json_if_match(key, merged, cast(str, etag)):
                    return merged
        raise DmlRepoError(f"Failed to update execution state object: {key}")

    def record_execution_dependency(
        self,
        *,
        caller_execution_id: str,
        callee_execution_id: str,
        retries: int = 8,
    ) -> None:
        edge = {
            "caller_execution_id": caller_execution_id,
            "callee_execution_id": callee_execution_id,
        }
        key = self._key_for_edge(callee_execution_id, caller_execution_id)
        for _ in range(retries):
            if self._write_json_if_absent(key, edge):
                return
            existing, _etag = self._read_json(key)
            if existing == edge:
                return
        raise DmlRepoError(f"Failed to write execution edge object: {key}")

    def create_invalidation_record(
        self,
        *,
        execution_id: str,
        cache_key: str,
        requested_by: str,
        requested_at: int,
    ) -> bool:
        return self._write_json_if_absent(
            self._key_for_invalidation(execution_id),
            {
                "execution_id": execution_id,
                "cache_key": cache_key,
                "requested_by": requested_by,
                "requested_at": requested_at,
            },
        )

    def list_execution_callers(self, callee_execution_id: str) -> list[str]:
        prefix = self._key_for_edge_prefix(callee_execution_id)
        paginator = self._s3().get_paginator("list_objects_v2")
        callers: list[str] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                payload = self._get_object_bytes(key)
                if payload is None:
                    continue
                edge = json.loads(payload[0])
                caller = edge.get("caller_execution_id")
                if isinstance(caller, str) and caller:
                    callers.append(caller)
        return callers
