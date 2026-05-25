"""S3-backed execution coordination and lineage helpers.

Public API:
    AdapterIO       - Scoped S3 stdin/stdout surrogate for fire-and-monitor executors
    ExecutionState  - S3-backed lock + execution metadata helper
    CancelledExecutionError - Raised when execution updates are interrupted by cancellation
    LaunchState     - TypedDict for caller-owned resumable launch objects
    ExecutionRecord - TypedDict for runtime-owned execution lifecycle objects
    LockRecord      - TypedDict for the lock file contents
    LOCK_TTL        - Default lock time-to-live in seconds
"""

from __future__ import annotations

import json
import time
from typing import Any, Literal, Optional, TypedDict, cast
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


class CancelledExecutionError(Exception):
    pass


class LaunchState(TypedDict):
    execution_id: str
    cache_key: str
    created_at: int
    resume_state: dict[str, Any]


class ExecutionRecord(TypedDict):
    execution_id: str
    cache_key: str | None
    lifecycle: Literal["running", "cancel-pending", "cancelled", "succeeded", "failed"]
    updated_at: int
    spawned_execution_ids: list[str]
    cancellation_requested_by: str | None


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

    def write_input(self, data, *, raw: bool = True) -> str:
        """Write ``data`` to the input S3 key and return ``input_uri``."""
        self._state._cas_item(self._input_key).write(data, force=True, raw=raw)
        return self.input_uri

    def read_output(self, raw: bool = True):
        """Read the output S3 key.  Returns ``None`` if not yet written."""
        return self._state._cas_item(self._output_key).read(raw=raw)


class CasItem:
    """S3-backed content-addressable storage item for function execution.

    Parameters
    ----------
    bucket: str
    key: str
    """

    def __init__(self, bucket: str, key: str, client=None) -> None:
        self.bucket = bucket
        self.key = key
        self.etag = None
        self.body = None
        self._client = client or boto3.client("s3")

    @property
    def uri(self) -> str:
        """S3 URI for this CAS item."""
        return f"s3://{self.bucket}/{self.key}"

    def read(self, raw: bool = False) -> str | None:
        """Factory method to create a CasItem by fetching the object from S3.

        Returns None if the object does not exist.
        """
        try:
            resp = self._client.get_object(Bucket=self.bucket, Key=self.key)
            self.etag = resp["ETag"].strip('"')
            body = resp["Body"].read().decode().strip()
            self.body = body if raw else json.loads(body)
            return self.body
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404"):
                return None
            raise

    def _write(self, data, new: bool, raw: bool, force: bool = False) -> bool:
        """Overwrite the body with new data and update the ETag."""
        kw = {}
        if force:
            pass
        elif new:
            kw["IfNoneMatch"] = "*"
        else:
            kw["IfMatch"] = self.etag
        if not raw:
            data = json.dumps(data, separators=(",", ":"), sort_keys=True)
        try:
            self._client.put_object(Bucket=self.bucket, Key=self.key, Body=data.encode(), **kw)
            return True
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if code in ("PreconditionFailed", "412"):
                return False
            raise
        finally:
            self.read(raw=raw)

    def write(self, data, raw: bool = False, force: bool = False) -> bool:
        """Overwrite the body with new data and update the ETag."""
        return self._write(data, new=True, raw=raw, force=force)

    def update(self, data, raw: bool = False) -> None:
        """Update the body with new data, using ETag for optimistic concurrency."""
        if self.etag is None:
            raise DmlRepoError(f"CAS item must be read before update: {self.uri}")
        if not self._write(data, new=False, raw=raw):
            raise DmlRepoError(f"CAS item update failed due to ETag mismatch: {self.uri}")

    def delete(self) -> None:
        """DELETE the lock file; no-op if already absent."""
        try:
            self._client.delete_object(Bucket=self.bucket, Key=self.key)
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404"):
                return
            raise


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

    def __init__(self, cache_key: Optional[str] = None, *, remote_root: str) -> None:
        parsed = urlparse(remote_root)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise DmlRepoError(f"ExecutionState remote_root must be a valid s3:// URI, got: {remote_root!r}")
        bucket = parsed.netloc
        prefix = parsed.path.strip("/")
        exec_prefix = f"{prefix}/dml" if prefix else "dml"
        self.cache_key = cache_key
        self._bucket = bucket
        self._exec_prefix = exec_prefix
        self._cas: dict[str, CasItem] = {}
        if cache_key is not None:
            self._lock_key = f"{exec_prefix}/locks/{cache_key}.json"
            self._active_key = f"{exec_prefix}/active/{cache_key}"
            self._lock_token: str | None = None

    @classmethod
    def from_execution_id(cls, execution_id: str, *, remote_root: str) -> ExecutionState:
        """Instantiate from an active execution ID by reading the launch state."""
        temp_state = cls(remote_root=remote_root)
        record = temp_state.read_execution_record(execution_id)
        if record is None:
            raise DmlRepoError(f"No execution record found for execution_id: {execution_id}")
        return cls(cache_key=record["cache_key"], remote_root=remote_root)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _s3():
        return boto3.client("s3", config=Config(max_pool_connections=S3_MAX_POOL_CONNECTIONS))

    def _cas_item(self, key: str) -> CasItem:
        if key not in self._cas:
            self._cas[key] = CasItem(self._bucket, key, client=self._s3())
        return self._cas[key]

    def _key_for_launch_state(self, execution_id: str) -> str:
        return f"{self._exec_prefix}/exec/launch/{execution_id}.json"

    def _key_for_execution(self, execution_id: str) -> str:
        return f"{self._exec_prefix}/exec/state/{execution_id}.json"

    def _key_for_edge_prefix(self, callee_execution_id: str) -> str:
        return f"{self._exec_prefix}/exec/edges/{callee_execution_id}/"

    def _key_for_edge(self, callee_execution_id: str, caller_execution_id: str) -> str:
        return f"{self._key_for_edge_prefix(callee_execution_id)}{caller_execution_id}.json"

    @staticmethod
    def _lifecycle_rank(lifecycle: str) -> int:
        ranks = {
            "running": 0,
            "cancel-pending": 1,
            "cancelled": 2,
            "succeeded": 3,
            "failed": 3,
        }
        if lifecycle not in ranks:
            raise DmlRepoError(f"Invalid execution lifecycle: {lifecycle}")
        return ranks[lifecycle]

    def _merge_execution_record(self, current: ExecutionRecord, incoming: ExecutionRecord) -> ExecutionRecord:
        lifecycle = current["lifecycle"]
        if self._lifecycle_rank(incoming["lifecycle"]) > self._lifecycle_rank(current["lifecycle"]):
            lifecycle = incoming["lifecycle"]
        spawned_execution_ids = sorted(
            {*current.get("spawned_execution_ids", []), *incoming.get("spawned_execution_ids", [])}
        )
        cancellation_requested_by = (
            incoming["cancellation_requested_by"]
            if incoming["cancellation_requested_by"] is not None
            else current["cancellation_requested_by"]
        )
        merged: ExecutionRecord = {
            "execution_id": current["execution_id"],
            "cache_key": current["cache_key"],
            "lifecycle": lifecycle,
            "updated_at": max(current["updated_at"], incoming["updated_at"]),
            "spawned_execution_ids": spawned_execution_ids,
            "cancellation_requested_by": cancellation_requested_by,
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
        existing = cast(LockRecord, self._cas_item(self._lock_key).read())
        if existing is not None:
            if existing["lock_expires_ts"] > now:
                # Lock is currently held by someone else
                return False
            # Lock is expired — steal it
            self._cas_item(self._lock_key).delete()
        record = cast(LockRecord, {"lock_token": str(uuid4()), "lock_expires_ts": now + ttl})
        if not self._cas_item(self._lock_key).write(record):
            # 412 — concurrent writer grabbed it first
            return False
        self._lock_token = record["lock_token"]
        return True

    def unlock(self) -> None:
        """Release the advisory lock by deleting the lock file.

        This is a best-effort delete; if the file is already absent (e.g.
        expired and stolen), the call is a no-op.
        """
        self._lock_token = None
        self._cas_item(self._lock_key).delete()

    def read_active_execution_id(self) -> str | None:
        return self._cas_item(self._active_key).read(raw=True)

    def create_active_execution(self, execution_id: str) -> bool:
        return self._cas_item(self._active_key).write(execution_id, raw=True)

    def delete_active_execution(self) -> None:
        self._cas_item(self._active_key).delete()

    def read_launch_state(self, execution_id: str) -> LaunchState:
        return cast(LaunchState, self._cas_item(self._key_for_launch_state(execution_id)).read())

    def create_launch_state(self, launch_state: LaunchState) -> bool:
        return self._cas_item(self._key_for_launch_state(launch_state["execution_id"])).write(launch_state)

    def update_launch_state(self, launch_state: LaunchState) -> None:
        self._cas_item(self._key_for_launch_state(launch_state["execution_id"])).update(launch_state)

    def read_execution_record(self, execution_id: str) -> ExecutionRecord:
        resp = self._cas_item(self._key_for_execution(execution_id)).read()
        if resp is None:
            raise DmlRepoError(f"No execution record found for execution_id: {execution_id}")
        return cast(ExecutionRecord, resp)

    def create_execution_record(self, record: ExecutionRecord) -> bool:
        return self._cas_item(self._key_for_execution(record["execution_id"])).write(record)

    def update_execution_record(self, record: ExecutionRecord) -> None:
        self._cas_item(self._key_for_execution(record["execution_id"])).update(record)

    def record_execution_dependency(self, caller_execution_id: str, callee_execution_id: str) -> None:
        edge = {"caller_execution_id": caller_execution_id, "callee_execution_id": callee_execution_id}
        key = self._key_for_edge(callee_execution_id, caller_execution_id)
        self._cas_item(key).write(edge, force=True)

    def delete_execution_dependency(self, *, caller_execution_id: str, callee_execution_id: str) -> None:
        return self._cas_item(self._key_for_edge(callee_execution_id, caller_execution_id)).delete()

    def list_execution_callers(self, callee_execution_id: str) -> list[str]:
        prefix = self._key_for_edge_prefix(callee_execution_id)
        paginator = self._s3().get_paginator("list_objects_v2")
        callers: list[str] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            callers.extend([x["Key"].split("/")[-1].removesuffix(".json") for x in page.get("Contents", [])])
        return callers
