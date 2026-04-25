"""S3-backed execution coordination and lineage helpers.

Public API:
    ExecutionState  - S3-backed lock + execution metadata helper
    ExecutionRecord - TypedDict for immutable execution records
    LockRecord      - TypedDict for the lock file contents
    LOCK_TTL        - Default lock time-to-live in seconds
"""

from __future__ import annotations

import json
import time
from typing import Any, Literal, TypedDict, cast
from urllib.parse import urlparse
from uuid import uuid4

from daggerml._internal.types import DmlRepoError

LOCK_TTL: float = 300.0


class LockRecord(TypedDict):
    lock_token: str
    lock_expires_ts: float


class ExecutionRecord(TypedDict):
    execution_number: int
    execution_id: str
    cache_key: str
    status: Literal["running"]
    state: dict[str, Any]


class ExecutionState:
    """S3-backed advisory mutex for function execution.

    Function-execution coordination lives under ``{prefix}/fn-exec/`` with:

    - ``locks/{cache_key}.json`` for the advisory mutex,
    - ``active/{cache_key}`` for the active execution number,
    - ``records/{cache_key}/{execution_number}.json`` for immutable execution records,
    - ``calls/...`` for caller/callee lineage.

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
        exec_prefix = f"{prefix}/fn-exec" if prefix else "fn-exec"
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
        import boto3

        return boto3.client("s3")

    def _execution_records_prefix(self) -> str:
        return f"{self._exec_prefix}/records/{self.cache_key}/"

    def _key_for_execution(self, execution_number: int) -> str:
        return f"{self._execution_records_prefix()}{execution_number}.json"

    def _key_for_calls_from_index(self, index_id: str) -> str:
        return f"{self._exec_prefix}/calls/from/index/{index_id}.json"

    def _key_for_calls_from_cache(self, caller_cache_key: str) -> str:
        return f"{self._exec_prefix}/calls/from/cache/{caller_cache_key}.json"

    def _key_for_calls_to_cache(self, callee_cache_key: str) -> str:
        return f"{self._exec_prefix}/calls/to/cache/{callee_cache_key}.json"

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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

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

    def read_active_execution_number(self) -> int | None:
        payload = self._get_object_bytes(self._active_key)
        if payload is None:
            return None
        raw = payload[0].decode().strip()
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError as e:
            raise DmlRepoError(f"Invalid active execution number for cache key {self.cache_key!r}: {raw!r}") from e

    def create_active_execution(self, execution_number: int) -> bool:
        return self._put_object(self._active_key, str(execution_number).encode(), if_none_match=True)

    def delete_active_execution(self) -> None:
        self._delete_object(self._active_key)

    def read_execution_record(self, execution_number: int) -> ExecutionRecord | None:
        payload = self._get_object_bytes(self._key_for_execution(execution_number))
        if payload is None:
            return None
        return json.loads(payload[0])

    def create_execution_record(self, execution_number: int, record: ExecutionRecord) -> bool:
        return self._write_json_if_absent(self._key_for_execution(execution_number), record)

    def next_execution_number(self) -> int:
        prefix = self._execution_records_prefix()
        token: str | None = None
        max_number = -1
        while True:
            kwargs: dict[str, Any] = {"Bucket": self._bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token is not None:
                kwargs["ContinuationToken"] = token
            resp = self._s3().list_objects_v2(**kwargs)
            for item in resp.get("Contents", []):
                key = item.get("Key", "")
                suffix = key.removeprefix(prefix)
                if not suffix.endswith(".json") or "/" in suffix:
                    continue
                stem = suffix[:-5]
                if stem.isdigit():
                    max_number = max(max_number, int(stem))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return max_number + 1

    def record_index_call(self, *, index_id: str, callee_cache_key: str, retries: int = 8) -> None:
        self._append_sorted_unique_string(self._key_for_calls_from_index(index_id), callee_cache_key, retries=retries)
        self._append_sorted_unique_member(
            self._key_for_calls_to_cache(callee_cache_key),
            member_key="indexes",
            value=index_id,
            retries=retries,
        )

    def record_fn_call(self, *, caller_cache_key: str, callee_cache_key: str, retries: int = 8) -> None:
        self._append_sorted_unique_string(
            self._key_for_calls_from_cache(caller_cache_key),
            callee_cache_key,
            retries=retries,
        )
        self._append_sorted_unique_member(
            self._key_for_calls_to_cache(callee_cache_key),
            member_key="cache_keys",
            value=caller_cache_key,
            retries=retries,
        )

    def _append_sorted_unique_string(self, key: str, value: str, *, retries: int) -> None:
        for _ in range(retries):
            current, etag = self._read_json(key)
            data = [] if current is None else list(current)
            merged = sorted({*data, value})
            if current is None:
                if self._write_json_if_absent(key, merged):
                    return
            else:
                if self._write_json_if_match(key, merged, cast(str, etag)):
                    return
        raise DmlRepoError(f"Failed to update execution lineage object: {key}")

    def _append_sorted_unique_member(self, key: str, *, member_key: str, value: str, retries: int) -> None:
        for _ in range(retries):
            current, etag = self._read_json(key)
            if current is None:
                merged = {"indexes": [], "cache_keys": []}
            else:
                merged = {
                    "indexes": list(current.get("indexes", [])),
                    "cache_keys": list(current.get("cache_keys", [])),
                }
            merged[member_key] = sorted({*merged[member_key], value})
            if current is None:
                if self._write_json_if_absent(key, merged):
                    return
            else:
                if self._write_json_if_match(key, merged, cast(str, etag)):
                    return
        raise DmlRepoError(f"Failed to update execution lineage object: {key}")
