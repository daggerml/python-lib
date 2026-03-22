from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, TypedDict, cast
from uuid import uuid4

from daggerml._internal.types import DmlRepoError
from daggerml.util import get_client

Status = Literal["pending", "running", "succeeded", "failed", "canceled"]
HEARTBEAT_STALENESS = 60.0


class StateRecord(TypedDict):
    version: int
    cache_key: str
    status: Status
    error: str | None
    heartbeat_ts: float
    metadata: dict[str, dict[str, Any]]


def is_stale(record: StateRecord) -> bool:
    return record["heartbeat_ts"] + HEARTBEAT_STALENESS < time.time()


class StateBase:
    def __init__(self, cache_key: str, **kwargs):
        if not isinstance(cache_key, str) or not cache_key:
            raise DmlRepoError("State cache_key must be a non-empty string")
        self.cache_key = cache_key
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _acquire_lock(self) -> bool:
        raise NotImplementedError

    def _release_lock(self) -> None:
        raise NotImplementedError

    def get(self) -> StateRecord | None:
        raise NotImplementedError

    def put_if_absent(self, state: StateRecord) -> bool:
        raise NotImplementedError

    def update(self, state: StateRecord) -> None:
        raise NotImplementedError

    def delete(self) -> None:
        raise NotImplementedError

    @contextmanager
    def lock(self):
        locked = self._acquire_lock()
        try:
            yield self if locked else None
        finally:
            if locked:
                self._release_lock()

    @staticmethod
    def _validate_record(state: dict[str, Any]) -> StateRecord:
        required = {
            "version",
            "cache_key",
            "status",
            "error",
            "heartbeat_ts",
            "metadata",
        }
        missing = sorted(required - set(state.keys()))
        if missing:
            raise DmlRepoError(f"State record missing required fields: {', '.join(missing)}")
        unknown = sorted(set(state.keys()) - required)
        if unknown:
            raise DmlRepoError(f"State record has unknown fields: {', '.join(unknown)}")
        if not isinstance(state["heartbeat_ts"], (int, float)):
            raise DmlRepoError("State record heartbeat_ts must be a number")
        if state["status"] not in {"pending", "running", "succeeded", "failed", "canceled"}:
            raise DmlRepoError("State record status must be one of pending|running|succeeded|failed|canceled")
        if not isinstance(state["metadata"], dict):
            raise DmlRepoError("State record metadata must be a dict")
        for key, value in cast(dict[str, Any], state["metadata"]).items():
            if not isinstance(key, str) or not key:
                raise DmlRepoError("State record metadata keys must be non-empty strings")
            if not isinstance(value, dict):
                raise DmlRepoError("State record metadata values must be dict objects")
        return cast(StateRecord, state)

    def init_record(
        self,
        *,
        status: Status = "pending",
        error: str | None = None,
        metadata: dict[str, dict[str, Any]] | None = None,
    ) -> StateRecord:
        return self._validate_record(
            {
                "version": 1,
                "cache_key": self.cache_key,
                "status": status,
                "error": error,
                "heartbeat_ts": time.time(),
                "metadata": metadata or {},
            }
        )

    def update_status(
        self,
        *,
        status: Status,
        error: str | None = None,
    ) -> StateRecord:
        record = self.get()
        if record is None:
            record = self.init_record()
        next_record: dict[str, Any] = dict(record)
        next_record.update(
            {
                "status": status,
                "error": error,
                "heartbeat_ts": time.time(),
            }
        )
        return self._validate_record(next_record)

    def set_executor_metadata(self, executor_id: str, data: dict[str, Any]) -> StateRecord:
        record = self.get()
        if record is None:
            record = self.init_record()
        next_record: dict[str, Any] = dict(record)
        metadata = dict(cast(dict[str, dict[str, Any]], next_record.get("metadata", {})))
        metadata[executor_id] = data
        next_record["metadata"] = metadata
        next_record["heartbeat_ts"] = time.time()
        return self._validate_record(next_record)

    def get_executor_metadata(self, executor_id: str) -> dict[str, Any]:
        return (self.get() or {}).get("metadata", {}).get(executor_id, {})


@dataclass
class LocalState(StateBase):
    cache_key: str
    cache_dir: str | None = None
    lock_timeout: float = 5.0
    poll_interval: float = 0.05
    _has_lock: bool = field(default=False, init=False)

    def __post_init__(self):
        # TODO: We should use the standard config dir: `<config_dir>/exec-state/<cache-namespace>/daggerml/`
        # but not yet
        base = self.cache_dir or os.getenv("DML_FN_CACHE_DIR") or str(Path.home() / ".daggerml" / "contrib-state")
        root = Path(base)
        root.mkdir(parents=True, exist_ok=True)
        self._state_path = root / f"{self.cache_key}.json"
        self._lock_path = root / f"{self.cache_key}.lock"

    def _acquire_lock(self) -> bool:
        deadline = time.time() + self.lock_timeout
        while time.time() < deadline:
            try:
                fd = os.open(str(self._lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode("utf-8"))
                os.close(fd)
                self._has_lock = True
                return True
            except FileExistsError:
                try:
                    age = time.time() - self._lock_path.stat().st_mtime
                    if age > self.lock_timeout:
                        self._lock_path.unlink(missing_ok=True)
                        continue
                except FileNotFoundError:
                    continue
                time.sleep(self.poll_interval)
        return False

    def _release_lock(self) -> None:
        if self._has_lock and self._lock_path.exists():
            self._lock_path.unlink(missing_ok=True)
        self._has_lock = False

    def get(self) -> StateRecord | None:
        if not self._state_path.exists():
            return None
        return self._validate_record(json.loads(self._state_path.read_text()))

    def put_if_absent(self, state: StateRecord) -> bool:
        if self._state_path.exists():
            return False
        state = self._validate_record(dict(state))
        self._state_path.write_text(json.dumps(state, separators=(",", ":"), sort_keys=True))
        return True

    def update(self, state: StateRecord) -> None:
        state = self._validate_record(dict(state))
        self._state_path.write_text(json.dumps(state, separators=(",", ":"), sort_keys=True))

    def delete(self) -> None:
        self._state_path.unlink(missing_ok=True)


@dataclass
class DynamoState(StateBase):
    cache_key: str
    table_name: str = field(default_factory=lambda: os.getenv("DML_DYNAMODB_TABLE"))  # pyright: ignore[reportAssignmentType]
    lock_timeout: float = 5.0
    owner_id: str = field(default_factory=lambda: str(uuid4()))
    client: Any = field(default_factory=lambda: get_client("dynamodb"))

    def __post_init__(self):
        if not self.table_name:
            raise DmlRepoError("DynamoState requires table_name parameter or DML_DYNAMODB_TABLE env var")

    def _item_key(self):
        return {"cache_key": {"S": self.cache_key}}

    def _acquire_lock(self) -> bool:
        try:
            self.client.update_item(
                TableName=self.table_name,
                Key=self._item_key(),
                UpdateExpression="SET #lk = :lk, #ts = :ts",
                ConditionExpression="attribute_not_exists(#lk) OR #lk = :lk OR #ts < :stale",
                ExpressionAttributeNames={"#lk": "lock_owner", "#ts": "updated_ts"},
                ExpressionAttributeValues={
                    ":lk": {"S": self.owner_id},
                    ":ts": {"N": str(time.time())},
                    ":stale": {"N": str(time.time() - self.lock_timeout)},
                },
            )
            return True
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            if code == "ConditionalCheckFailedException":
                return False
            raise

    def _release_lock(self) -> None:
        try:
            self.client.update_item(
                TableName=self.table_name,
                Key=self._item_key(),
                UpdateExpression="REMOVE #lk",
                ConditionExpression="#lk = :lk",
                ExpressionAttributeNames={"#lk": "lock_owner"},
                ExpressionAttributeValues={":lk": {"S": self.owner_id}},
            )
        except Exception:
            return

    def get(self) -> StateRecord | None:
        resp = self.client.get_item(TableName=self.table_name, Key=self._item_key(), ConsistentRead=True)
        item = resp.get("Item")
        if item is None:
            return None
        raw = item.get("state", {}).get("S")
        if not raw:
            return None
        return self._validate_record(json.loads(raw))

    def put_if_absent(self, state: StateRecord) -> bool:
        state = self._validate_record(dict(state))
        try:
            self.client.put_item(
                TableName=self.table_name,
                Item={
                    "cache_key": {"S": self.cache_key},
                    "state": {"S": json.dumps(state, separators=(",", ":"), sort_keys=True)},
                    "updated_ts": {"N": str(time.time())},
                    "lock_owner": {"S": self.owner_id},
                },
                ConditionExpression="attribute_not_exists(cache_key)",
            )
            return True
        except Exception as e:
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            if code == "ConditionalCheckFailedException":
                return False
            raise

    def update(self, state: StateRecord) -> None:
        state = self._validate_record(dict(state))
        self.client.update_item(
            TableName=self.table_name,
            Key=self._item_key(),
            UpdateExpression="SET #st = :st, #ts = :ts",
            ConditionExpression="#lk = :lk",
            ExpressionAttributeNames={"#st": "state", "#ts": "updated_ts", "#lk": "lock_owner"},
            ExpressionAttributeValues={
                ":st": {"S": json.dumps(state, separators=(",", ":"), sort_keys=True)},
                ":ts": {"N": str(time.time())},
                ":lk": {"S": self.owner_id},
            },
        )

    def delete(self) -> None:
        try:
            self.client.delete_item(
                TableName=self.table_name,
                Key=self._item_key(),
                ConditionExpression="#lk = :lk",
                ExpressionAttributeNames={"#lk": "lock_owner"},
                ExpressionAttributeValues={":lk": {"S": self.owner_id}},
            )
        except Exception:
            return


def state_from_comms(cache_key: str, comms: dict[str, Any]) -> StateBase:
    kind = comms.get("kind")
    spec = comms.get("spec")
    if not isinstance(kind, str) or not isinstance(spec, dict):
        raise DmlRepoError("State comms requires kind/spec")
    cls = LocalState if kind == "local" else DynamoState if kind == "dynamo" else None
    if cls is None:
        raise DmlRepoError(f"Unsupported comms kind: {kind}")
    return cls(cache_key, **spec)


@contextmanager
def lock_from_comms(cache_key: str, comms: dict[str, Any]):
    state = state_from_comms(cache_key, comms)
    with state.lock() as state:
        yield state
