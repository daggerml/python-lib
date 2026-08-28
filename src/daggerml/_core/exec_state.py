"""Split S3 execution state, cache coordination, and execution lineage."""

from __future__ import annotations

import json
import logging
import math
import random
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import InitVar, asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Literal, Sequence, TypedDict, cast
from uuid import uuid4

from daggerml._core.remote import Remote
from daggerml._core.s3_cas import CasItem, CasItemConflict, S3Remote
from daggerml._core.types import (
    BadExecutionStatusError,
    CanceledExecutionError,
    Dag,
    DmlDB,
    DmlRepoError,
    Error,
    Ref,
    Runnable,
)
from daggerml._core.util import uuid7
from daggerml.util import get_client

if TYPE_CHECKING:
    import boto3

LOCK_TTL = 300.0
COORDINATION_CAS_ATTEMPTS = 10
COORDINATION_CAS_BACKOFF_SECONDS = 0.01
COORDINATION_CAS_MAX_BACKOFF_SECONDS = 1.0
EXECUTION_LIFECYCLES = Literal["pending", "running", "succeeded", "failed", "cancel-pending", "canceled"]
GRAPH_LIFECYCLES = EXECUTION_LIFECYCLES
logger = logging.getLogger(__name__)


class ExecutionLock(TypedDict):
    owner: str
    ttl: float


class ControlRecord(TypedDict):
    requested_by: str
    requested_at: int


class ExecutionMetadata(TypedDict):
    execution_id: str
    cache_key: str | None
    argv_ref: str | None
    created_at: int


class ExecutionSemanticState(TypedDict):
    lifecycle: EXECUTION_LIFECYCLES
    result_ref: str | None
    result_source: Literal["runtime", "adapter-error"] | None
    spawned_execution_ids: list[str]
    child_execution_ids: list[str]
    cancelation: ControlRecord | None
    invalidation: ControlRecord | None
    updated_at: int


class CleanupRecord(TypedDict):
    status: Literal["complete", "failed"]
    error: str | None


class ExecutionDriver(TypedDict):
    lock: ExecutionLock | None
    not_before: int | None
    adapter_state: dict[str, Any] | None
    cleanup: CleanupRecord | None


class ExecutionRecord(TypedDict):
    metadata: ExecutionMetadata
    state: ExecutionSemanticState
    driver: ExecutionDriver


class InvalidationRecord(TypedDict):
    execution_id: str
    cache_key: str | None
    requested_by: str
    requested_at: int


class InvalidationResponse(TypedDict):
    total_time: float
    invalidations: list[InvalidationRecord]


class CacheStateDescription(TypedDict):
    execution_id: str
    result_ref: str | None
    lifecycle: EXECUTION_LIFECYCLES


class RemotePayload(TypedDict):
    root: str


class AdapterInvokeRequest(TypedDict):
    operation: Literal["invoke"]
    cache_key: str
    execution_id: str
    remote: RemotePayload
    runnable: dict
    adapter_state: dict | None
    scratch_uri: str


class AdapterCleanupRequest(TypedDict):
    operation: Literal["cleanup"]
    cache_key: str
    execution_id: str
    remote: RemotePayload
    runnable: dict
    adapter_state: dict | None
    scratch_uri: str
    result_ref: str


class AdapterCancelRequest(TypedDict):
    operation: Literal["cancel"]
    cache_key: str
    execution_id: str
    argv_ref: str
    remote: RemotePayload
    runnable: dict
    adapter_state: dict | None
    scratch_uri: str
    requested_by: str | None


AdapterInvokeResponse = dict[str, Any]
AdapterCleanupResponse = dict[str, Any]
AdapterCancelResponse = dict[str, Any]


def validate_adapter_response(
    response: object, *, success_status: Literal["success", "cancelled"] = "success"
) -> dict[str, Any]:
    if not isinstance(response, dict):
        raise DmlRepoError("Adapter response must be a JSON object")
    if set(response) - {"status", "adapter_state", "retry_after_ms", "error"}:
        raise DmlRepoError("Adapter response contains unsupported fields")
    status = response.get("status")
    if not isinstance(status, str) or not status:
        raise DmlRepoError("Adapter response must contain a non-empty status")
    adapter_state = response.get("adapter_state")
    if "adapter_state" in response and adapter_state is not None and not isinstance(adapter_state, dict):
        raise DmlRepoError("Adapter response adapter_state must be an object or null")
    if "retry_after_ms" in response:
        retry_after_ms = response["retry_after_ms"]
        if not isinstance(retry_after_ms, int) or isinstance(retry_after_ms, bool) or retry_after_ms < 0:
            raise DmlRepoError("Adapter response retry_after_ms must be a nonnegative integer")
    error = response.get("error")
    if error is not None and not isinstance(error, str):
        raise DmlRepoError("Adapter response error must be a string or null")
    if status == "running":
        raise DmlRepoError("Adapter response status 'running' is unsupported")
    if status == "retry":
        if not isinstance(adapter_state, dict):
            raise DmlRepoError("Retry adapter response requires object adapter_state")
        if error is not None:
            raise DmlRepoError("Retry adapter response cannot contain error text")
    elif status == success_status:
        if error is not None:
            raise DmlRepoError(f"Successful adapter response '{status}' cannot contain error text")
        if "retry_after_ms" in response:
            raise DmlRepoError(f"Successful adapter response '{status}' cannot contain retry_after_ms")
    elif status in {"success", "cancelled"}:
        raise DmlRepoError(f"Adapter response status '{status}' is invalid for this operation")
    else:
        if not isinstance(error, str) or not error:
            raise DmlRepoError("Failed adapter response requires error text")
        if "retry_after_ms" in response:
            raise DmlRepoError("Failed adapter response cannot contain retry_after_ms")
    return response


class ExecutionGraphNode(TypedDict):
    execution_id: str
    cache_key: str | None
    lifecycle: GRAPH_LIFECYCLES
    updated_at: int
    created_at: int
    cancel_requested_by: str | None
    children: list[str]
    spawned: list[str]


class ExecutionGraph(TypedDict):
    roots: list[str]
    nodes: dict[str, ExecutionGraphNode]


def _is_ref(value: object, namespace: str) -> bool:
    try:
        return isinstance(value, str) and Ref(value).ns() == namespace
    except (TypeError, ValueError):
        return False


def _timestamp(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


class CancellationError(CanceledExecutionError):
    def __init__(self, message: str, *, lifecycle: str | None = None):
        super().__init__(message, lifecycle=lifecycle)
        self.type = "cancellationerror"


@dataclass
class ExecutionState:
    root_uri: str
    n_workers: int
    client: InitVar["boto3.client"]
    cache_key: str | None = None
    _store: S3Remote = field(init=False)
    _owned_reservations: dict[str, dict[str, CasItem]] = field(init=False, default_factory=dict)

    def __post_init__(self, client) -> None:
        self._store = S3Remote(self.root_uri.rstrip("/") + "/exec", client=client)
        self._remote = Remote(self.root_uri, self.n_workers, client=client)

    @classmethod
    def from_execution_id(cls, execution_id: str, *, root_uri: str, n_workers: int, client=None) -> ExecutionState:
        client = client or get_client("s3")
        state = cls(root_uri, n_workers, client=client)
        cache_key = state.read_execution_record(execution_id)["metadata"]["cache_key"]
        return cls(root_uri, n_workers, cache_key=cache_key, client=client)

    def _execution_key(self, execution_id: str, part: str = "state") -> str:
        return self._store._key_for(f"execution/{execution_id}/{part}.json")

    def _cache_key(self, cache_key: str) -> str:
        return self._store._key_for(f"cache/{cache_key}")

    def _edge_prefix(self, callee_id: str) -> str:
        return self._store._key_for(f"edges/{callee_id}/")

    def _edge_key(self, callee_id: str, caller_id: str) -> str:
        return f"{self._edge_prefix(callee_id)}{caller_id}.json"

    def adapter_scratch(self, execution_id: str) -> str:
        return f"s3://{self._store.bucket}/{self._store._key_for(f'io/{execution_id}/')}"

    def _snapshot(self, key: str) -> CasItem | None:
        try:
            return cast(CasItem, self._store._get(key, cas=True))
        except Exception as exc:
            if self._store._is_missing_error(exc):
                return None
            raise

    @staticmethod
    def _validate_metadata(value: object, execution_id: str | None = None) -> ExecutionMetadata:
        if not isinstance(value, dict) or set(value) != {"execution_id", "cache_key", "argv_ref", "created_at"}:
            raise DmlRepoError("Invalid execution metadata")
        if value.get("execution_id") != (execution_id or value.get("execution_id")):
            raise DmlRepoError("Invalid execution_id")
        if not isinstance(value["execution_id"], str) or not value["execution_id"]:
            raise DmlRepoError("Invalid execution_id")
        if value["cache_key"] is not None and (not isinstance(value["cache_key"], str) or not value["cache_key"]):
            raise DmlRepoError("Invalid cache_key")
        if value["argv_ref"] is not None and not _is_ref(value["argv_ref"], "node-argv"):
            raise DmlRepoError("Invalid argv_ref")
        if not _timestamp(value["created_at"]):
            raise DmlRepoError("Invalid created_at")
        return cast(ExecutionMetadata, value)

    @staticmethod
    def _validate_state(value: object) -> ExecutionSemanticState:
        required = {
            "lifecycle", "result_ref", "result_source", "spawned_execution_ids", "child_execution_ids",
            "cancelation", "invalidation", "updated_at",
        }
        if not isinstance(value, dict) or set(value) != required:
            raise DmlRepoError("Invalid execution state")
        if value["lifecycle"] not in {"pending", "running", "succeeded", "failed", "cancel-pending", "canceled"}:
            raise DmlRepoError("Invalid execution state")
        result, source = value["result_ref"], value["result_source"]
        if (result is None) != (source is None):
            raise DmlRepoError("Invalid execution result")
        if result is not None and (not _is_ref(result, "dag") or source not in {"runtime", "adapter-error"}):
            raise DmlRepoError("Invalid execution result")
        for name in ("spawned_execution_ids", "child_execution_ids"):
            items = value[name]
            if not isinstance(items, list) or not all(isinstance(x, str) and x for x in items):
                raise DmlRepoError(f"Invalid {name}")
            if len(items) != len(set(items)):
                raise DmlRepoError(f"Invalid {name}")
        if set(value["spawned_execution_ids"]) & set(value["child_execution_ids"]):
            raise DmlRepoError("Execution lineage must be disjoint")
        for name in ("cancelation", "invalidation"):
            control = value[name]
            if control is None:
                continue
            if not isinstance(control, dict) or set(control) != {"requested_by", "requested_at"}:
                raise DmlRepoError(f"Invalid {name}")
            if not isinstance(control["requested_by"], str) or not control["requested_by"]:
                raise DmlRepoError(f"Invalid {name}")
            if not _timestamp(control["requested_at"]):
                raise DmlRepoError(f"Invalid {name}")
        if not _timestamp(value["updated_at"]):
            raise DmlRepoError("Invalid updated_at")
        return cast(ExecutionSemanticState, value)

    @staticmethod
    def _validate_driver(value: object) -> ExecutionDriver:
        if not isinstance(value, dict) or set(value) != {"lock", "not_before", "adapter_state", "cleanup"}:
            raise DmlRepoError("Invalid execution driver")
        lock = value["lock"]
        if lock is not None:
            if not isinstance(lock, dict) or set(lock) != {"owner", "ttl"}:
                raise DmlRepoError("Invalid execution lock")
            if not isinstance(lock["owner"], str) or not lock["owner"]:
                raise DmlRepoError("Invalid execution lock")
            if not isinstance(lock["ttl"], (int, float)) or isinstance(lock["ttl"], bool):
                raise DmlRepoError("Invalid execution lock")
            if not math.isfinite(lock["ttl"]) or lock["ttl"] <= 0:
                raise DmlRepoError("Invalid execution lock")
        if value["not_before"] is not None and not _timestamp(value["not_before"]):
            raise DmlRepoError("Invalid not_before")
        if value["adapter_state"] is not None and not isinstance(value["adapter_state"], dict):
            raise DmlRepoError("Invalid adapter_state")
        cleanup = value["cleanup"]
        if cleanup is not None:
            if not isinstance(cleanup, dict) or set(cleanup) != {"status", "error"}:
                raise DmlRepoError("Invalid cleanup")
            if cleanup["status"] not in {"complete", "failed"}:
                raise DmlRepoError("Invalid cleanup")
            if not isinstance(cleanup["error"], (str, type(None))):
                raise DmlRepoError("Invalid cleanup")
            if cleanup["status"] == "complete" and cleanup["error"] is not None:
                raise DmlRepoError("Invalid cleanup")
            if cleanup["status"] == "failed" and not cleanup["error"]:
                raise DmlRepoError("Invalid cleanup")
        return cast(ExecutionDriver, value)

    def _part_snapshot(self, execution_id: str, part: Literal["metadata", "state", "driver"]) -> tuple[Any, CasItem]:
        item = self._snapshot(self._execution_key(execution_id, part))
        if item is None:
            raise DmlRepoError(f"No execution {part} found for execution_id: {execution_id}")
        validator = {
            "metadata": self._validate_metadata,
            "state": self._validate_state,
            "driver": self._validate_driver,
        }[part]
        value = validator(item.json, execution_id) if part == "metadata" else validator(item.json)
        return value, item

    def read_execution_record(self, execution_id: str) -> ExecutionRecord:
        metadata, _ = self._part_snapshot(execution_id, "metadata")
        state, _ = self._part_snapshot(execution_id, "state")
        driver, _ = self._part_snapshot(execution_id, "driver")
        return {"metadata": metadata, "state": state, "driver": driver}

    def create_execution_record(self, record: ExecutionRecord) -> bool:
        if not isinstance(record, dict) or set(record) != {"metadata", "state", "driver"}:
            raise DmlRepoError("Invalid execution record")
        metadata = self._validate_metadata(record.get("metadata"), None)
        state, driver = self._validate_state(record.get("state")), self._validate_driver(record.get("driver"))
        created: list[CasItem] = []
        try:
            for part, value in (
                (cast(Literal["metadata", "state", "driver"], "metadata"), metadata),
                (cast(Literal["metadata", "state", "driver"], "state"), state),
                (cast(Literal["metadata", "state", "driver"], "driver"), driver),
            ):
                self._store._put_js(self._execution_key(metadata["execution_id"], part), value, overwrite=False)
                item = self._snapshot(self._execution_key(metadata["execution_id"], part))
                assert item is not None
                created.append(item)
        except CasItemConflict:
            for item in created:
                self._store._delete(item)
            return False
        return True

    @staticmethod
    def _expired(item: CasItem, lock: ExecutionLock) -> bool:
        return item.last_modified.timestamp() + float(lock["ttl"]) <= item.date.timestamp()

    def _retry(self, action: Callable[[], Any], message: str) -> Any:
        for attempt in range(COORDINATION_CAS_ATTEMPTS):
            try:
                return action()
            except CasItemConflict:
                if attempt + 1 == COORDINATION_CAS_ATTEMPTS:
                    break
                delay = min(COORDINATION_CAS_MAX_BACKOFF_SECONDS, COORDINATION_CAS_BACKOFF_SECONDS * 2**attempt)
                time.sleep(random.uniform(0, delay))
        raise DmlRepoError(message)

    def _mutate_state(
        self, execution_id: str, mutate: Callable[[ExecutionSemanticState], None], *, owner: str | None = None,
        retry: bool = True,
    ) -> ExecutionSemanticState:
        def action() -> ExecutionSemanticState:
            state, item = self._part_snapshot(execution_id, "state")
            original = cast(ExecutionSemanticState, json.loads(json.dumps(state)))
            mutate(state)
            changed = {key for key in state if key != "updated_at" and state[key] != original[key]}
            lifecycle = original["lifecycle"]
            transition = (lifecycle, state["lifecycle"])
            lock_free_fields = {
                frozenset(): {"pending", "running", "succeeded", "failed", "cancel-pending", "canceled"},
                frozenset({"result_ref", "result_source"}): {"running"},
                frozenset({"spawned_execution_ids"}): {"running"},
                frozenset({"spawned_execution_ids", "child_execution_ids"}): {"running", "cancel-pending"},
            }
            changed_fields = frozenset(changed)
            if changed_fields in lock_free_fields:
                if lifecycle not in lock_free_fields[changed_fields]:
                    raise CancellationError(
                        f"Execution {execution_id} cannot accept lock-free state mutation",
                        lifecycle=lifecycle,
                    )
            else:
                if owner is None:
                    raise DmlRepoError(f"Driver lock ownership required for state mutation: {execution_id}")
                driver, _ = self._part_snapshot(execution_id, "driver")
                if driver["lock"] is None or driver["lock"]["owner"] != owner:
                    raise DmlRepoError(f"Driver lock ownership lost: {execution_id}")
                allowed = {
                    ("pending", "running"),
                    ("pending", "cancel-pending"),
                    ("running", "succeeded"),
                    ("running", "failed"),
                    ("running", "cancel-pending"),
                    ("cancel-pending", "canceled"),
                }
                if transition[0] != transition[1] and transition not in allowed:
                    raise BadExecutionStatusError(
                        f"Invalid execution lifecycle transition: {transition[0]} -> {transition[1]}",
                        lifecycle=lifecycle,
                    )
                if lifecycle == "cancel-pending" and changed - {"lifecycle"}:
                    raise CancellationError(
                        f"Execution {execution_id} is cancellation-owned", lifecycle=lifecycle
                    )
            state["updated_at"] = int(time.time())
            self._validate_state(state)
            self._store._put_js(item, state)
            return state

        if retry:
            return cast(
                ExecutionSemanticState,
                self._retry(action, f"Failed to mutate execution state: {execution_id}"),
            )
        return action()

    def acquire(self, execution_id: str, ttl: float = LOCK_TTL) -> str | None:
        owner = uuid4().hex
        for attempt in range(COORDINATION_CAS_ATTEMPTS):
            driver, item = self._part_snapshot(execution_id, "driver")
            if driver["lock"] is not None and not self._expired(item, driver["lock"]):
                return None
            driver["lock"] = {"owner": owner, "ttl": ttl}
            try:
                self._store._put_js(item, driver)
                return owner
            except CasItemConflict:
                if attempt + 1 < COORDINATION_CAS_ATTEMPTS:
                    delay = min(
                        COORDINATION_CAS_MAX_BACKOFF_SECONDS,
                        COORDINATION_CAS_BACKOFF_SECONDS * 2**attempt,
                    )
                    time.sleep(random.uniform(0, delay))
        raise DmlRepoError(f"Failed to acquire driver lock: {execution_id}")

    def _mutate_driver(
        self, execution_id: str, owner: str, mutate: Callable[[ExecutionDriver], None]
    ) -> ExecutionDriver:
        def action() -> ExecutionDriver:
            driver, item = self._part_snapshot(execution_id, "driver")
            if driver["lock"] is None or driver["lock"]["owner"] != owner:
                raise DmlRepoError(f"Driver lock ownership lost: {execution_id}")
            mutate(driver)
            self._validate_driver(driver)
            self._store._put_js(item, driver)
            return driver

        return cast(ExecutionDriver, self._retry(action, f"Failed to mutate execution driver: {execution_id}"))

    def unlock(self, execution_id: str, owner: str) -> bool:
        try:
            self._mutate_driver(execution_id, owner, lambda driver: driver.update(lock=None))
        except (CasItemConflict, DmlRepoError):
            return False
        return True

    def _wait_acquire(self, execution_id: str) -> str:
        while (owner := self.acquire(execution_id)) is None:
            time.sleep(0.1)
        return owner

    def reserve_execution(self, argv_ref: Ref | None, execution_id: str | None = None) -> tuple[str, str, CasItem]:
        now = int(time.time())
        execution_id = execution_id or uuid7().hex
        record: ExecutionRecord = {
            "metadata": {
                "execution_id": execution_id,
                "cache_key": self.cache_key,
                "argv_ref": argv_ref.to if argv_ref else None,
                "created_at": now,
            },
            "state": {
                "lifecycle": "pending" if self.cache_key else "running",
                "result_ref": None,
                "result_source": None,
                "spawned_execution_ids": [],
                "child_execution_ids": [],
                "cancelation": None,
                "invalidation": None,
                "updated_at": now,
            },
            "driver": {"lock": None, "not_before": None, "adapter_state": None, "cleanup": None},
        }
        if not self.create_execution_record(record):
            raise DmlRepoError(f"Execution record already exists for execution_id: {execution_id}")
        owner = self.acquire(execution_id)
        assert owner is not None
        snapshots = {
            part: self._snapshot(self._execution_key(execution_id, part))
            for part in ("metadata", "state", "driver")
        }
        assert all(item is not None for item in snapshots.values())
        if not hasattr(self, "_owned_reservations"):
            self._owned_reservations = {}
        self._owned_reservations[execution_id] = cast(dict[str, CasItem], snapshots)
        return execution_id, owner, cast(CasItem, snapshots["state"])

    def _read_cache(self, cache_key: str) -> tuple[str, CasItem] | None:
        item = self._snapshot(self._cache_key(cache_key))
        return None if item is None else (item.data, item)

    def _delete_cache(self, cache_key: str, execution_id: str) -> bool:
        pointer = self._read_cache(cache_key)
        return bool(pointer and pointer[0] == execution_id and self._store._delete(pointer[1]))

    def _create_cache(self, cache_key: str, execution_id: str) -> bool:
        try:
            self._store._put(self._cache_key(cache_key), execution_id, overwrite=False)
        except CasItemConflict:
            return False
        return True

    def _resolve_or_create(self, argv_ref: Ref, db: DmlDB | None = None) -> tuple[str, str | None, bool]:
        assert self.cache_key is not None
        while True:
            pointer = self._read_cache(self.cache_key)
            if pointer:
                try:
                    self.read_execution_record(pointer[0])
                    return pointer[0], None, False
                except DmlRepoError:
                    self._store._delete(pointer[1])
                    continue
            if db is not None:
                self._remote.upload_object_graph(argv_ref, db)
            execution_id, owner, _ = self.reserve_execution(argv_ref)
            try:
                self._store._put(self._cache_key(self.cache_key), execution_id, overwrite=False)
                return execution_id, owner, True
            except CasItemConflict:
                self._delete_reserved_execution(execution_id, owner)

    def _delete_reserved_execution(self, execution_id: str, owner: str) -> None:
        """Remove a reservation only while this caller still owns its driver."""
        owned = getattr(self, "_owned_reservations", {}).get(execution_id)
        if owned is None:
            return
        try:
            driver, _ = self._part_snapshot(execution_id, "driver")
        except DmlRepoError:
            return
        if driver["lock"] is None or driver["lock"]["owner"] != owner:
            return
        current = {part: self._snapshot(self._execution_key(execution_id, part)) for part in owned}
        if any(item is None or item.etag != owned[part].etag for part, item in current.items()):
            return
        if all(self._store._delete(owned[part]) for part in ("state", "driver", "metadata")):
            self._owned_reservations.pop(execution_id, None)

    def _materialize(self, value: str | None, db: DmlDB) -> Ref | None:
        return None if value is None else self._remote.materialize_ref(Ref(value), db)

    def _validate_adapter_response(
        self, response: object, *, success_status: Literal["success", "cancelled"] = "success"
    ) -> AdapterInvokeResponse:
        return validate_adapter_response(response, success_status=success_status)

    def _retry_not_before(self, response: AdapterInvokeResponse) -> int | None:
        if response["status"] != "retry":
            return None
        delay = response.get("retry_after_ms", int(COORDINATION_CAS_BACKOFF_SECONDS * 1000))
        return int(time.time() * 1000) + cast(int, delay)

    def _drive_cleanup_owned(
        self, execution_id: str, owner: str, db: DmlDB, runnable: Runnable | None = None
    ) -> None:
        record = self.read_execution_record(execution_id)
        metadata = record["metadata"]
        state = record["state"]
        driver = record["driver"]
        if state["result_ref"] is None or driver["cleanup"] is not None:
            return
        if driver["not_before"] is not None and driver["not_before"] > int(time.time() * 1000):
            return
        runnable = runnable or self._runnable_for_execution(metadata, db)
        if metadata["cache_key"] is None or runnable is None:
            return
        response = self._validate_adapter_response(
            self._call_adapter(
                {
                    "operation": "cleanup",
                    "cache_key": metadata["cache_key"],
                    "execution_id": execution_id,
                    "remote": {"root": self.root_uri},
                    "runnable": asdict(runnable),
                    "adapter_state": driver["adapter_state"],
                    "scratch_uri": self.adapter_scratch(execution_id),
                    "result_ref": state["result_ref"],
                }
            )
        )
        current = self.read_execution_record(execution_id)["driver"]
        if current["lock"] is None or current["lock"]["owner"] != owner:
            return
        adapter_state = response.get("adapter_state", current["adapter_state"])
        if response["status"] == "success":
            self._mutate_driver(
                execution_id,
                owner,
                lambda item: item.update(
                    adapter_state=adapter_state,
                    not_before=None,
                    cleanup={"status": "complete", "error": None},
                ),
            )
        elif response["status"] == "retry":
            self._mutate_driver(
                execution_id,
                owner,
                lambda item: item.update(
                    adapter_state=adapter_state,
                    not_before=self._retry_not_before(response),
                ),
            )
        else:
            self._mutate_driver(
                execution_id,
                owner,
                lambda item: item.update(
                    adapter_state=adapter_state,
                    not_before=None,
                    cleanup={"status": "failed", "error": response["error"]},
                ),
            )

    def _drive_cleanup(self, execution_id: str, db: DmlDB) -> None:
        owner = self.acquire(execution_id)
        if owner is None:
            return
        try:
            self._drive_cleanup_owned(execution_id, owner, db)
        finally:
            self.unlock(execution_id, owner)

    def get_cached_result(self, cache_key: str, db: DmlDB) -> Ref | None:
        pointer = self._read_cache(cache_key)
        if pointer is None:
            return None
        try:
            record = self.read_execution_record(pointer[0])
        except DmlRepoError:
            self._store._delete(pointer[1])
            return None
        state = record["state"]
        if (
            state["cancelation"]
            or state["invalidation"]
            or state["lifecycle"] not in ("succeeded", "failed")
            or state["result_ref"] is None
        ):
            return None
        self._drive_cleanup(pointer[0], db)
        return self._materialize(state["result_ref"], db)

    def describe_cache(self, cache_key: str) -> CacheStateDescription | None:
        pointer = self._read_cache(cache_key)
        if pointer is None:
            return None
        try:
            record = self.read_execution_record(pointer[0])
        except DmlRepoError:
            self._store._delete(pointer[1])
            return None
        state = record["state"]
        reusable = (
            state["lifecycle"] in ("succeeded", "failed")
            and not state["cancelation"]
            and not state["invalidation"]
        )
        return {
            "execution_id": pointer[0],
            "result_ref": state["result_ref"] if reusable else None,
            "lifecycle": state["lifecycle"],
        }

    def require_mutation(
        self,
        execution_id: str,
        db: DmlDB,
        *,
        mode: Literal["activation", "mutation"] = "activation",
    ) -> ExecutionSemanticState:
        state = self.read_execution_record(execution_id)["state"]
        if state["lifecycle"] == ("pending" if mode == "activation" else "running"):
            return state
        if state["lifecycle"] == "cancel-pending":
            self.cancel(execution_id, None, db)
            raise CancellationError(f"Execution {execution_id} is canceled", lifecycle="canceled")
        if state["lifecycle"] == "canceled":
            raise CanceledExecutionError(
                f"Execution {execution_id} is {state['lifecycle']}", lifecycle=state["lifecycle"]
            )
        raise BadExecutionStatusError(f"Execution {execution_id} is {state['lifecycle']}", lifecycle=state["lifecycle"])

    def activate(self, execution_id: str, db: DmlDB) -> tuple[ExecutionRecord, str]:
        owner = self._wait_acquire(execution_id)
        record = self.read_execution_record(execution_id)
        if record["state"]["lifecycle"] == "pending":
            return record, owner
        self.unlock(execution_id, owner)
        self.require_mutation(execution_id, db)
        raise AssertionError("execution mutation guard returned an invalid activation state")

    def mark_running(self, execution_id: str, owner: str) -> None:
        self._mutate_state(execution_id, lambda state: state.update(lifecycle="running"), owner=owner)
        self.unlock(execution_id, owner)

    def finish_execution(self, execution_id: str, dag: Ref, db: DmlDB) -> None:
        self._remote.upload_object_graph(dag, db)

        def publish(state: ExecutionSemanticState) -> None:
            if state["lifecycle"] in ("cancel-pending", "canceled"):
                raise CancellationError(f"Execution {execution_id} is canceled", lifecycle=state["lifecycle"])
            if state["result_ref"] is None:
                state.update(result_ref=dag.to, result_source="runtime")
            elif state["result_ref"] != dag.to:
                raise DmlRepoError(f"Execution {execution_id} already has a result")

        self._mutate_state(execution_id, publish)

    def _finalize_runtime_result(self, execution_id: str, owner: str) -> ExecutionSemanticState:
        def finalize(state: ExecutionSemanticState) -> None:
            if state["lifecycle"] in ("cancel-pending", "canceled"):
                raise CancellationError(f"Execution {execution_id} is canceled", lifecycle=state["lifecycle"])
            if state["lifecycle"] == "running" and state["result_source"] == "runtime":
                state["lifecycle"] = "succeeded"

        return self._mutate_state(execution_id, finalize, owner=owner)

    def _record_edge(self, caller: str, callee: str) -> bool:
        try:
            self._store._put_js(
                self._edge_key(callee, caller),
                {"caller_execution_id": caller, "callee_execution_id": callee},
                overwrite=False,
            )
        except CasItemConflict:
            return False
        return True

    def delete_execution_dependency(self, *, caller_execution_id: str, callee_execution_id: str) -> None:
        self._store._delete(self._edge_key(callee_execution_id, caller_execution_id))

    def list_execution_callers(self, callee_execution_id: str) -> list[str]:
        callers = []
        prefix = self._edge_prefix(callee_execution_id)
        for key in self._store._iter(prefix):
            caller_execution_id = key.removeprefix(prefix).removesuffix(".json")
            try:
                payload = json.loads(self._store._get(key))
            except (TypeError, ValueError) as exc:
                raise DmlRepoError(f"Invalid execution edge {key}") from exc
            expected = {
                "caller_execution_id": caller_execution_id,
                "callee_execution_id": callee_execution_id,
            }
            if (
                not key.endswith(".json")
                or not caller_execution_id
                or "/" in caller_execution_id
                or payload != expected
            ):
                raise DmlRepoError(f"Invalid execution edge {key}")
            callers.append(caller_execution_id)
        return sorted(callers)

    def _update_child(self, caller: str, callee: str, *, complete: bool) -> None:
        def mutate(state: ExecutionSemanticState) -> None:
            if not complete and state["lifecycle"] != "running":
                raise CancellationError(
                    f"Execution {caller} cannot spawn", lifecycle=state["lifecycle"]
                )
            if complete:
                state["spawned_execution_ids"] = sorted(set(state["spawned_execution_ids"]) - {callee})
                state["child_execution_ids"] = sorted({*state["child_execution_ids"], callee})
            else:
                state["spawned_execution_ids"] = sorted({*state["spawned_execution_ids"], callee})

        state = self._mutate_state(caller, mutate)
        if complete and state["lifecycle"] == "cancel-pending":
            raise CancellationError(f"Execution {caller} is canceled", lifecycle="cancel-pending")

    def _call_adapter(
        self, request: AdapterInvokeRequest | AdapterCancelRequest | AdapterCleanupRequest
    ) -> dict[str, Any]:
        path = shutil.which(request["runnable"]["adapter"])
        if path is None:
            raise DmlRepoError(f"Adapter executable not found: {request['runnable']['adapter']}")
        result = subprocess.run(
            [path],
            input=json.dumps(request, sort_keys=True, separators=(",", ":")),
            text=True,
            capture_output=True,
        )
        if result.returncode:
            return {"status": "failure", "error": result.stderr or "Adapter process failed"}
        try:
            response = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise DmlRepoError("Invalid adapter response JSON") from exc
        if not isinstance(response, dict) or not isinstance(response.get("status"), str) or not response["status"]:
            raise DmlRepoError("Invalid adapter response")
        return self._validate_adapter_response(
            response,
            success_status="cancelled" if request["operation"] == "cancel" else "success",
        )

    def _error_dag(self, message: str, argv: Ref, db: DmlDB) -> Ref:
        def persist(txn):
            error = txn.put(Error(message, "fn-call", "adapter-error"))
            return txn.put(Dag(nodes=[argv], names={}, tags=[], argv=argv, error=error))

        dag = db.write_with_growth(persist)
        self._remote.upload_object_graph(dag, db)
        return dag

    def get_or_start_fn(self, index: Ref, runnable: Runnable, argv_node: Ref, db: DmlDB) -> Ref | None:
        assert self.cache_key is not None
        cached = self.get_cached_result(self.cache_key, db)
        if cached is not None:
            return cached
        execution_id, owner, created = self._resolve_or_create(argv_node, db)
        owner = owner or self.acquire(execution_id)
        if owner is None:
            return None
        adapter_called = False
        edge_created = False
        try:
            record = self.read_execution_record(execution_id)
            state = record["state"]
            driver = record["driver"]
            if state["lifecycle"] in ("cancel-pending", "canceled"):
                raise CancellationError(
                    f"Execution {execution_id} is canceled", lifecycle=state["lifecycle"]
                )
            edge_created = self._record_edge(index.id(), execution_id)
            self._update_child(index.id(), execution_id, complete=False)
            if state["result_source"] == "runtime":
                state = self._finalize_runtime_result(execution_id, owner)
                self._update_child(index.id(), execution_id, complete=True)
                self._drive_cleanup_owned(execution_id, owner, db, runnable)
                return self._materialize(state["result_ref"], db)
            if driver["not_before"] is not None and driver["not_before"] > int(time.time() * 1000):
                return None
            adapter_called = True
            response = self._call_adapter(
                {
                    "operation": "invoke",
                    "cache_key": self.cache_key,
                    "execution_id": execution_id,
                    "remote": {"root": self.root_uri},
                    "runnable": asdict(runnable),
                    "adapter_state": driver["adapter_state"],
                    "scratch_uri": self.adapter_scratch(execution_id),
                }
            )
            current = self.read_execution_record(execution_id)
            if current["driver"]["lock"] is None or current["driver"]["lock"]["owner"] != owner:
                return None
            adapter_state = response.get("adapter_state", current["driver"]["adapter_state"])
            if response["status"] == "retry":
                if not isinstance(adapter_state, dict):
                    raise DmlRepoError("Retry adapter response requires object adapter_state")
                self._mutate_driver(
                    execution_id,
                    owner,
                    lambda d: d.update(
                        adapter_state=adapter_state,
                        not_before=self._retry_not_before(response),
                    ),
                )
                return None
            if response["status"] == "success":
                self._mutate_driver(execution_id, owner, lambda d: d.update(adapter_state=adapter_state))
                state = self.read_execution_record(execution_id)["state"]
                if state["result_source"] == "runtime":
                    state = self._finalize_runtime_result(execution_id, owner)
                    self._update_child(index.id(), execution_id, complete=True)
                    self._drive_cleanup_owned(execution_id, owner, db, runnable)
                    return self._materialize(state["result_ref"], db)
            dag = self._error_dag(
                str(response.get("error") or "Adapter failed before publishing a result"), argv_node, db
            )
            self._mutate_state(
                execution_id,
                lambda value: value.update(
                    lifecycle="failed", result_ref=dag.to, result_source="adapter-error"
                ),
                owner=owner,
            )
            self._mutate_driver(execution_id, owner, lambda value: value.update(adapter_state=adapter_state))
            self._update_child(index.id(), execution_id, complete=True)
            self._drive_cleanup_owned(execution_id, owner, db, runnable)
            return dag
        except Exception:
            if not adapter_called and edge_created:
                self.delete_execution_dependency(
                    caller_execution_id=index.id(), callee_execution_id=execution_id
                )
            if created and not adapter_called:
                self._delete_cache(self.cache_key, execution_id)
                self._delete_reserved_execution(execution_id, owner)
            raise
        finally:
            self.unlock(execution_id, owner)

    def describe_graph(self, roots: Sequence[str]) -> ExecutionGraph:
        roots = list(dict.fromkeys(roots))
        nodes: dict[str, ExecutionGraphNode] = {}
        pending = list(roots)
        while pending:
            execution_id = pending.pop()
            if execution_id in nodes:
                continue
            record = self.read_execution_record(execution_id)
            metadata = record["metadata"]
            state = record["state"]
            nodes[execution_id] = {
                "execution_id": execution_id,
                "cache_key": metadata["cache_key"],
                "lifecycle": state["lifecycle"],
                "updated_at": state["updated_at"],
                "created_at": metadata["created_at"],
                "cancel_requested_by": (
                    None if state["cancelation"] is None else state["cancelation"]["requested_by"]
                ),
                "children": state["child_execution_ids"],
                "spawned": state["spawned_execution_ids"],
            }
            pending.extend(state["child_execution_ids"] + state["spawned_execution_ids"])
        return {"roots": roots, "nodes": nodes}

    def invalidate_executions(self, execution_ids: Sequence[str], requested_by: str) -> InvalidationResponse:
        started = time.time()
        pending = list(execution_ids)
        seen: set[str] = set()
        invalidations: list[InvalidationRecord] = []
        roots = set(execution_ids)
        while pending:
            execution_id = pending.pop()
            if execution_id in seen:
                continue
            seen.add(execution_id)
            try:
                record = self.read_execution_record(execution_id)
            except DmlRepoError:
                parts = ("metadata", "state", "driver")
                if any(self._snapshot(self._execution_key(execution_id, part)) for part in parts):
                    raise
                continue
            owner = self._wait_acquire(execution_id)
            try:
                record = self.read_execution_record(execution_id)
                metadata = record["metadata"]
                if record["state"]["lifecycle"] == "cancel-pending":
                    continue
                if execution_id not in roots:
                    cache_key = metadata["cache_key"]
                    pointer = None if cache_key is None else self._read_cache(cache_key)
                    if pointer is None or pointer[0] != execution_id:
                        continue
                if metadata["cache_key"]:
                    self._delete_cache(metadata["cache_key"], execution_id)

                def invalidate(state: ExecutionSemanticState) -> None:
                    state["invalidation"] = state["invalidation"] or {
                        "requested_by": requested_by,
                        "requested_at": int(time.time()),
                    }

                state = self._mutate_state(execution_id, invalidate, owner=owner)
                pending.extend(self.list_execution_callers(execution_id))
                invalidation = state["invalidation"]
                assert invalidation is not None
                invalidations.append(
                    {
                        "execution_id": execution_id,
                        "cache_key": metadata["cache_key"],
                        "requested_by": invalidation["requested_by"],
                        "requested_at": invalidation["requested_at"],
                    }
                )
            finally:
                self.unlock(execution_id, owner)
        return {"total_time": time.time() - started, "invalidations": invalidations}

    def _runnable_for_execution(self, metadata: ExecutionMetadata, db: DmlDB) -> Runnable | None:
        if metadata["argv_ref"] is None:
            return None
        argv = self._materialize(metadata["argv_ref"], db)
        assert argv is not None
        with db.tx(readonly=True) as txn:
            datum_ref, _ = txn.get(argv).datum_ref(txn)
            assert datum_ref is not None
            runnable = txn.get(txn.get(datum_ref).value(txn)[0]).value(txn)
        return cast(Runnable, runnable)

    def _invoke_cancel_adapter(self, execution_id: str, requested_by: str | None, db: DmlDB) -> bool:
        while True:
            owner = self._wait_acquire(execution_id)
            try:
                record = self.read_execution_record(execution_id)
                lifecycle = record["state"]["lifecycle"]
                if lifecycle == "canceled":
                    return True
                if lifecycle != "cancel-pending":
                    logger.warning(
                        "Dropping cancellation work for execution %s with unexpected lifecycle %s",
                        execution_id,
                        lifecycle,
                    )
                    return True
                not_before = record["driver"]["not_before"]
                delay = 0 if not_before is None else (not_before - int(time.time() * 1000)) / 1000
                if delay > 0:
                    self.unlock(execution_id, owner)
                    owner = ""
                    time.sleep(delay)
                    continue
                metadata, driver = record["metadata"], record["driver"]
                runnable = self._runnable_for_execution(metadata, db) if metadata["cache_key"] is not None else None
                if runnable is None:
                    response = {"status": "cancelled"}
                else:
                    assert metadata["cache_key"] is not None
                    assert metadata["argv_ref"] is not None
                    request: AdapterCancelRequest = {
                        "operation": "cancel",
                        "cache_key": metadata["cache_key"],
                        "execution_id": execution_id,
                        "argv_ref": metadata["argv_ref"],
                        "remote": {"root": self.root_uri},
                        "runnable": asdict(runnable),
                        "adapter_state": driver["adapter_state"],
                        "scratch_uri": self.adapter_scratch(execution_id),
                        "requested_by": requested_by,
                    }
                    response = self._call_adapter(request)
                not_before = self._retry_not_before(response)
                adapter_state = response.get("adapter_state")

                def update_driver(
                    item: ExecutionDriver,
                    not_before: int | None = not_before,
                    adapter_state: Any = adapter_state,
                ) -> None:
                    item["not_before"] = not_before
                    if isinstance(adapter_state, dict):
                        item["adapter_state"] = adapter_state

                self._mutate_driver(
                    execution_id,
                    owner,
                    update_driver,
                )
                if response["status"] != "cancelled":
                    return False

                def complete_cancel(state: ExecutionSemanticState) -> None:
                    if state["lifecycle"] == "cancel-pending":
                        state["lifecycle"] = "canceled"
                    elif state["lifecycle"] not in {"canceled"}:
                        logger.warning(
                            "Dropping cancellation completion for execution %s with unexpected lifecycle %s",
                            execution_id,
                            state["lifecycle"],
                        )

                self._mutate_state(execution_id, complete_cancel, owner=owner)
                return True
            finally:
                if owner:
                    self.unlock(execution_id, owner)

    def _plan_cancel(self, execution_ids: Sequence[str], requested_by: str) -> list[str]:
        pending = list(execution_ids)
        selected: list[str] = []
        selected_ids: set[str] = set()
        complete: set[str] = set()
        while pending:
            execution_id = pending.pop()
            if execution_id in selected_ids or execution_id in complete:
                continue
            try:
                owner = self._wait_acquire(execution_id)
            except DmlRepoError:
                if self._snapshot(self._execution_key(execution_id, "metadata")) is None:
                    complete.add(execution_id)
                    continue
                raise
            try:
                for attempt in range(COORDINATION_CAS_ATTEMPTS):
                    record = self.read_execution_record(execution_id)
                    metadata = record["metadata"]
                    state = record["state"]
                    expected_lifecycle = state["lifecycle"]
                    if state["lifecycle"] in ("succeeded", "failed", "canceled"):
                        complete.add(execution_id)
                        break
                    if state["lifecycle"] == "cancel-pending":
                        break
                    if self.list_execution_callers(execution_id):
                        break

                    def mark_pending(
                        item: ExecutionSemanticState, expected_lifecycle: EXECUTION_LIFECYCLES = expected_lifecycle
                    ) -> None:
                        if item["lifecycle"] != expected_lifecycle:
                            raise CasItemConflict
                        item["lifecycle"] = "cancel-pending"
                        item["cancelation"] = {
                            "requested_by": requested_by,
                            "requested_at": int(time.time()),
                        }

                    try:
                        state = self._mutate_state(execution_id, mark_pending, owner=owner, retry=False)
                    except CasItemConflict as exc:
                        if attempt + 1 == COORDINATION_CAS_ATTEMPTS:
                            raise DmlRepoError(f"Failed to plan cancellation: {execution_id}") from exc
                        continue
                    break
                else:
                    raise AssertionError("unreachable cancellation planning loop")
            finally:
                self.unlock(execution_id, owner)
            if execution_id in complete or state["lifecycle"] != "cancel-pending":
                continue
            selected.append(execution_id)
            selected_ids.add(execution_id)
            if metadata["cache_key"] is not None:
                self._delete_cache(metadata["cache_key"], execution_id)
            for child in state["spawned_execution_ids"]:
                self.delete_execution_dependency(
                    caller_execution_id=execution_id,
                    callee_execution_id=child,
                )
                pending.append(child)
        return selected

    def _run_cancel_driver(
        self, execution_ids: Sequence[str], requested_by: str | None, db: DmlDB, max_retries: int
    ) -> None:
        remaining = set(execution_ids)
        with ThreadPoolExecutor(max_workers=max(1, len(remaining))) as pool:
            for _ in range(max_retries + 1):
                futures = {
                    execution_id: pool.submit(self._invoke_cancel_adapter, execution_id, requested_by, db)
                    for execution_id in remaining
                }
                remaining = {
                    execution_id
                    for execution_id, future in futures.items()
                    if future.exception() is not None or not future.result()
                }
                if not remaining:
                    return
        raise DmlRepoError(f"Cancellation failed for executions: {', '.join(sorted(remaining))}")

    def cancel(
        self,
        execution_id: str,
        requested_by: str | None,
        db: DmlDB,
        *,
        max_retries: int = 3,
    ) -> None:
        if not isinstance(max_retries, int) or isinstance(max_retries, bool) or max_retries < 0:
            raise TypeError("max_retries must be a nonnegative integer")
        record = self.read_execution_record(execution_id)
        cancelation = record["state"]["cancelation"]
        effective = cancelation["requested_by"] if cancelation is not None else requested_by
        if effective is None:
            raise DmlRepoError(f"Execution {execution_id} has no cancellation requester")
        selected = self._plan_cancel([execution_id], effective)
        self._run_cancel_driver(selected, effective, db, max_retries)
