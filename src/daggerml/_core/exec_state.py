"""S3-backed unified execution state and cache coordination."""

from __future__ import annotations

import json
import logging
import math
import shutil
import subprocess
import time
from dataclasses import InitVar, asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Literal, NotRequired, Sequence, TypedDict, cast
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

if TYPE_CHECKING:
    import boto3

logger = logging.getLogger(__name__)
LOCK_TTL = 300.0
COORDINATION_CAS_ATTEMPTS = 10
COORDINATION_CAS_BACKOFF_SECONDS = 0.01
COORDINATION_CAS_MAX_BACKOFF_SECONDS = 1.0
CANCEL_READY_TIMEOUT_SECONDS = 60.0

EXECUTION_LIFECYCLES = Literal[
    "pending", "running", "succeeded", "failed", "cancel-requested", "cancel-ready", "canceled"
]
GRAPH_LIFECYCLES = EXECUTION_LIFECYCLES


def _is_ref_in_namespace(value: str, expected_ns: str) -> bool:
    try:
        return Ref(value).ns() == expected_ns
    except (TypeError, ValueError):
        return False


class CancellationError(CanceledExecutionError):
    def __init__(self, message: str, *, lifecycle: str | None = None):
        super().__init__(message, lifecycle=lifecycle)
        self.type = "cancellationerror"
        self.lifecycle = lifecycle


class ExecutionLock(TypedDict):
    owner: str
    ttl: float


class ControlRecord(TypedDict):
    requested_by: str
    requested_at: int


class ExecutionRecord(TypedDict):
    execution_id: str
    cache_key: str | None
    lifecycle: EXECUTION_LIFECYCLES
    created_at: int
    updated_at: int
    lock: ExecutionLock | None
    adapter_state: dict[str, Any] | None
    argv_ref: str | None
    result_ref: str | None
    spawned_execution_ids: list[str]
    child_execution_ids: list[str]
    cancelation: ControlRecord | None
    invalidation: ControlRecord | None


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


class AdapterInvokeResponse(TypedDict):
    status: str
    adapter_state: NotRequired[dict[str, Any] | None]
    state: NotRequired[dict[str, Any] | None]
    dag_id: NotRequired[str | None]
    error: NotRequired[str | None]


class AdapterCancelResponse(TypedDict):
    status: str
    adapter_state: NotRequired[dict[str, Any] | None]
    state: NotRequired[dict[str, Any] | None]
    error: NotRequired[str | None]


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


@dataclass
class ExecutionState:
    root_uri: str
    n_workers: int
    client: InitVar["boto3.client"]
    cache_key: str | None = None
    _store: S3Remote = field(init=False)

    def __post_init__(self, client) -> None:
        self._store = S3Remote(self.root_uri.rstrip("/") + "/exec", client=client)
        self._remote = Remote(self.root_uri, self.n_workers, client=client)

    @classmethod
    def from_execution_id(cls, execution_id: str, *, root_uri: str, n_workers: int, client=None) -> ExecutionState:
        state = cls(root_uri, n_workers, client=client)
        record = state.read_execution_record(execution_id)
        return cls(root_uri, n_workers, cache_key=record["cache_key"], client=client)

    def _execution_key(self, execution_id: str) -> str:
        return self._store._key_for(f"execution/{execution_id}.json")

    def _cache_key(self, cache_key: str) -> str:
        return self._store._key_for(f"cache/{cache_key}")

    def _edge_prefix(self, callee_id: str) -> str:
        return self._store._key_for(f"edge/{callee_id}/")

    def _edge_key(self, callee_id: str, caller_id: str) -> str:
        return f"{self._edge_prefix(callee_id)}{caller_id}.json"

    def adapter_scratch(self, execution_id: str) -> str:
        key = self._store._key_for(f"io/{execution_id}/")
        return f"s3://{self._store.bucket}/{key}"

    def _snapshot(self, key: str) -> CasItem | None:
        try:
            return cast(CasItem, self._store._get(key, cas=True))
        except Exception as exc:
            if self._store._is_missing_error(exc):
                return None
            raise

    @staticmethod
    def _validate_record(value: object, execution_id: str | None = None) -> ExecutionRecord:
        if not isinstance(value, dict):
            raise DmlRepoError("Execution record must be a JSON object")
        required = {
            "execution_id", "cache_key", "lifecycle", "created_at", "updated_at", "lock", "adapter_state",
            "argv_ref", "result_ref", "spawned_execution_ids", "child_execution_ids", "cancelation", "invalidation",
        }
        if set(value) != required or (execution_id is not None and value.get("execution_id") != execution_id):
            invalid_id = execution_id or value.get("execution_id")
            raise DmlRepoError(f"Invalid execution record for execution_id: {invalid_id}")
        if not isinstance(value["execution_id"], str) or not value["execution_id"]:
            raise DmlRepoError("Invalid execution_id")
        if value["cache_key"] is not None and (
            not isinstance(value["cache_key"], str) or not value["cache_key"]
        ):
            raise DmlRepoError("Invalid cache_key")
        if not isinstance(value["lifecycle"], str) or value["lifecycle"] not in {
            "pending", "running", "succeeded", "failed", "cancel-requested", "cancel-ready", "canceled"
        }:
            raise DmlRepoError("Invalid execution lifecycle")
        lock = value["lock"]
        if (
            not isinstance(value["created_at"], int)
            or isinstance(value["created_at"], bool)
            or value["created_at"] < 0
            or not isinstance(value["updated_at"], int)
            or isinstance(value["updated_at"], bool)
            or value["updated_at"] < value["created_at"]
        ):
            raise DmlRepoError("Invalid execution timestamps")
        if lock is not None and (
            not isinstance(lock, dict) or set(lock) != {"owner", "ttl"}
            or not isinstance(lock["owner"], str) or not lock["owner"]
            or not isinstance(lock["ttl"], (int, float)) or isinstance(lock["ttl"], bool)
            or not math.isfinite(lock["ttl"]) or lock["ttl"] <= 0
        ):
            raise DmlRepoError("Invalid execution lock")
        if value["adapter_state"] is not None and not isinstance(value["adapter_state"], dict):
            raise DmlRepoError("Invalid adapter state")
        for name, expected_ns in (("argv_ref", "node-argv"), ("result_ref", "dag")):
            if value[name] is not None and (
                not isinstance(value[name], str)
                or not value[name]
                or not _is_ref_in_namespace(value[name], expected_ns)
            ):
                raise DmlRepoError(f"Invalid {name}")
        for name in ("spawned_execution_ids", "child_execution_ids"):
            if (
                not isinstance(value[name], list)
                or not all(isinstance(item, str) and item for item in value[name])
                or len(set(value[name])) != len(value[name])
            ):
                raise DmlRepoError(f"Invalid {name}")
        if set(value["spawned_execution_ids"]) & set(value["child_execution_ids"]):
            raise DmlRepoError("Spawned and child execution IDs must be disjoint")
        for name in ("cancelation", "invalidation"):
            control = value[name]
            if control is not None and (
                not isinstance(control, dict) or set(control) != {"requested_by", "requested_at"}
                or not isinstance(control["requested_by"], str) or not control["requested_by"]
                or not isinstance(control["requested_at"], int) or isinstance(control["requested_at"], bool)
                or control["requested_at"] < 0
            ):
                raise DmlRepoError(f"Invalid {name}")
        return cast(ExecutionRecord, value)

    def _record_snapshot(self, execution_id: str) -> tuple[ExecutionRecord, CasItem]:
        item = self._snapshot(self._execution_key(execution_id))
        if item is None:
            raise DmlRepoError(f"No execution record found for execution_id: {execution_id}")
        return self._validate_record(item.json, execution_id), item

    def create_execution_record(self, record: ExecutionRecord) -> bool:
        self._validate_record(record, record["execution_id"])
        try:
            self._store._put_js(self._execution_key(record["execution_id"]), record, overwrite=False)
        except CasItemConflict:
            return False
        return True

    def read_execution_record(self, execution_id: str) -> ExecutionRecord:
        return self._record_snapshot(execution_id)[0]

    def delete_execution_record(self, execution_id: str, snapshot: CasItem | None = None) -> bool:
        item = snapshot or self._snapshot(self._execution_key(execution_id))
        return False if item is None else self._store._delete(item)

    @staticmethod
    def _expired(item: CasItem, lock: ExecutionLock) -> bool:
        return (item.last_modified.timestamp() + float(lock["ttl"])) <= item.date.timestamp()

    def acquire(self, execution_id: str, ttl: float = LOCK_TTL) -> str | None:
        owner = uuid4().hex
        for _ in range(COORDINATION_CAS_ATTEMPTS):
            record, item = self._record_snapshot(execution_id)
            lock = record["lock"]
            if lock is not None and not self._expired(item, lock):
                return None
            record["lock"] = {"owner": owner, "ttl": ttl}
            record["updated_at"] = int(time.time())
            try:
                self._store._put_js(item, record)
                return owner
            except CasItemConflict:
                continue
        raise DmlRepoError(f"Failed to acquire execution lock: {execution_id}")

    def _mutate(self, execution_id: str, owner: str, mutate: Callable[[ExecutionRecord], None]) -> ExecutionRecord:
        for _ in range(COORDINATION_CAS_ATTEMPTS):
            record, item = self._record_snapshot(execution_id)
            lock = record["lock"]
            if lock is None or lock["owner"] != owner:
                raise CasItemConflict(f"Execution lock ownership lost: {execution_id}")
            mutate(record)
            record["updated_at"] = int(time.time())
            self._validate_record(record, execution_id)
            try:
                self._store._put_js(item, record)
                return record
            except CasItemConflict:
                continue
        raise DmlRepoError(f"Failed to mutate execution after CAS retries: {execution_id}")

    def unlock(self, execution_id: str, owner: str) -> bool:
        try:
            self._mutate(execution_id, owner, lambda record: record.update(lock=None))
        except CasItemConflict:
            return False
        return True

    def _wait_acquire(self, execution_id: str) -> str:
        while True:
            owner = self.acquire(execution_id)
            if owner is not None:
                return owner
            time.sleep(0.1)

    def reserve_execution(self, argv_ref: Ref | None, execution_id: str | None = None) -> tuple[str, str, CasItem]:
        now = int(time.time())
        exec_id = execution_id or uuid7().hex
        owner = uuid4().hex
        record = ExecutionRecord(
            execution_id=exec_id,
            cache_key=self.cache_key,
            lifecycle="pending" if self.cache_key is not None else "running",
            created_at=now,
            updated_at=now,
            lock={"owner": owner, "ttl": LOCK_TTL},
            adapter_state=None,
            argv_ref=argv_ref.to if argv_ref is not None else None,
            result_ref=None,
            spawned_execution_ids=[],
            child_execution_ids=[],
            cancelation=None,
            invalidation=None,
        )
        if not self.create_execution_record(record):
            raise DmlRepoError(f"Execution record already exists for execution_id: {exec_id}")
        snapshot = self._snapshot(self._execution_key(exec_id))
        assert snapshot is not None
        return exec_id, owner, snapshot

    def _read_cache(self, cache_key: str) -> tuple[str, CasItem] | None:
        item = self._snapshot(self._cache_key(cache_key))
        return None if item is None else (item.data, item)

    def _create_cache(self, cache_key: str, execution_id: str) -> bool:
        try:
            self._store._put(self._cache_key(cache_key), execution_id, overwrite=False)
        except CasItemConflict:
            return False
        return True

    def _delete_cache(self, cache_key: str, execution_id: str) -> bool:
        current = self._read_cache(cache_key)
        if current is None or current[0] != execution_id:
            return False
        return self._store._delete(current[1])

    def _materialize_record_ref(self, value: str | None, db: DmlDB, expected_ns: str) -> Ref | None:
        if value is None:
            return None
        ref = Ref(value)
        if ref.ns() != expected_ns:
            raise DmlRepoError(f"Expected {expected_ns} execution ref, got: {ref}")
        return self._remote.materialize_ref(ref, db)

    def get_cached_result(self, cache_key: str, db: DmlDB) -> Ref | None:
        pointer = self._read_cache(cache_key)
        if pointer is None:
            return None
        try:
            record = self.read_execution_record(pointer[0])
        except DmlRepoError:
            self._store._delete(pointer[1])
            return None
        if record["cancelation"] is not None or record["invalidation"] is not None:
            return None
        if record["lifecycle"] not in ("succeeded", "failed"):
            return None
        return self._materialize_record_ref(record["result_ref"], db, "dag")

    def describe_cache(self, cache_key: str) -> CacheStateDescription | None:
        pointer = self._read_cache(cache_key)
        if pointer is None:
            return None
        try:
            record = self.read_execution_record(pointer[0])
        except DmlRepoError:
            self._store._delete(pointer[1])
            return None
        reusable = (
            record["lifecycle"] in ("succeeded", "failed")
            and record["cancelation"] is None
            and record["invalidation"] is None
        )
        return {
            "execution_id": record["execution_id"],
            "result_ref": record["result_ref"] if reusable else None,
            "lifecycle": record["lifecycle"],
        }

    def _resolve_or_create(self, argv_ref: Ref, db: DmlDB | None = None) -> tuple[str, str | None, bool]:
        if self.cache_key is None:
            raise DmlRepoError("cache_key is required")
        while True:
            pointer = self._read_cache(self.cache_key)
            if pointer is not None:
                if self._snapshot(self._execution_key(pointer[0])) is not None:
                    return pointer[0], None, False
                self._store._delete(pointer[1])
                continue
            if db is not None:
                self._remote.upload_object_graph(argv_ref, db)
            exec_id, owner, snapshot = self.reserve_execution(argv_ref)
            if self._create_cache(self.cache_key, exec_id):
                return exec_id, owner, True
            self.delete_execution_record(exec_id, snapshot)

    def require_mutation(
        self, execution_id: str, db: DmlDB, *, mode: Literal["activation", "mutation"] = "activation"
    ) -> ExecutionRecord:
        record = self.read_execution_record(execution_id)
        lifecycle = record["lifecycle"]
        allowed = "pending" if mode == "activation" else "running"
        if lifecycle == allowed:
            return record
        if lifecycle == "cancel-requested":
            self.cancel(execution_id, None, db, mode="drive")
        if lifecycle in ("cancel-requested", "cancel-ready", "canceled"):
            raise CanceledExecutionError(f"Execution {execution_id} is {lifecycle}", lifecycle=lifecycle)
        raise BadExecutionStatusError(f"Execution {execution_id} is {lifecycle}", lifecycle=lifecycle)

    def activate(self, execution_id: str, db: DmlDB) -> tuple[ExecutionRecord, str]:
        owner = self._wait_acquire(execution_id)
        record = self.read_execution_record(execution_id)
        if record["lifecycle"] != "pending":
            self.unlock(execution_id, owner)
            self.require_mutation(execution_id, db, mode="activation")
        return record, owner

    def mark_running(self, execution_id: str, owner: str) -> None:
        self._mutate(execution_id, owner, lambda record: record.update(lifecycle="running"))
        self.unlock(execution_id, owner)

    def _record_edge(self, caller_id: str, callee_id: str) -> None:
        try:
            self._store._put_js(
                self._edge_key(callee_id, caller_id),
                {"caller_execution_id": caller_id, "callee_execution_id": callee_id},
                overwrite=False,
            )
        except CasItemConflict:
            pass

    def delete_execution_dependency(self, *, caller_execution_id: str, callee_execution_id: str) -> None:
        self._store._delete(self._edge_key(callee_execution_id, caller_execution_id))

    def list_execution_callers(self, callee_execution_id: str) -> list[str]:
        keys = self._store._iter(self._edge_prefix(callee_execution_id))
        return [key.rsplit("/", 1)[-1].removesuffix(".json") for key in keys]

    def _update_child(self, caller_id: str, callee_id: str, *, complete: bool) -> None:
        owner = self._wait_acquire(caller_id)
        try:
            def mutate(record: ExecutionRecord) -> None:
                if not complete and record["lifecycle"] != "running":
                    raise CancellationError(
                        f"Execution {caller_id} status: {record['lifecycle']} cannot spawn",
                        lifecycle=record["lifecycle"],
                    )
                if complete:
                    record["spawned_execution_ids"] = sorted(set(record["spawned_execution_ids"]) - {callee_id})
                    record["child_execution_ids"] = sorted({*record["child_execution_ids"], callee_id})
                else:
                    record["spawned_execution_ids"] = sorted({*record["spawned_execution_ids"], callee_id})
            self._mutate(caller_id, owner, mutate)
        finally:
            self.unlock(caller_id, owner)

    def _add_spawned_execution(self, caller_id: str, callee_id: str) -> None:
        self._update_child(caller_id, callee_id, complete=False)

    def _complete_spawned_execution(self, caller_id: str, callee_id: str) -> None:
        self._update_child(caller_id, callee_id, complete=True)

    def _call_adapter(
        self,
        request: AdapterInvokeRequest | AdapterCancelRequest,
        *,
        on_call: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        adapter = request["runnable"]["adapter"]
        adapter_path = shutil.which(adapter)
        if adapter_path is None:
            raise DmlRepoError(f"Adapter executable not found: {adapter}")
        if on_call is not None:
            on_call()
        result = subprocess.run(
            [adapter_path],
            input=json.dumps(request, sort_keys=True, separators=(",", ":")),
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            return {"status": "error", "error": result.stderr, "adapter_state": request["adapter_state"] or {}}
        try:
            response = json.loads(result.stdout)
        except json.JSONDecodeError:
            raise DmlRepoError(f"Invalid adapter response JSON: {result.stdout}") from None
        if not isinstance(response, dict):
            raise DmlRepoError("Adapter response must be a JSON object")
        status = response.get("status")
        if not isinstance(status, str) or not status:
            raise DmlRepoError("Adapter response must contain a non-empty status")
        state = response.get("adapter_state")
        if "adapter_state" in response and state is not None and not isinstance(state, dict):
            raise DmlRepoError("Adapter response adapter_state must be an object or null")
        if (
            request["operation"] == "invoke"
            and status == "running"
            and state is None
            and request["adapter_state"] is None
        ):
            raise DmlRepoError("Running adapter response requires object adapter_state for a fresh execution")
        return response

    def _error_dag(self, message: str, argv_ref: Ref, db: DmlDB) -> Ref:
        def persist(txn):
            error = txn.put(Error(message, "fn-call", "adapter-error"))
            return txn.put(Dag(nodes=[argv_ref], names={}, argv=argv_ref, error=error))
        dag = db.write_with_growth(persist)
        self._remote.upload_object_graph(dag, db)
        return dag

    def get_or_start_fn(self, index: Ref, runnable: Runnable, argv_node: Ref, db: DmlDB) -> Ref | None:
        if self.cache_key is None:
            raise DmlRepoError("cache_key is required")
        cached = self.get_cached_result(self.cache_key, db)
        if cached is not None:
            return cached
        execution_id, owner, created = self._resolve_or_create(argv_node, db)
        if owner is None:
            owner = self.acquire(execution_id)
            if owner is None:
                return None
        adapter_called = False
        execution_deleted = False
        try:
            self._record_edge(index.id(), execution_id)
            self._add_spawned_execution(index.id(), execution_id)
            record = self.read_execution_record(execution_id)
            request = AdapterInvokeRequest(
                operation="invoke",
                cache_key=self.cache_key,
                execution_id=execution_id,
                remote={"root": self.root_uri},
                runnable=asdict(runnable),
                adapter_state=record["adapter_state"],
                scratch_uri=self.adapter_scratch(execution_id),
            )
            def mark_adapter_called() -> None:
                nonlocal adapter_called
                adapter_called = True

            response = self._call_adapter(request, on_call=mark_adapter_called)
            logger.debug("Adapter response for %s: %s", execution_id, response)
            current = self.read_execution_record(execution_id)
            if current["lock"] is None or current["lock"]["owner"] != owner:
                return None
            state = response.get("adapter_state", current["adapter_state"])
            status = response.get("status")
            if status == "running":
                if not isinstance(state, dict):
                    raise DmlRepoError("Running adapter response requires object adapter_state")
                self._mutate(execution_id, owner, lambda item: item.update(adapter_state=state))
                return None
            if status == "succeeded":
                self._mutate(execution_id, owner, lambda item: item.update(adapter_state=state))
                result = self.read_execution_record(execution_id)["result_ref"]
                if result is not None:
                    dag = self._materialize_record_ref(result, db, "dag")
                    self._complete_spawned_execution(index.id(), execution_id)
                    return dag
                logger.warning("Adapter succeeded before execution %s published result_ref", execution_id)
                response["error"] = "Adapter succeeded before publishing an execution result"
            dag = self._error_dag(str(response.get("error") or "Unknown adapter error"), argv_node, db)
            self._mutate(
                execution_id,
                owner,
                lambda item: item.update(adapter_state=state, lifecycle="failed", result_ref=dag.to),
            )
            self._complete_spawned_execution(index.id(), execution_id)
            return dag
        except Exception:
            if created and not adapter_called:
                self.delete_execution_dependency(caller_execution_id=index.id(), callee_execution_id=execution_id)
                self._delete_cache(self.cache_key, execution_id)
                try:
                    record, snapshot = self._record_snapshot(execution_id)
                    if record["lock"] is not None and record["lock"]["owner"] == owner:
                        execution_deleted = self.delete_execution_record(execution_id, snapshot)
                except DmlRepoError:
                    pass
            raise
        finally:
            if not execution_deleted:
                self.unlock(execution_id, owner)

    def finish_execution(self, execution_id: str, dag: Ref, db: DmlDB) -> None:
        self._remote.upload_object_graph(dag, db)
        owner = self._wait_acquire(execution_id)
        try:
            def finish(record: ExecutionRecord) -> None:
                if record["lifecycle"].startswith("cancel") or record["lifecycle"] == "canceled":
                    raise CancellationError(
                        f"Execution {execution_id} is {record['lifecycle']}", lifecycle=record["lifecycle"]
                    )
                record["lifecycle"] = "succeeded"
                record["result_ref"] = dag.to
            self._mutate(execution_id, owner, finish)
        finally:
            self.unlock(execution_id, owner)

    def describe_graph(self, root_execution_ids: Sequence[str]) -> ExecutionGraph:
        roots = list(dict.fromkeys(root_execution_ids))
        nodes: dict[str, ExecutionGraphNode] = {}
        pending = list(reversed(roots))
        while pending:
            execution_id = pending.pop()
            if execution_id in nodes:
                continue
            try:
                record = self.read_execution_record(execution_id)
            except DmlRepoError:
                nodes[execution_id] = {
                    "execution_id": execution_id, "cache_key": None, "lifecycle": "pending", "updated_at": 0,
                    "created_at": 0, "cancel_requested_by": None, "children": [], "spawned": [],
                }
                continue
            children = list(record["child_execution_ids"])
            spawned = list(record["spawned_execution_ids"])
            nodes[execution_id] = {
                "execution_id": execution_id,
                "cache_key": record["cache_key"],
                "lifecycle": record["lifecycle"],
                "updated_at": record["updated_at"],
                "created_at": record["created_at"],
                "cancel_requested_by": None if record["cancelation"] is None else record["cancelation"]["requested_by"],
                "children": children,
                "spawned": spawned,
            }
            pending.extend(reversed(children + spawned))
        return {"roots": roots, "nodes": nodes}

    def invalidate_executions(self, execution_ids: Sequence[str], requested_by: str) -> InvalidationResponse:
        started = time.time()
        invalidations: list[InvalidationRecord] = []
        roots = set(execution_ids)
        pending = list(execution_ids)
        seen: set[str] = set()
        while pending:
            execution_id = pending.pop()
            if execution_id in seen:
                continue
            seen.add(execution_id)
            try:
                self.read_execution_record(execution_id)
            except DmlRepoError:
                continue
            owner = self._wait_acquire(execution_id)
            try:
                record = self.read_execution_record(execution_id)
                cache_key = record["cache_key"]
                if execution_id not in roots:
                    if cache_key is None:
                        continue
                    pointer = self._read_cache(cache_key)
                    if pointer is None or pointer[0] != execution_id or not self._store._delete(pointer[1]):
                        continue
                elif cache_key is not None:
                    self._delete_cache(cache_key, execution_id)

                requested_at = int(time.time())

                def mark_invalid(item: ExecutionRecord, at: int = requested_at) -> None:
                    if item["invalidation"] is None:
                        item["invalidation"] = {"requested_by": requested_by, "requested_at": at}

                record = self._mutate(execution_id, owner, mark_invalid)
            finally:
                self.unlock(execution_id, owner)
            pending.extend(self.list_execution_callers(execution_id))
            invalidation = record["invalidation"]
            assert invalidation is not None
            invalidations.append(
                {
                    "execution_id": execution_id,
                    "cache_key": cache_key,
                    "requested_by": invalidation["requested_by"],
                    "requested_at": invalidation["requested_at"],
                }
            )
        return {"total_time": time.time() - started, "invalidations": invalidations}

    def _mark_lifecycle(
        self, execution_id: str, lifecycle: EXECUTION_LIFECYCLES, requested_by: str | None = None
    ) -> ExecutionRecord:
        owner = self._wait_acquire(execution_id)
        try:
            def mutate(record: ExecutionRecord) -> None:
                record["lifecycle"] = lifecycle
                if requested_by is not None:
                    record["cancelation"] = {"requested_by": requested_by, "requested_at": int(time.time())}
            return self._mutate(execution_id, owner, mutate)
        finally:
            self.unlock(execution_id, owner)

    def set_canceled(self, execution_id: str) -> None:
        record = self.read_execution_record(execution_id)
        if record["lifecycle"] not in ("cancel-requested", "cancel-ready", "canceled"):
            raise DmlRepoError(f"Execution {execution_id} is not cancelling and cannot be marked canceled")
        self._mark_lifecycle(execution_id, "canceled")

    def _invoke_cancel_adapter(self, execution_id: str, requested_by: str | None, db: DmlDB) -> str:
        owner = self._wait_acquire(execution_id)
        try:
            record = self.read_execution_record(execution_id)
            if record["cache_key"] is None or record["argv_ref"] is None:
                return "inactive"
            argv = self._materialize_record_ref(record["argv_ref"], db, "node-argv")
            assert argv is not None
            with db.tx(readonly=True) as txn:
                datum_ref, _ = txn.get(argv).datum_ref(txn)
                assert datum_ref is not None
                runnable = txn.get(txn.get(datum_ref).value(txn)[0]).value(txn)
            response = self._call_adapter(
                AdapterCancelRequest(
                    operation="cancel",
                    cache_key=record["cache_key"],
                    execution_id=execution_id,
                    argv_ref=record["argv_ref"],
                    remote={"root": self.root_uri},
                    runnable=asdict(runnable),
                    adapter_state=record["adapter_state"],
                    scratch_uri=self.adapter_scratch(execution_id),
                    requested_by=requested_by,
                )
            )
            adapter_state = response.get("adapter_state")
            if isinstance(adapter_state, dict):
                self._mutate(execution_id, owner, lambda item: item.update(adapter_state=adapter_state))
            return "cancelled" if response.get("status") == "cancelled" else "inactive"
        finally:
            self.unlock(execution_id, owner)

    def _plan_cancel(self, execution_ids: Sequence[str], requested_by: str) -> None:
        pending = list(execution_ids)
        seen: set[str] = set()
        while pending:
            execution_id = pending.pop()
            if execution_id in seen:
                continue
            seen.add(execution_id)
            try:
                record = self.read_execution_record(execution_id)
            except DmlRepoError:
                continue
            if self.list_execution_callers(execution_id):
                continue
            owner = self._wait_acquire(execution_id)
            try:
                def mutate(item: ExecutionRecord, execution_id: str = execution_id) -> None:
                    if item["lifecycle"] in ("succeeded", "failed", "canceled"):
                        raise BadExecutionStatusError(
                            f"Execution {execution_id} is {item['lifecycle']} and cannot be canceled",
                            lifecycle=item["lifecycle"],
                        )
                    item["lifecycle"] = "cancel-requested"
                    item["cancelation"] = {"requested_by": requested_by, "requested_at": int(time.time())}
                record = self._mutate(execution_id, owner, mutate)
                if record["cache_key"] is not None:
                    self._delete_cache(record["cache_key"], execution_id)
                for child in record["spawned_execution_ids"]:
                    self.delete_execution_dependency(caller_execution_id=execution_id, callee_execution_id=child)
                    if not self.list_execution_callers(child):
                        pending.append(child)
            finally:
                self.unlock(execution_id, owner)

    def _run_cancel_driver(self, execution_id: str, requested_by: str | None, db: DmlDB, **_kw) -> dict:
        plan = {"active-callers": [], "inactive": [], "cancelled": [], "timeout": [], "error": []}
        record = self.read_execution_record(execution_id)
        if record["lifecycle"] not in ("cancel-requested", "cancel-ready"):
            return plan
        cancelation = record["cancelation"]
        timed_out = (
            cancelation is not None and time.time() - cancelation["requested_at"] >= CANCEL_READY_TIMEOUT_SECONDS
        )
        if record["lifecycle"] == "cancel-ready" and timed_out:
            response = self._invoke_cancel_adapter(execution_id, requested_by, db)
            self.set_canceled(execution_id)
            plan[response].append({"cache_key": record["cache_key"], "execution_id": execution_id})
            return plan
        waiting = False
        for child in record["spawned_execution_ids"]:
            child_record = self.read_execution_record(child)
            if child_record["lifecycle"] == "cancel-requested":
                child_cancelation = child_record["cancelation"]
                child_timed_out = (
                    child_cancelation is not None
                    and time.time() - child_cancelation["requested_at"] >= CANCEL_READY_TIMEOUT_SECONDS
                )
                if not child_timed_out:
                    waiting = True
                    continue
                response = self._invoke_cancel_adapter(child, requested_by, db)
                self.set_canceled(child)
                plan[response].append({"cache_key": child_record["cache_key"], "execution_id": child})
            elif child_record["lifecycle"] == "cancel-ready":
                response = self._invoke_cancel_adapter(child, requested_by, db)
                self.set_canceled(child)
                plan[response].append({"cache_key": child_record["cache_key"], "execution_id": child})
        if not waiting and self.read_execution_record(execution_id)["lifecycle"] == "cancel-requested":
            self._mark_lifecycle(execution_id, "cancel-ready")
            if timed_out:
                record = self.read_execution_record(execution_id)
                response = self._invoke_cancel_adapter(execution_id, requested_by, db)
                self.set_canceled(execution_id)
                plan[response].append({"cache_key": record["cache_key"], "execution_id": execution_id})
        return plan

    def cancel(
        self,
        execution_id: str,
        requested_by: str | None,
        db: DmlDB,
        *,
        mode: Literal["full", "drive"] = "full",
    ) -> dict:
        record = self.read_execution_record(execution_id)
        if record["lifecycle"] in ("succeeded", "failed", "canceled"):
            raise BadExecutionStatusError(
                f"Execution {execution_id} is {record['lifecycle']} and cannot be canceled",
                lifecycle=record["lifecycle"],
            )
        effective = requested_by or (None if record["cancelation"] is None else record["cancelation"]["requested_by"])
        if mode == "full":
            if requested_by is None:
                raise DmlRepoError("requested_by is required for full cancellation")
            self._plan_cancel([execution_id], requested_by)
        return self._run_cancel_driver(execution_id, effective, db)
