"""Remote execution coordination backed by S3.

`ExecutionState` owns the `exec/` prefix under the configured remote root. That
prefix stores the coordination records needed to manage one in-flight execution
across multiple callers and machines:

exec/
    lock/{cache_key}.json               # advisory lock for claiming work on a cache key
    launch/{execution_id}.json          # immutable adapter resume state for an execution
    state/{execution_id}.json           # mutable lifecycle record for an execution
    edge/{callee_id}/{caller_id}.json   # caller -> callee dependency edge
    invalidate/{execution_id}.json      # invalidation tombstone for a cached execution
    io/{exec_id}/                       # adapter scratch / transport space

This module works alongside `remote.py`, which owns two related refs outside of
`exec/`:

- `active` points remote callers at the execution currently coordinating work
  for a cache key.
- `cache` publishes the completed DAG result for that cache key.

That split is intentional. The `active` pointer is a coordination target for
concurrent remote callers, while the `exec/` records track lifecycle, resume,
lineage, invalidation, and adapter state for a specific execution id.

Cancellation is best-effort. When cancellation starts, the runtime may clear
the `active` pointer before adapter confirmation so new callers do not inherit
an execution that is being cancelled, even if that older execution continues
running until the adapter eventually stops it or times out.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from dataclasses import InitVar, asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Optional, Sequence, TypedDict, cast
from uuid import uuid4

from daggerml._core.remote import Remote
from daggerml._core.s3_cas import CasItem, CasItemConflict, S3Remote
from daggerml._core.types import Dag, DmlDB, DmlRepoError, Error, Index, Ref, Runnable
from daggerml._core.util import uuid7

if TYPE_CHECKING:
    import boto3

logger = logging.getLogger(__name__)
LOCK_TTL: float = 300.0
EXECUTION_LIFECYCLES = Literal[
    "pending", "running", "succeeded", "failed", "cancel-pending", "cancel-ready", "canceled"
]
ADAPTER_LIFECYCLES = Literal["running", "succeeded", "failed", "cancelled"]
GRAPH_LIFECYCLES = Literal["running", "succeeded", "failed", "cancel-pending", "cancel-ready", "canceled", "pending"]
INDEX_LIFECYCLES = Literal["active", "inactive", "canceled"]


class CancellationError(DmlRepoError):
    """Raised when local or remote cancellation blocks further work."""

    def __init__(self, message: str, *, lifecycle: str | None = None):
        super().__init__(message, type="cancellationerror")
        self.lifecycle = lifecycle


class LockRecord(TypedDict):
    lock_token: str
    lock_expires_ts: float


class LaunchState(TypedDict):
    execution_id: str
    cache_key: str
    created_at: int
    resume_state: dict[str, Any]


class ExecutionRecord(TypedDict):
    execution_id: str
    cache_key: str | None
    lifecycle: EXECUTION_LIFECYCLES
    updated_at: int
    created_at: int
    spawned_execution_ids: list[str]
    child_execution_ids: list[str]
    cancellation_requested_by: str | None


class InvalidationRecord(TypedDict):
    execution_id: str
    cache_key: str
    requested_by: str
    requested_at: int


class InvalidationResponse(TypedDict):
    total_time: float
    invalidations: list[InvalidationRecord]


class RemotePayload(TypedDict):
    root: str


class AdapterEnvelope(TypedDict):
    cache_key: str
    execution_id: str
    remote: RemotePayload
    runnable: dict
    state: dict | None
    scratch_uri: str
    cancel_requested_by: str | None


class AdapterResponse(TypedDict):
    lifecycle: ADAPTER_LIFECYCLES
    state: dict | None
    dag_id: str | None
    error: str | None


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
    """S3-backed advisory mutex for function execution.

    Function-execution coordination lives under ``{prefix}/dml/`` with:

    - ``lock/{cache_key}.json`` for the advisory mutex,
    - ``state/{execution_id}.json`` for mutable execution state,
    - ``edge/<callee_execution_id>/<caller_execution_id>.json`` for canonical lineage,
    - ``invalidate/{execution_id}.json`` for invalidation tombstones,
    - ``io/{exec_id}/`` for adapter scratch space.

    Lock lifecycle is create-only (``If-None-Match: *``) then delete — no
    updates.
    """

    root_uri: str
    n_workers: int
    client: InitVar["boto3.client"]
    cache_key: Optional[str] = None
    _store: S3Remote = field(init=False)
    _cas: dict[str, CasItem] = field(init=False, default_factory=dict)

    def __post_init__(self, client) -> None:
        self._store = S3Remote(self.root_uri.rstrip("/") + "/exec", client=client)
        self._remote = Remote(self.root_uri, self.n_workers, client=client)

    @classmethod
    def from_execution_id(cls, execution_id: str, *, root_uri: str, n_workers: int, client=None) -> ExecutionState:
        """Instantiate from an execution ID by reading the launch state."""
        temp_state = cls(root_uri, n_workers, client=client)
        record = temp_state.read_execution_record(execution_id)
        return cls(root_uri, n_workers, cache_key=record["cache_key"], client=client)

    @property
    def _lock_key(self) -> str:
        if self.cache_key is None:
            raise DmlRepoError("cache_key is required for lock operations")
        return self._store._key_for(f"lock/{self.cache_key}.json")

    def adapter_scratch(self, exec_id: str) -> str:
        """S3 URI for adapter scratch space for the current execution attempt."""
        if self.cache_key is None:
            raise DmlRepoError("cache_key is required for adapter scratch space")
        key = self._store._key_for(f"io/{exec_id}/")
        return f"s3://{self._store.bucket}/{key}"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read(self, key: str, *, raw: bool = False):
        try:
            item = self._store._get(key, cas=True)
        except Exception as exc:
            if self._store._is_missing_error(exc):
                self._cas.pop(key, None)
                return None
            raise
        self._cas[key] = item
        return item.data if raw else item.json

    def _write(self, key: str, data, *, raw: bool = False, overwrite: bool = False) -> bool:
        writer = self._store._put if raw else self._store._put_js
        try:
            writer(key, data, overwrite=overwrite)
        except CasItemConflict:
            return False
        self._cas[key] = self._store._get(key, cas=True)
        return True

    def _update(self, key: str, data, *, raw: bool = False) -> None:
        current = self._cas.get(key)
        if current is None:
            try:
                current = self._store._get(key, cas=True)
            except Exception as exc:
                if self._store._is_missing_error(exc):
                    raise DmlRepoError(f"CAS item must be read before update: s3://{self._store.bucket}/{key}") from exc
                raise
        writer = self._store._put if raw else self._store._put_js
        writer(current, data)
        self._cas[key] = self._store._get(key, cas=True)

    def _delete(self, key: str, cas=False) -> bool:
        item = self._cas.pop(key, None) if cas else key
        if item is None:
            return False
        return self._store._delete(item)

    def _key_for_launch_state(self, execution_id: str) -> str:
        return self._store._key_for(f"launch/{execution_id}.json")

    def _key_for_execution(self, execution_id: str) -> str:
        return self._store._key_for(f"state/{execution_id}.json")

    def _key_for_edge_prefix(self, callee_execution_id: str) -> str:
        return self._store._key_for(f"edge/{callee_execution_id}/")

    def _key_for_edge(self, callee_execution_id: str, caller_execution_id: str) -> str:
        return f"{self._key_for_edge_prefix(callee_execution_id)}{caller_execution_id}.json"

    def _call_adapter(self, envelope: AdapterEnvelope) -> AdapterResponse:
        if self.cache_key is None:
            raise DmlRepoError("cache_key is required for adapter calls")
        adapter = envelope["runnable"]["adapter"]
        adapter_path = shutil.which(adapter)
        if adapter_path is None:
            raise DmlRepoError(f"Adapter executable not found: {adapter}")
        inp = json.dumps(envelope, sort_keys=True, separators=(",", ":"))
        result = subprocess.run([adapter_path], input=inp, text=True, capture_output=True)
        if result.returncode != 0:
            raise DmlRepoError(f"Adapter process failed with code {result.returncode}: {result.stderr}")
        try:
            response = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise DmlRepoError(f"Failed to parse adapter response as JSON: {result.stdout}") from exc
        return AdapterResponse(**response)

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
        existing = cast(Optional[LockRecord], self._read(self._lock_key))
        if existing is not None:
            if existing["lock_expires_ts"] > now:
                # Lock is currently held by someone else
                return False
            # Lock is expired — steal it
            if not self._delete(self._lock_key, cas=True):
                # Failed to delete, which means someone else stole it first — return False
                return False
        return self._write(self._lock_key, {"lock_expires_ts": now + ttl, "lock_token": uuid4().hex})

    def unlock(self) -> None:
        """Release the advisory lock by deleting the lock file.

        This is a best-effort delete; if the file is already absent (e.g.
        expired and stolen), the call is a no-op.
        """
        self._delete(self._lock_key, cas=True)

    def get_active_execution_id(self) -> Optional[str]:
        """Get the active execution ID for the current cache key, if any."""
        if self.cache_key is None:
            raise DmlRepoError("cache_key is required for active execution operations")
        record = cast(dict, self._remote.get_active(self.cache_key, raw=True))
        return record["meta"]["execution_id"] if record is not None else None

    def put_active_execution(self, execution_id: str, argv: Ref, db) -> None:
        """Put an active execution record for the current cache key."""
        if self.cache_key is None:
            raise DmlRepoError("cache_key is required for active execution operations")
        self._remote.put_active(self.cache_key, execution_id, argv, db)

    def read_launch_state(self, execution_id: str) -> dict | None:
        ls = cast(LaunchState, self._read(self._key_for_launch_state(execution_id)))
        return ls["resume_state"] if ls is not None else None

    def create_launch_state(self, execution_id: str, launch_state: dict) -> bool:
        if self.cache_key is None:
            raise DmlRepoError("cache_key is required for launch state operations")
        payload = LaunchState(
            execution_id=execution_id,
            cache_key=self.cache_key,
            created_at=int(time.time()),
            resume_state=launch_state,
        )
        return self._write(self._key_for_launch_state(execution_id), payload, overwrite=False)

    def update_launch_state(self, execution_id: str, launch_state: dict) -> None:
        if self.cache_key is None:
            raise DmlRepoError("cache_key is required for launch state operations")
        payload = LaunchState(
            execution_id=execution_id,
            cache_key=self.cache_key,
            created_at=int(time.time()),
            resume_state=launch_state,
        )
        self._update(self._key_for_launch_state(execution_id), payload)

    def create_execution_record(self, record: ExecutionRecord) -> bool:
        # owned by the execution runtime
        return self._write(self._key_for_execution(record["execution_id"]), record, overwrite=False)

    def reserve_execution(self, execution_id: str | None = None) -> str:
        if self.cache_key is None:
            raise DmlRepoError("cache_key is required for execution reservation")
        now_ts = int(time.time())
        exec_id = execution_id or uuid7().hex
        created = self.create_execution_record(
            {
                "execution_id": exec_id,
                "cache_key": self.cache_key,
                "lifecycle": "pending",
                "updated_at": now_ts,
                "created_at": now_ts,
                "spawned_execution_ids": [],
                "child_execution_ids": [],
                "cancellation_requested_by": None,
            }
        )
        if not created:
            raise DmlRepoError(f"Execution record already exists for execution_id: {exec_id}")
        return exec_id

    def read_execution_record(self, execution_id: str) -> ExecutionRecord:
        resp = self._read(self._key_for_execution(execution_id))
        if resp is None:
            raise DmlRepoError(f"No execution record found for execution_id: {execution_id}")
        return cast(ExecutionRecord, resp)

    def update_execution_record(self, record: ExecutionRecord) -> None:
        self._update(self._key_for_execution(record["execution_id"]), record)

    @staticmethod
    def _is_cancel_lifecycle(lifecycle: str) -> bool:
        return lifecycle.startswith("cancel") or lifecycle == "canceled"

    def _set_local_index_lifecycle(self, execution_id: str, lifecycle: INDEX_LIFECYCLES, db: DmlDB) -> None:
        with db.tx() as txn:
            index_ref = Ref(f"index:{execution_id}")
            try:
                index_obj = cast(Index, txn.get(index_ref))
            except DmlRepoError:
                return
            if index_obj.lifecycle == "canceled" or index_obj.lifecycle == lifecycle:
                return
            index_obj.lifecycle = lifecycle
            txn.put(index_obj, to=index_ref)

    def _add_spawned_execution(self, caller_id: str, callee_id: str) -> None:
        for _ in range(3):
            caller_record = self.read_execution_record(caller_id)
            if caller_record is None:
                raise DmlRepoError(f"No execution record found for caller execution_id: {caller_id}")
            if caller_record["lifecycle"] != "running":
                msg = f"Execution {caller_id} status: {caller_record['lifecycle']} cannot spawn new executions"
                raise CancellationError(msg, lifecycle=caller_record["lifecycle"])
            caller_record["spawned_execution_ids"] = sorted({*caller_record["spawned_execution_ids"], callee_id})
            try:
                return self.update_execution_record(caller_record)
            except CasItemConflict:
                # Retry on conflict, which means the caller record was concurrently updated
                logger.warning(f"CAS conflict when updating execution record for {caller_id}...")
        logger.warning(
            f"Failed to update execution record for {caller_id} after 3 attempts due to CAS conflicts... "
            "This execution may be un-cancelable."
        )

    def _complete_spawned_execution(self, caller_id: str, callee_id: str) -> None:
        for _ in range(3):
            caller_record = self.read_execution_record(caller_id)
            if caller_record is None:
                raise DmlRepoError(f"No execution record found for caller execution_id: {caller_id}")
            if caller_record["lifecycle"] != "running":
                msg = f"Execution {caller_id} status: {caller_record['lifecycle']} cannot spawn new executions"
                raise CancellationError(msg, lifecycle=caller_record["lifecycle"])
            caller_record["spawned_execution_ids"] = sorted(set(caller_record["spawned_execution_ids"]) - {callee_id})
            caller_record["child_execution_ids"] = sorted({*caller_record["child_execution_ids"], callee_id})
            try:
                return self.update_execution_record(caller_record)
            except CasItemConflict:
                # Retry on conflict, which means the caller record was concurrently updated
                logger.warning(f"CAS conflict when updating execution record for {caller_id}, retrying...")
        logger.warning(
            f"Failed to finalize spawned execution for {caller_id} after 3 attempts due to CAS conflicts... "
            "Cancel might get annoying"
        )

    def describe_graph(self, root_execution_ids: Sequence[str]) -> ExecutionGraph:
        roots = list(dict.fromkeys(root_execution_ids))
        nodes: dict[str, ExecutionGraphNode] = {}
        pending = list(reversed(roots))
        while pending:
            execution_id = pending.pop()
            if execution_id in nodes:
                continue
            record = self._read(self._key_for_execution(execution_id))
            if record is None:
                nodes[execution_id] = {
                    "execution_id": execution_id,
                    "cache_key": None,
                    "lifecycle": "pending",
                    "updated_at": 0,
                    "created_at": 0,
                    "cancel_requested_by": None,
                    "children": [],
                    "spawned": [],
                }
                continue
            record = cast(ExecutionRecord, record)
            spawned = list(record["spawned_execution_ids"])
            children = list(record["child_execution_ids"])
            nodes[execution_id] = {
                "execution_id": record["execution_id"],
                "cache_key": record["cache_key"],
                "lifecycle": record["lifecycle"],
                "updated_at": record["updated_at"],
                "created_at": record["created_at"],
                "cancel_requested_by": record["cancellation_requested_by"],
                "children": children,
                "spawned": spawned,
            }
            pending.extend(reversed(children))
            pending.extend(reversed(spawned))
        return {"roots": roots, "nodes": nodes}

    def _record_execution_dependency(self, caller_id: str, callee_id: str) -> None:
        edge = {"caller_execution_id": caller_id, "callee_execution_id": callee_id}
        key = self._key_for_edge(callee_id, caller_id)
        self._write(key, edge, overwrite=True)

    def get_or_start_fn(self, index: Ref, runnable: Runnable, argv_node: Ref, db) -> Ref | None:
        """Get the active execution ID or start a new execution if none is active."""
        if self.cache_key is None:
            raise DmlRepoError("cache_key is required for execution operations")
        resp = self._remote.get_cache(self.cache_key, db=db)
        if resp is not None:
            return resp  # Step 1: Cache hit
        # Step 2: Not in cache. Lock execution
        if not self.lock():
            logger.warning(f"`start_fn` failed to acquire lock for cache key: {self.cache_key}. Returning None")
            return None
        try:
            # Step 3: Re-check cache after acquiring lock, then call adapter if still not cached
            resp = self._remote.get_cache(self.cache_key, db=db)
            if resp is not None:
                return resp
            active_id = self.get_active_execution_id()
            if active_id is None:
                active_id = self.reserve_execution()
                self.put_active_execution(active_id, argv=argv_node, db=db)
            else:
                try:
                    active_record = self.read_execution_record(active_id)
                except DmlRepoError:
                    self._remote.delete_active(self.cache_key)
                    active_id = self.reserve_execution()
                    self.put_active_execution(active_id, argv=argv_node, db=db)
                else:
                    if self._is_cancel_lifecycle(active_record["lifecycle"]):
                        self._remote.delete_active(self.cache_key)
                        active_id = self.reserve_execution()
                        self.put_active_execution(active_id, argv=argv_node, db=db)
            # add spawned execution to `index.id()`'s record
            self._record_execution_dependency(caller_id=index.id(), callee_id=active_id)
            self._add_spawned_execution(caller_id=index.id(), callee_id=active_id)
            # get launch state
            launch_state = self.read_launch_state(active_id)
            adapter_envelope = AdapterEnvelope(
                cache_key=self.cache_key,
                execution_id=active_id,
                remote={"root": self.root_uri},
                runnable=asdict(runnable),
                state=launch_state,
                scratch_uri=self.adapter_scratch(active_id),
                cancel_requested_by=None,
            )
            try:
                resp = self._call_adapter(adapter_envelope)
            except Exception as exc:
                try:
                    record = self.read_execution_record(active_id)
                except DmlRepoError:
                    record = None
                if record is not None and not self._is_cancel_lifecycle(record["lifecycle"]):
                    record.update({"lifecycle": "failed", "updated_at": int(time.time())})
                    self.update_execution_record(record)
                logger.error(f"Adapter call failed for execution {active_id}: {exc}")
                raise DmlRepoError(f"Adapter call failed for execution {active_id}") from exc
            dag = None
            if resp["lifecycle"] == "running" and launch_state is None:
                self.create_launch_state(active_id, resp["state"] or {})
            elif resp["lifecycle"] == "running":
                self.update_launch_state(active_id, resp["state"] or {})
            elif resp["lifecycle"] == "cancelled":
                raise CancellationError(f"Execution {active_id} was cancelled", lifecycle="canceled")
            else:
                dag = self._remote.get_transport(active_id, db=db)
                if dag is not None:
                    logger.info(f"Execution {active_id} succeeded with DAG {dag.id}")
                    self._remote.delete_transport(active_id)
                else:
                    record = self.read_execution_record(active_id)
                    if not self._is_cancel_lifecycle(record["lifecycle"]):
                        record.update({"lifecycle": "failed", "updated_at": int(time.time())})
                        self.update_execution_record(record)
                    logger.error(f"Execution {active_id} failed with error: {resp['error']}")
                    error_msg = resp["error"] or "Unknown error"
                    with db.tx() as txn:
                        error = txn.put(Error(error_msg, "fn-call", "adapter-error"))
                        dag = txn.put(Dag(nodes=[argv_node], names={}, argv=argv_node, error=error))
                self._complete_spawned_execution(caller_id=index.id(), callee_id=active_id)
                self._remote.put_cache(dag, active_id, db)
                self._remote.delete_active(self.cache_key)
        finally:
            self.unlock()
        return dag

    def finish_execution(self, execution_id: str, dag: Ref, db) -> None:
        record = self.read_execution_record(execution_id)
        if record is None:
            raise DmlRepoError(f"No execution record found for execution_id: {execution_id}")
        if self._is_cancel_lifecycle(record["lifecycle"]):
            msg = f"Execution {execution_id} status: {record['lifecycle']} cannot be finalized"
            raise CancellationError(msg, lifecycle=record["lifecycle"])
        record.update({"lifecycle": "succeeded", "updated_at": int(time.time())})
        self.update_execution_record(record)
        self._remote.put_transport(execution_id, dag, db)

    def delete_execution_dependency(self, *, caller_execution_id: str, callee_execution_id: str) -> None:
        self._delete(self._key_for_edge(callee_execution_id, caller_execution_id))

    def list_execution_callers(self, callee_execution_id: str) -> list[str]:
        prefix = self._key_for_edge_prefix(callee_execution_id)
        return [key.split("/")[-1].removesuffix(".json") for key in self._store._iter(prefix)]

    def invalidate_cache(self, cache_keys: Sequence[str], requested_by: str) -> InvalidationResponse:
        """Invalidate cache keys.

        Algorithm
        ---------
        0. set `seen: set[str] = set(), plan: set[str] = set()`
        def run_inval(cache_key, exec_id=None):
            1. read `ref` from `refs/cache/{cache_key}.json` in Remote. If not exists, return
            2. if `ref.execution_id != exec_id` and `exec_id is not None`: return
            3. look up `caller` edges of `ref.execution_id` and add caller execution ids to `plan`
            4. add `ref.execution_id` to `seen`
            5. write invalidation tombstone for `cache_key` with `requested_by` and current timestamp
            6. delete `refs/cache/{cache_key}.json` from Remote
        1. call `run_inval(cache_key, None)` for each `cache_key` in `cache_keys`
        2. while `plan` is not empty`:
            a. pop `exec_id` from `plan`
            b. If `exec_id` in `seen`: continue
            c. read `record` for `exec_id`. If not exists, continue
            d. call `run_inval(record.cache_key, exec_id)`
        return invalidation stats

        Sharp Bits
        ----------
        **Assumes no active callers**
            - Funk `A` calls `B`
            - User invalidates `B` while `A` is still running (`B` is considered "tainted")
            - `A` commits and publishes its cache
            - Tainted results from `B` are transitively in the cache.
            later versions could call `cancel` on `A`.
        """
        now = time.time()
        seen = set()
        plan = set()
        remote = Remote(self.root_uri, self.n_workers, self._store.client)
        invalidations = []  # just for stats

        def run_inval(cache_key: str, exec_id: str | None = None) -> None:
            # 1. read `ref` from `refs/cache/{cache_key}.json` in Remote. If not exists, return
            ref = remote._get_path(remote._cache_key(cache_key), raw=True)
            if ref is None:
                return
            # 2. if `ref.execution_id != exec_id` and `exec_id is not None`: return
            if exec_id is not None and ref["meta"]["execution_id"] != exec_id:
                return
            this_exec_id: str = ref["meta"]["execution_id"]
            # 3. look up `caller` edges of `ref.execution_id` and add caller execution ids to `plan`
            plan.update(self.list_execution_callers(this_exec_id))
            # 4. add `ref.execution_id` to `seen`
            seen.add(this_exec_id)
            # 5. write invalidation tombstone for `cache_key` with `requested_by` and current timestamp
            inval_key = self._store._key_for(f"invalidate/{this_exec_id}.json")
            inval_record = {
                "execution_id": this_exec_id,
                "cache_key": cache_key,
                "requested_by": requested_by,
                "requested_at": int(now),
            }
            self._write(inval_key, inval_record, overwrite=False)
            # 6. delete `refs/cache/{cache_key}.json` from Remote
            remote.delete_cache(cache_key)
            # Stats
            invalidations.append(inval_record)

        for cache_key in cache_keys:
            run_inval(cache_key)
        while plan:
            exec_id = plan.pop()
            if exec_id in seen:
                continue
            try:
                record = self.read_execution_record(exec_id)
            except DmlRepoError:
                continue
            if record["cache_key"] is not None:
                run_inval(record["cache_key"], exec_id)
        return {"total_time": time.time() - now, "invalidations": invalidations}

    def _state_for_execution(self, execution_id: str) -> tuple[ExecutionRecord, "ExecutionState"]:
        record = self.read_execution_record(execution_id)
        cache_key = record["cache_key"]
        if cache_key == self.cache_key:
            return record, self
        return record, ExecutionState(self.root_uri, self.n_workers, cache_key=cache_key, client=self._store.client)

    def _mark_execution_lifecycle(
        self,
        execution_id: str,
        lifecycle: EXECUTION_LIFECYCLES,
        *,
        requested_by: str | None = None,
    ) -> ExecutionRecord:
        record = self.read_execution_record(execution_id)
        record["lifecycle"] = lifecycle
        record["updated_at"] = int(time.time())
        if requested_by is not None:
            record["cancellation_requested_by"] = requested_by
        self.update_execution_record(record)
        return record

    def _set_cancel_ready(self, execution_id: str) -> None:
        record = self.read_execution_record(execution_id)
        if record["lifecycle"] not in ("cancel-pending", "cancel-ready"):
            msg = f"Execution {execution_id} is not pending cancellation and cannot be marked cancel-ready"
            raise DmlRepoError(msg)
        if record["lifecycle"] != "cancel-ready":
            self._mark_execution_lifecycle(execution_id, "cancel-ready")

    def set_canceled(self, execution_id: str) -> None:
        record = self.read_execution_record(execution_id)
        if record["lifecycle"] not in ("cancel-ready", "canceled"):
            raise DmlRepoError(f"Execution {execution_id} is not ready for cancellation and cannot be marked canceled")
        if record["lifecycle"] != "canceled":
            self._mark_execution_lifecycle(execution_id, "canceled")

    def _invoke_cancel_adapter(self, execution_id: str, requested_by: str | None, db: DmlDB) -> str:
        record = self.read_execution_record(execution_id)
        if record["cache_key"] is None:
            return "inactive"
        active = cast(dict | None, self._remote.get_active(record["cache_key"], raw=True))
        if active is None:
            return "inactive"
        with db.tx() as txn:
            argv = self._remote._materialize_manifest(cast(dict, active), txn, expected_root_ns="node-argv")
            argv_datum = txn.get(txn.get(argv).datum_ref(txn))
            runnable = txn.get(argv_datum.value(txn)[0]).value(txn)
        adapter_envelope = AdapterEnvelope(
            cache_key=record["cache_key"],
            execution_id=execution_id,
            remote={"root": self.root_uri},
            runnable=asdict(runnable),
            state=self.read_launch_state(execution_id),
            scratch_uri=self.adapter_scratch(execution_id),
            cancel_requested_by=requested_by,
        )
        try:
            resp = self._call_adapter(adapter_envelope)
        except Exception as exc:
            logger.error(f"Adapter call failed for cancellation of execution {execution_id}: {exc}")
            raise DmlRepoError(f"Adapter call failed for cancellation of execution {execution_id}") from exc
        return "cancelled" if resp["lifecycle"] == "cancelled" else "inactive"

    def _plan_cancel(self, execution_id: str, requested_by: str) -> None:
        record, state = self._state_for_execution(execution_id)
        locked = False
        if record["cache_key"] is not None:
            while not state.lock():
                time.sleep(0.1)
            locked = True
        try:
            record = state.read_execution_record(execution_id)
            if state.list_execution_callers(execution_id):
                return
            if record["lifecycle"] not in ("pending", "running", "cancel-pending", "cancel-ready"):
                return
            if record["lifecycle"] != "cancel-pending":
                state._mark_execution_lifecycle(execution_id, "cancel-pending", requested_by=requested_by)
                record = state.read_execution_record(execution_id)
            for spawned_id in list(record["spawned_execution_ids"]):
                state.delete_execution_dependency(caller_execution_id=execution_id, callee_execution_id=spawned_id)
                if not state.list_execution_callers(spawned_id):
                    self._plan_cancel(spawned_id, execution_id)
        finally:
            if locked:
                state.unlock()

    def _run_cancel_driver(
        self,
        execution_id: str,
        requested_by: str | None,
        db: DmlDB,
        *,
        timeout_seconds: float = 5.0,
    ) -> dict[str, list[dict[str, str | None]]]:
        self._set_local_index_lifecycle(execution_id, "inactive", db)
        plan: dict[str, list[dict[str, str | None]]] = {
            "active-callers": [],
            "inactive": [],
            "cancelled": [],
            "timeout": [],
            "error": [],
        }
        deadline = time.time() + timeout_seconds
        record = self.read_execution_record(execution_id)
        drive_set = set()
        for spawned_id in record["spawned_execution_ids"]:
            try:
                if self.read_execution_record(spawned_id)["lifecycle"] == "cancel-pending":
                    drive_set.add(spawned_id)
            except DmlRepoError:
                continue
        while drive_set and time.time() < deadline:
            progressed = False
            for spawned_id in list(drive_set):
                try:
                    spawned_record, spawned_state = self._state_for_execution(spawned_id)
                except DmlRepoError:
                    drive_set.remove(spawned_id)
                    continue
                locked = False
                if spawned_record["cache_key"] is not None:
                    while not spawned_state.lock():
                        time.sleep(0.1)
                    locked = True
                try:
                    spawned_record = spawned_state.read_execution_record(spawned_id)
                    if spawned_record["lifecycle"] == "cancel-ready":
                        resp = spawned_state._invoke_cancel_adapter(spawned_id, requested_by, db)
                        plan[resp].append(
                            {"cache_key": spawned_record["cache_key"], "execution_id": spawned_id}
                        )
                        drive_set.remove(spawned_id)
                        progressed = True
                    elif spawned_record["lifecycle"] != "cancel-pending":
                        plan["inactive"].append(
                            {"cache_key": spawned_record["cache_key"], "execution_id": spawned_id}
                        )
                        drive_set.remove(spawned_id)
                        progressed = True
                except Exception as exc:
                    logger.error(f"Error cancelling spawned execution {spawned_id}: {exc}")
                    plan["error"].append({"cache_key": spawned_record.get("cache_key"), "execution_id": spawned_id})
                    drive_set.remove(spawned_id)
                    progressed = True
                finally:
                    if locked:
                        spawned_state.unlock()
            if not progressed:
                time.sleep(0.1)
        for spawned_id in sorted(drive_set):
            try:
                cache_key = self.read_execution_record(spawned_id)["cache_key"]
            except DmlRepoError:
                cache_key = None
            plan["timeout"].append({"cache_key": cache_key, "execution_id": spawned_id})
        self._set_cancel_ready(execution_id)
        self._set_local_index_lifecycle(execution_id, "canceled", db)
        return plan

    def cancel(
        self,
        execution_id: str,
        requested_by: str | None,
        db: DmlDB,
        *,
        mode: Literal["full", "drive"] = "full",
    ) -> dict[str, list[dict[str, str | None]]]:
        record = self.read_execution_record(execution_id)
        if record["lifecycle"] not in ("pending", "running", "cancel-pending", "cancel-ready", "canceled"):
            raise DmlRepoError(f"Execution {execution_id} is not active and cannot be cancelled")
        if mode == "full":
            if requested_by is None:
                raise DmlRepoError("requested_by is required for full cancellation")
            self._plan_cancel(execution_id, requested_by)
        plan = self._run_cancel_driver(execution_id, requested_by, db)
        if mode == "full":
            self.set_canceled(execution_id)
        return plan
