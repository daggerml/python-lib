from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from daggerml._core.db import Ref
from daggerml._core.exec_state import ExecutionGraph
from daggerml._core.index import IndexOps
from daggerml._core.s3_cas import CasItem, CasItemConflict
from daggerml._core.types import BadExecutionStatusError, CanceledExecutionError, DmlDB, DmlRepoError

DEFAULT_REMOTE_ROOT = "s3://bucket/root"


def execution_record(
    execution_id: str,
    *,
    cache_key: str | None = None,
    lifecycle: str = "running",
    argv_ref: str | None = None,
    created_at: int = 0,
    updated_at: int = 0,
    spawned_execution_ids: list[str] | None = None,
    child_execution_ids: list[str] | None = None,
    cancelation: dict[str, Any] | None = None,
    invalidation: dict[str, Any] | None = None,
    result_ref: str | None = None,
    result_source: str | None = None,
    adapter_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "metadata": {
            "execution_id": execution_id,
            "cache_key": cache_key,
            "argv_ref": argv_ref,
            "created_at": created_at,
        },
        "state": {
            "lifecycle": lifecycle,
            "result_ref": result_ref,
            "result_source": result_source,
            "spawned_execution_ids": spawned_execution_ids or [],
            "child_execution_ids": child_execution_ids or [],
            "cancelation": cancelation,
            "invalidation": invalidation,
            "updated_at": updated_at,
        },
        "driver": {
            "lock": None,
            "not_before": None,
            "adapter_state": adapter_state,
            "cleanup": None,
        },
    }


def make_db(path: Path) -> DmlDB:
    return DmlDB(str(path), 1024 * 1024, 64 * 1024 * 1024)


def make_local_dml(
    project_home: Path,
    monkeypatch,
    *,
    user: str = "tester",
    remote_root: str = DEFAULT_REMOTE_ROOT,
):
    import daggerml._core.dml as dml_mod
    from daggerml._core.dml import Dml

    project_home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))
    init_kwargs = {"user": user, "remote_root": remote_root}
    Dml.init(str(project_home), **init_kwargs)
    ops = local_index_ops()
    monkeypatch.setattr(dml_mod, "_index_ops", lambda dml: ops)
    return Dml(str(project_home), remote_root=remote_root, user=user)


def commit_literal_dag(
    dml,
    dag_name: str,
    value: Any,
    *,
    node_name: str = "value",
    message: str | None = None,
) -> Ref:
    index = dml.runtime.create()
    node = dml.runtime.put_literal(index, value, name=node_name)
    dml.runtime.commit(index, node, name=dag_name, message=message or f"commit {dag_name}")
    commit = dml.status()["commit"]
    assert commit is not None
    return commit


def run_parallel(count: int, fn: Callable[[int], Any]) -> list[Any]:
    barrier = threading.Barrier(count)

    def run(i: int) -> Any:
        barrier.wait(timeout=5)
        return fn(i)

    with ThreadPoolExecutor(max_workers=count) as pool:
        return [future.result(timeout=10) for future in [pool.submit(run, i) for i in range(count)]]


class NoopExecutionState:
    def __init__(self) -> None:
        self.records: dict[str, dict[str, Any]] = {}
        self.cancel_calls: list[tuple[str, str | None, str]] = []

    def create_execution_record(self, record: dict[str, Any]) -> bool:
        metadata = record["metadata"]
        execution_id = metadata["execution_id"]
        if execution_id in self.records:
            return False
        self.records[execution_id] = record
        return True

    def reserve_execution(self, argv_ref, execution_id=None):
        execution_id = execution_id or "reserved"
        self.create_execution_record(
            {
                "metadata": {
                    "execution_id": execution_id,
                    "cache_key": None,
                    "argv_ref": None if argv_ref is None else argv_ref.to,
                    "created_at": int(time.time()),
                },
                "state": {
                    "lifecycle": "running",
                    "result_ref": None,
                    "result_source": None,
                    "spawned_execution_ids": [],
                    "child_execution_ids": [],
                    "cancelation": None,
                    "invalidation": None,
                    "updated_at": int(time.time()),
                },
                "driver": {
                    "lock": {"owner": "owner", "ttl": 300.0},
                    "not_before": None,
                    "adapter_state": None,
                    "cleanup": None,
                },
            }
        )
        return execution_id, "owner", None

    def activate(self, execution_id, db):
        record = self.require_mutation(execution_id, db, mode="activation")
        return record, "owner"

    def mark_running(self, execution_id, owner):
        record = self.read_execution_record(execution_id)
        record["state"]["lifecycle"] = "running"
        self.update_execution_record(record)

    def unlock(self, execution_id, owner):
        return True

    def update_execution_record(self, record: dict[str, Any]) -> None:
        self.records[record["metadata"]["execution_id"]] = record

    def read_execution_record(self, execution_id: str) -> dict[str, Any]:
        try:
            return dict(self.records[execution_id])
        except KeyError as exc:
            raise DmlRepoError(f"No execution record found for execution_id: {execution_id}") from exc

    def finish_execution(self, execution_id: str, dag: Ref, db: DmlDB) -> None:
        record = self.read_execution_record(execution_id)
        record["state"].update(
            lifecycle="succeeded",
            result_ref=dag.to,
            result_source="runtime",
            updated_at=int(time.time()),
        )
        self.update_execution_record(record)

    def require_mutation(self, execution_id: str, db: DmlDB, *, mode: str = "activation") -> dict[str, Any]:
        record = self.read_execution_record(execution_id)
        lifecycle = record["state"]["lifecycle"]
        allowed = "pending" if mode == "activation" else "running"
        action = "activated" if mode == "activation" else "mutated"
        if lifecycle == allowed:
            return record
        if lifecycle in ("cancel-pending", "canceled"):
            raise CanceledExecutionError(
                f"Execution {execution_id} is {lifecycle} and cannot be {action}", lifecycle=lifecycle
            )
        raise BadExecutionStatusError(
            f"Execution {execution_id} is {lifecycle} and cannot be {action}", lifecycle=lifecycle
        )

    def cancel(self, execution_id: str, requested_by: str | None, db: DmlDB, *, mode: str = "full") -> dict:
        self.cancel_calls.append((execution_id, requested_by, mode))
        return {"active-callers": [], "inactive": [], "cancelled": [], "timeout": [], "error": []}

    def describe_graph(self, root_execution_ids: list[str]) -> ExecutionGraph:
        roots = list(dict.fromkeys(root_execution_ids))
        nodes = {}
        pending = list(reversed(roots))
        while pending:
            execution_id = pending.pop()
            if execution_id in nodes:
                continue
            record = self.read_execution_record(execution_id)
            metadata = record["metadata"]
            state = record["state"]
            spawned = list(state["spawned_execution_ids"])
            children = list(state["child_execution_ids"])
            nodes[execution_id] = {
                "execution_id": metadata["execution_id"],
                "cache_key": metadata["cache_key"],
                "lifecycle": state["lifecycle"],
                "updated_at": state["updated_at"],
                "created_at": metadata["created_at"],
                "cancel_requested_by": (
                    None if state["cancelation"] is None else state["cancelation"]["requested_by"]
                ),
                "children": children,
                "spawned": spawned,
            }
            pending.extend(reversed(children))
            pending.extend(reversed(spawned))
        return {"roots": roots, "nodes": nodes}


class FakeRemote:
    n_workers = 1

    class _Store:
        client = None

    _store = _Store()
    materialized_ref = None

    def get_active(self, cache_key: str, raw: bool = False):
        return None

    def materialize_ref(self, ref, db):
        return self.materialized_ref or ref


def local_index_ops(state: NoopExecutionState | None = None) -> IndexOps:
    ops = object.__new__(IndexOps)
    ops.remote_root = "s3://bucket/root"
    ops._remote = FakeRemote()
    state = state or NoopExecutionState()
    ops.exec_state = lambda cache_key=None: state  # type: ignore[method-assign]
    return ops


class MissingKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeCasStore:
    bucket = "bucket"
    client = None

    def __init__(self, prefix: str = "root/exec") -> None:
        self.prefix = prefix
        self.objects: dict[str, tuple[str, str, datetime]] = {}
        self.conflict_keys: set[str] = set()
        self.now = datetime.now(timezone.utc)
        self._lock = threading.Lock()

    def _key_for(self, relative_key: str) -> str:
        return f"{self.prefix}/{relative_key}" if self.prefix else relative_key

    @staticmethod
    def _is_missing_error(exc: Exception) -> bool:
        return getattr(exc, "response", {}).get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}

    def _get(self, key: str, *, cas: bool = False):
        with self._lock:
            if key not in self.objects:
                raise MissingKey()
            data, etag, last_modified = self.objects[key]
            return CasItem(key, data, etag, last_modified, self.now) if cas else data

    def _put(self, key: str | CasItem, value: str | bytes, *, overwrite: bool = True, **kwargs) -> bool:
        expected_etag = None
        if isinstance(key, CasItem):
            expected_etag = key.etag
            key = key.key
        with self._lock:
            if key in self.conflict_keys:
                self.conflict_keys.remove(key)
                raise CasItemConflict(key)
            if not overwrite and key in self.objects:
                raise CasItemConflict(key)
            if expected_etag is not None and self.objects.get(key, (None, None, None))[1] != expected_etag:
                raise CasItemConflict(key)
            text = value.decode() if isinstance(value, bytes) else value
            current = int(self.objects.get(key, ("", "0", self.now))[1])
            self.now += timedelta(milliseconds=1)
            self.objects[key] = (text, str(current + 1), self.now)
        return True

    def _put_js(self, key: str | CasItem, value: Any, *, overwrite: bool = True, **kwargs) -> bool:
        return self._put(key, json.dumps(value, separators=(",", ":"), sort_keys=True), overwrite=overwrite)

    def _delete(self, key: str | CasItem, **kwargs) -> bool:
        expected_etag = None
        if isinstance(key, CasItem):
            expected_etag = key.etag
            key = key.key
        with self._lock:
            if key not in self.objects:
                return False
            if expected_etag is not None and self.objects[key][1] != expected_etag:
                return False
            del self.objects[key]
            return True

    def _iter(self, prefix: str):
        return (key for key in sorted(self.objects) if key.startswith(prefix))


class FakeExecutionRemote:
    def __init__(self) -> None:
        self.active: dict[str, dict[str, Any]] = {}
        self.cancel_targets: dict[str, dict[str, Any]] = {}
        self.cache: dict[str, Ref] = {}
        self.transport: dict[str, Ref] = {}

    def upload_object_graph(self, ref, db) -> None:
        return None

    def materialize_ref(self, ref, db):
        return ref

    def get_cache(self, cache_key: str, db=None):
        return self.cache.get(cache_key)

    def get_active(self, cache_key: str, raw: bool = False):
        return self.active.get(cache_key)

    def put_active(self, cache_key: str, execution_id: str, argv: Ref, db=None) -> None:
        self.active[cache_key] = {"meta": {"execution_id": execution_id}, "argv": argv.to}

    def delete_active(self, cache_key: str) -> None:
        self.active.pop(cache_key, None)

    def move_active_to_cancel_target(self, cache_key: str, execution_id: str) -> bool:
        active = self.active.get(cache_key)
        if active is None or active["meta"]["execution_id"] != execution_id:
            return False
        self.cancel_targets[execution_id] = self.active.pop(cache_key)
        return True

    def get_cancel_target(self, execution_id: str, raw: bool = False):
        return self.cancel_targets.get(execution_id)

    def delete_cancel_target(self, execution_id: str) -> None:
        self.cancel_targets.pop(execution_id, None)

    def put_cache(self, dag: Ref, active_id: str, db=None) -> None:
        self.cache[active_id] = dag

    def get_transport(self, active_id: str, db=None):
        return None

    def delete_transport(self, active_id: str) -> None:
        self.transport.pop(active_id, None)

    def put_transport(self, execution_id: str, dag: Ref, db=None) -> None:
        self.transport[execution_id] = dag
