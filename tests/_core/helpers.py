from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

from daggerml._core.db import Ref
from daggerml._core.index import IndexOps
from daggerml._core.s3_cas import CasItem, CasItemConflict
from daggerml._core.types import DmlDB

DEFAULT_REMOTE_ROOT = "s3://bucket/root"


def make_db(path: Path) -> DmlDB:
    return DmlDB(str(path), 1024 * 1024, 64 * 1024 * 1024)


def make_local_dml(
    project_home: Path,
    monkeypatch,
    *,
    user: str = "tester",
    remote_root: str = DEFAULT_REMOTE_ROOT,
    remote_project: str | None = None,
):
    import daggerml._core.dml as dml_mod
    from daggerml._core.dml import Dml

    project_home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))
    init_kwargs = {"user": user, "remote_root": remote_root}
    if remote_project is not None:
        init_kwargs["remote_project"] = remote_project
    Dml.init(str(project_home), **init_kwargs)
    monkeypatch.setattr(dml_mod, "_index_ops", lambda dml: local_index_ops())
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
    def create_execution_record(self, record: dict[str, Any]) -> bool:
        return True

    def finish_execution(self, execution_id: str, dag: Ref, db: DmlDB) -> None:
        return None


class FakeRemote:
    n_workers = 1

    class _Store:
        client = None

    _store = _Store()

    def get_active(self, cache_key: str, raw: bool = False):
        return None


def local_index_ops() -> IndexOps:
    ops = object.__new__(IndexOps)
    ops.remote_root = "s3://bucket/root"
    ops._remote = FakeRemote()
    ops.exec_state = lambda cache_key=None: NoopExecutionState()  # type: ignore[method-assign]
    return ops


class MissingKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeCasStore:
    bucket = "bucket"
    client = None

    def __init__(self, prefix: str = "root/exec") -> None:
        self.prefix = prefix
        self.objects: dict[str, tuple[str, str]] = {}
        self.conflict_keys: set[str] = set()

    def _key_for(self, relative_key: str) -> str:
        return f"{self.prefix}/{relative_key}" if self.prefix else relative_key

    @staticmethod
    def _is_missing_error(exc: Exception) -> bool:
        return getattr(exc, "response", {}).get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}

    def _get(self, key: str, *, cas: bool = False):
        if key not in self.objects:
            raise MissingKey()
        data, etag = self.objects[key]
        return CasItem(key, data, etag) if cas else data

    def _put(self, key: str | CasItem, value: str | bytes, *, overwrite: bool = True, **kwargs) -> bool:
        expected_etag = None
        if isinstance(key, CasItem):
            expected_etag = key.etag
            key = key.key
        if key in self.conflict_keys:
            self.conflict_keys.remove(key)
            raise CasItemConflict(key)
        if not overwrite and key in self.objects:
            raise CasItemConflict(key)
        if expected_etag is not None and self.objects.get(key, (None, None))[1] != expected_etag:
            raise CasItemConflict(key)
        text = value.decode() if isinstance(value, bytes) else value
        current = int(self.objects.get(key, ("", "0"))[1])
        self.objects[key] = (text, str(current + 1))
        return True

    def _put_js(self, key: str | CasItem, value: Any, *, overwrite: bool = True, **kwargs) -> bool:
        return self._put(key, json.dumps(value, separators=(",", ":"), sort_keys=True), overwrite=overwrite)

    def _delete(self, key: str | CasItem, **kwargs) -> bool:
        expected_etag = None
        if isinstance(key, CasItem):
            expected_etag = key.etag
            key = key.key
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
        self.cache: dict[str, Ref] = {}

    def get_cache(self, cache_key: str, db=None):
        return self.cache.get(cache_key)

    def get_active(self, cache_key: str, raw: bool = False):
        return self.active.get(cache_key)

    def put_active(self, cache_key: str, execution_id: str, argv: Ref, db=None) -> None:
        self.active[cache_key] = {"meta": {"execution_id": execution_id}, "argv": argv.to}

    def delete_active(self, cache_key: str) -> None:
        self.active.pop(cache_key, None)

    def put_cache(self, dag: Ref, active_id: str, db=None) -> None:
        self.cache[active_id] = dag

    def get_transport(self, active_id: str, db=None):
        return None

    def delete_transport(self, active_id: str) -> None:
        return None
