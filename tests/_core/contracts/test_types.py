from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings

from daggerml._core.db import DmlDb as RawDmlDB
from daggerml._core.db import (
    DmlDbBusyError,
    DmlDbInvalidPathError,
    DmlDbInvalidTypeError,
    DmlDbMapFullError,
    DmlDbReadonlyTxnError,
    DmlDbRegistryFullError,
    Ref,
)
from daggerml._core.types import (
    NAMESPACES,
    Commit,
    Dag,
    DictDatum,
    Error,
    FrozenIndex,
    Index,
    ListDatum,
    LiteralNode,
    Runnable,
    RunnableDatum,
    ScalarDatum,
    Tree,
    Uri,
    UriDatum,
    require_ref,
)
from tests._core.helpers import make_db
from tests._core.strategies import runnables


def _put_datum(txn, value) -> Ref:
    if isinstance(value, Uri):
        return txn.put(UriDatum(value.uri))
    if isinstance(value, Runnable):
        return txn.put(
            RunnableDatum(
                target=_put_datum(txn, value.target),
                sub=_put_datum(txn, value.sub) if value.sub is not None else None,
                kwargs=_put_datum(txn, value.kwargs),
                adapter=value.adapter,
            )
        )
    if isinstance(value, list):
        return txn.put(ListDatum([_put_datum(txn, item) for item in value]))
    if isinstance(value, dict):
        return txn.put(DictDatum({key: _put_datum(txn, item) for key, item in value.items()}))
    return txn.put(ScalarDatum(value))


def test_namespaces_are_registered_for_persisted_shapes() -> None:
    assert NAMESPACES["datum-scalar"].__name__ == "ScalarDatum"
    assert NAMESPACES["node-literal"].__name__ == "LiteralNode"
    assert NAMESPACES["dag"] is Dag
    assert NAMESPACES["error"] is Error
    assert NAMESPACES["frozenindex"] is FrozenIndex


def test_require_ref_enforces_namespace_hierarchy() -> None:
    require_ref(Ref("node-literal:x"), ["node"])

    with pytest.raises(TypeError, match="expected namespace hierarchy"):
        require_ref(Ref("datum-scalar:x"), ["node"])
    with pytest.raises(TypeError, match="expected Ref"):
        require_ref("node-literal:x", ["node"])


def test_object_validation_rejects_invalid_graph_shapes() -> None:
    with pytest.raises(TypeError, match="cannot have both result and error"):
        Dag(nodes=[], names={}, result=Ref("node-literal:ok"), error=Ref("error:bad"))._validate()
    with pytest.raises(TypeError, match="keys must be strings"):
        DictDatum({1: Ref("datum-scalar:x")})._validate()  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="expected namespace hierarchy"):
        LiteralNode(Ref("commit:x"))._validate()


def test_dag_result_error_invariants() -> None:
    node = Ref("node-literal:x")
    dag = Dag(nodes=[node], names={"answer": node}, result=node)

    assert dag.nameof(node) == "answer"
    assert dag.is_finished()
    assert dag.is_finished(success=True)
    assert not dag.is_finished(success=False)


def test_error_from_exception_captures_type_message_and_stack() -> None:
    try:
        raise RuntimeError("boom")
    except RuntimeError as exc:
        error = Error.from_ex(exc)

    assert error.message == "boom"
    assert error.origin == "python"
    assert error.type == "runtimeerror"
    assert error.stack


def test_txn_iter_returns_typed_objects(tmp_path) -> None:
    db = make_db(tmp_path)
    with db.tx(create_if_missing=True) as txn:
        ref = txn.put(ScalarDatum("value"))

    with db.tx(readonly=True) as txn:
        items = list(txn.iter("datum-scalar"))

    assert items == [(ref, ScalarDatum("value"))]


def test_raw_db_write_with_growth_retries_until_commit(tmp_path) -> None:
    db = RawDmlDB(str(tmp_path), namespaces=sorted(NAMESPACES), map_size_headroom=64 * 1024, max_map_size=2 * 1024**2)
    with db.tx(map_size=64 * 1024, create_if_missing=True):
        pass
    attempts = 0

    def write(txn):
        nonlocal attempts
        attempts += 1
        return txn.put("x" * (200 * 1024), ns="datum-scalar")

    ref = db.write_with_growth(write)

    assert attempts > 1
    with db.tx(readonly=True) as txn:
        assert txn.get(ref) == "x" * (200 * 1024)


def test_raw_db_current_validation_and_transaction_errors_remain(tmp_path) -> None:
    db = RawDmlDB(str(tmp_path), namespaces=sorted(NAMESPACES), map_size_headroom=64 * 1024, max_map_size=2 * 1024**2)
    with db.tx(create_if_missing=True) as txn:
        with pytest.raises(DmlDbInvalidTypeError):
            txn.put(object(), ns="datum-scalar")
    with db.tx(readonly=True) as txn:
        with pytest.raises(DmlDbReadonlyTxnError):
            txn.put("value", ns="datum-scalar")


def test_raw_db_write_with_growth_retries_map_full_from_commit(tmp_path) -> None:
    class CommitMapFullDb(RawDmlDB):
        fail_next_commit = True

        @contextmanager
        def tx(self, *args, **kwargs):
            with super().tx(*args, **kwargs) as txn:
                yield txn
                if not kwargs.get("readonly", True) and self.fail_next_commit:
                    self.fail_next_commit = False
                    raise DmlDbMapFullError("database map is full: dml_db_txn_close")

    db = CommitMapFullDb(
        str(tmp_path), namespaces=sorted(NAMESPACES), map_size_headroom=1024 * 1024, max_map_size=2 * 1024**2
    )
    with db.tx(create_if_missing=True):
        pass
    attempts = 0
    retry_values = [f"retry-{i}" for i in range(10)]

    def write(txn):
        nonlocal attempts
        attempts += 1
        return [txn.put(value, ns="datum-scalar") for value in retry_values]

    refs = db.write_with_growth(write)

    assert attempts == 2
    with db.tx(readonly=True) as txn:
        assert {value for _, value in txn.iter("datum-scalar")} == set(retry_values)
        assert [txn.get(ref) for ref in refs] == retry_values


def test_raw_db_write_with_growth_reports_capacity_limit(tmp_path) -> None:
    db = RawDmlDB(str(tmp_path), namespaces=sorted(NAMESPACES), map_size_headroom=64 * 1024, max_map_size=64 * 1024)

    with pytest.raises(DmlDbMapFullError, match=r"configured maximum is 65536 bytes"):
        db.write_with_growth(lambda txn: txn.put("x" * (200 * 1024), ns="datum-scalar"), create_if_missing=True)


def test_raw_db_explicit_resize_waits_for_active_lease(tmp_path) -> None:
    db = RawDmlDB(
        str(tmp_path), namespaces=sorted(NAMESPACES), map_size_headroom=1024 * 1024, max_map_size=64 * 1024**2
    )
    with db.tx(create_if_missing=True):
        pass

    started = threading.Event()
    completed = threading.Event()

    def resize() -> None:
        started.set()
        db.resize()
        completed.set()

    with db.tx(readonly=True):
        worker = threading.Thread(target=resize)
        worker.start()
        assert started.wait(timeout=1)
        time.sleep(0.05)
        assert not completed.is_set()

    worker.join(timeout=1)
    assert completed.is_set()
    with db.tx(readonly=True):
        pass


def test_raw_db_explicit_resize_rejects_calling_transaction_owner(tmp_path) -> None:
    db = RawDmlDB(
        str(tmp_path), namespaces=sorted(NAMESPACES), map_size_headroom=1024 * 1024, max_map_size=64 * 1024**2
    )
    with db.tx(create_if_missing=True):
        pass

    with db.tx(readonly=True):
        with pytest.raises(DmlDbBusyError):
            db.resize()


def test_raw_db_open_recovers_after_failed_resize(tmp_path) -> None:
    path = tmp_path / "db"
    db = RawDmlDB(str(path), namespaces=sorted(NAMESPACES), map_size_headroom=1024 * 1024, max_map_size=8 * 1024**2)

    with pytest.raises(DmlDbInvalidPathError):
        db.resize()

    path.mkdir()
    with db.tx(create_if_missing=True):
        pass


def test_raw_db_open_with_map_size_does_not_resize_active_environment(tmp_path) -> None:
    db = RawDmlDB(str(tmp_path), namespaces=sorted(NAMESPACES), map_size_headroom=1024 * 1024, max_map_size=8 * 1024**2)
    with db.tx(create_if_missing=True):
        pass

    completed = threading.Event()

    def open_with_map_size() -> None:
        with db.tx(readonly=True, map_size=2 * 1024**2):
            completed.set()

    with db.tx(readonly=True):
        worker = threading.Thread(target=open_with_map_size)
        worker.start()
        worker.join(timeout=1)
        assert completed.is_set()


def test_get_ctx_loads_dag_from_index_but_not_from_commit(tmp_path) -> None:
    db = make_db(tmp_path)
    with db.tx(create_if_missing=True) as txn:
        dag_ref = txn.put(Dag(nodes=[], names={}))
        committed_tree = txn.put(Tree(dags={}, tags={}))
        staged_tree = txn.put(Tree(dags={"staged": dag_ref}, tags={}))
        commit_ref = txn.put(Commit(parents=[], tree=committed_tree, author="a", message="m"))
        index_ref = txn.put(Index(parents=[commit_ref], tree=staged_tree, author="b", message="", dag=dag_ref))

    with db.tx(readonly=True) as txn:
        commit_ctx = txn.get_ctx(commit_ref)
        index_ctx = txn.get_ctx(index_ref)

    assert commit_ctx.dag is None
    assert index_ctx.dag is not None
    assert index_ctx.tree.dags == {"staged": dag_ref}


def test_index_validation_requires_commit_shape_and_dag(tmp_path) -> None:
    db = make_db(tmp_path)
    commit_ref = db.init()
    with db.tx(readonly=True) as txn:
        base_commit = txn.get(commit_ref)

    Index(parents=[commit_ref], tree=base_commit.tree, author="user", message="", dag=Ref("dag:ok"))._validate()

    with pytest.raises(TypeError, match=r"Index\.tree"):
        Index(
            parents=[commit_ref],
            tree=Ref("commit:not-a-tree"),
            author="user",
            message="",
            dag=Ref("dag:ok"),
        )._validate()

    with pytest.raises(TypeError, match=r"Index\.dag"):
        Index(
            parents=[commit_ref],
            tree=base_commit.tree,
            author="user",
            message="",
            dag=Ref("commit:not-a-dag"),
        )._validate()

    FrozenIndex(
        parents=[commit_ref],
        tree=base_commit.tree,
        author="user",
        message="",
        dag=Ref("dag:ok"),
        frozen_message="Review output",
    )._validate()


def test_raw_db_tx_accepts_larger_map_size_on_reopen(tmp_path) -> None:
    db = RawDmlDB(str(tmp_path), namespaces=sorted(NAMESPACES), map_size_headroom=1024 * 1024, max_map_size=1024 * 1024)
    with db.tx(create_if_missing=True):
        pass

    with db.tx(readonly=True, map_size=2 * 1024 * 1024):
        pass


def test_raw_db_registry_reuses_same_path_slots_and_enforces_active_capacity(tmp_path) -> None:
    paths = [tmp_path / f"db-{i}" for i in range(10)]
    for path in paths:
        path.mkdir()

    dbs = [
        RawDmlDB(str(path), namespaces=sorted(NAMESPACES), map_size_headroom=1024 * 1024, max_map_size=1024 * 1024)
        for path in paths
    ]
    for db in dbs:
        with db.tx(create_if_missing=True):
            pass

    txns = [db.tx(readonly=True) for db in dbs]
    for txn in txns:
        txn.__enter__()

    overflow_path = tmp_path / "db-overflow"
    overflow_path.mkdir()
    overflow = RawDmlDB(
        str(overflow_path),
        namespaces=sorted(NAMESPACES),
        map_size_headroom=1024 * 1024,
        max_map_size=1024 * 1024,
    )
    with pytest.raises(DmlDbRegistryFullError):
        with overflow.tx(create_if_missing=True):
            pass

    txns[1].__exit__(None, None, None)

    replacement_path = tmp_path / "db-replacement"
    replacement_path.mkdir()
    replacement = RawDmlDB(
        str(replacement_path),
        namespaces=sorted(NAMESPACES),
        map_size_headroom=1024 * 1024,
        max_map_size=1024 * 1024,
    )
    with replacement.tx(create_if_missing=True):
        pass

    for i, txn in enumerate(txns):
        if i != 1:
            txn.__exit__(None, None, None)


@given(runnables)
@settings(max_examples=50, deadline=None)
def test_runnable_datum_unroll_materializes_nested_runnable_fields(value: Runnable) -> None:
    with TemporaryDirectory() as tmpdir:
        db = make_db(Path(tmpdir))

        with db.tx(create_if_missing=True) as txn:
            ref = _put_datum(txn, value)
            unrolled = txn.get(ref).unroll(txn)

    assert unrolled == value
