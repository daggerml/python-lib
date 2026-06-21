from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings

from daggerml._core.db import DmlDb as RawDmlDB
from daggerml._core.db import DmlDbRegistryFullError, Ref
from daggerml._core.types import (
    NAMESPACES,
    Commit,
    Dag,
    DictDatum,
    Error,
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


def test_call_with_resize_wraps_typed_transaction(tmp_path) -> None:
    db = make_db(tmp_path)
    commit = db.init()
    dag = Ref("dag:current")

    with db.tx(readonly=True) as txn:
        base_commit = txn.get(commit)

    def write_index(txn):
        return txn.put(Index(parents=[commit], tree=base_commit.tree, author="user", message="", dag=dag))

    ref = db.call_with_resize(write_index)

    with db.tx(readonly=True) as txn:
        obj = txn.get(ref)

    assert isinstance(obj, Index)
    assert obj.parents == [commit]
    assert obj.tree == base_commit.tree
    assert obj.dag == dag


def test_get_ctx_loads_dag_from_index_but_not_from_commit(tmp_path) -> None:
    db = make_db(tmp_path)
    with db.tx(create_if_missing=True) as txn:
        dag_ref = txn.put(Dag(nodes=[], names={}))
        committed_tree = txn.put(Tree(dags={}))
        staged_tree = txn.put(Tree(dags={"staged": dag_ref}))
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
    Index(
        parents=[commit_ref],
        tree=base_commit.tree,
        author="user",
        message="",
        dag=Ref("dag:ok"),
        lifecycle="inactive",
    )._validate()

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

    with pytest.raises(TypeError, match=r"Index\.lifecycle"):
        Index(
            parents=[commit_ref],
            tree=base_commit.tree,
            author="user",
            message="",
            dag=Ref("dag:ok"),
            lifecycle="paused",
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
