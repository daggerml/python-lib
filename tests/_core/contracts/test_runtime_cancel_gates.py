from __future__ import annotations

import pytest

from daggerml._core import BadExecutionStatusError, CanceledExecutionError, DmlRepoError
from daggerml._core.db import Ref
from daggerml._core.types import ArgvNode, Dag, Index, ListDatum
from tests._core.helpers import NoopExecutionState, local_index_ops, make_db


def _put_index(db) -> Ref:
    commit_ref = db.init()
    with db.tx(create_if_missing=True) as txn:
        base_commit = txn.get(commit_ref)
        dag_ref = txn.put(Dag(nodes=[], names={}))
        return txn.put(
            Index(
                parents=[commit_ref],
                tree=base_commit.tree,
                author="user",
                message="",
                dag=dag_ref,
            ),
            to=Ref("index:idx"),
        )


def _put_argv_node(db) -> Ref:
    with db.tx(create_if_missing=True) as txn:
        return txn.put(ArgvNode(value=txn.put(ListDatum([]))))


def _put_reserved_execution(state: NoopExecutionState, execution_id: str, *, lifecycle: str) -> None:
    state.create_execution_record(
        {
            "execution_id": execution_id,
            "cache_key": "ck1",
            "lifecycle": lifecycle,
            "updated_at": 0,
            "created_at": 0,
            "spawned_execution_ids": [],
            "child_execution_ids": [],
            "cancellation_requested_by": None,
        }
    )


def test_put_literal_cancel_requested_execution_drives_cancel_then_raises(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    state.create_execution_record(
        {
            "execution_id": "idx",
            "cache_key": None,
            "lifecycle": "cancel-requested",
            "updated_at": 0,
            "created_at": 0,
            "spawned_execution_ids": [],
            "child_execution_ids": [],
            "cancellation_requested_by": "user",
        }
    )
    ops = local_index_ops(state)
    index = _put_index(db)

    with pytest.raises(CanceledExecutionError):
        ops.put_literal(index, 42, db=db)

    assert state.cancel_calls == [("idx", None, "drive")]


def test_put_literal_canceled_execution_raises_without_drive(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    state.create_execution_record(
        {
            "execution_id": "idx",
            "cache_key": None,
            "lifecycle": "canceled",
            "updated_at": 0,
            "created_at": 0,
            "spawned_execution_ids": [],
            "child_execution_ids": [],
            "cancellation_requested_by": "user",
        }
    )
    ops = local_index_ops(state)
    index = _put_index(db)

    with pytest.raises(CanceledExecutionError):
        ops.put_literal(index, 42, db=db)

    assert state.cancel_calls == []


@pytest.mark.parametrize("lifecycle", ["pending", "succeeded", "failed"])
def test_put_literal_rejects_non_running_execution_states(tmp_path, lifecycle: str) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    state.create_execution_record(
        {
            "execution_id": "idx",
            "cache_key": None,
            "lifecycle": lifecycle,
            "updated_at": 0,
            "created_at": 0,
            "spawned_execution_ids": [],
            "child_execution_ids": [],
            "cancellation_requested_by": None,
        }
    )
    ops = local_index_ops(state)
    index = _put_index(db)

    with pytest.raises(BadExecutionStatusError):
        ops.put_literal(index, 42, db=db)


def test_execution_aware_create_activates_pending_execution(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    ops = local_index_ops(state)
    argv_ref = _put_argv_node(db)
    _put_reserved_execution(state, "exec", lifecycle="pending")
    ops._remote.get_active = lambda cache_key, raw=False: {"meta": {"execution_id": "exec"}}
    ops._remote._materialize_manifest = lambda manifest, txn, expected_root_ns: argv_ref

    index = ops.create("user", commit=db.init(), cache_key="ck1", execution_id="exec", db=db)

    assert index == Ref("index:exec")
    assert state.read_execution_record("exec")["lifecycle"] == "running"


@pytest.mark.parametrize("lifecycle", ["running", "succeeded", "failed"])
def test_execution_aware_create_rejects_non_pending_activation_states(tmp_path, lifecycle: str) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    ops = local_index_ops(state)
    argv_ref = _put_argv_node(db)
    _put_reserved_execution(state, "exec", lifecycle=lifecycle)
    ops._remote.get_active = lambda cache_key, raw=False: {"meta": {"execution_id": "exec"}}
    ops._remote._materialize_manifest = lambda manifest, txn, expected_root_ns: argv_ref

    with pytest.raises(BadExecutionStatusError):
        ops.create("user", commit=db.init(), cache_key="ck1", execution_id="exec", db=db)


def test_execution_aware_create_rejects_missing_execution_record(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    ops = local_index_ops(state)
    argv_ref = _put_argv_node(db)
    ops._remote.get_active = lambda cache_key, raw=False: {"meta": {"execution_id": "exec"}}
    ops._remote._materialize_manifest = lambda manifest, txn, expected_root_ns: argv_ref

    with pytest.raises(DmlRepoError):
        ops.create("user", commit=db.init(), cache_key="ck1", execution_id="exec", db=db)


def test_execution_aware_create_drives_cancel_requested_before_raising(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    ops = local_index_ops(state)
    argv_ref = _put_argv_node(db)
    _put_reserved_execution(state, "exec", lifecycle="cancel-requested")
    ops._remote.get_active = lambda cache_key, raw=False: {"meta": {"execution_id": "exec"}}
    ops._remote._materialize_manifest = lambda manifest, txn, expected_root_ns: argv_ref

    with pytest.raises(CanceledExecutionError):
        ops.create("user", commit=db.init(), cache_key="ck1", execution_id="exec", db=db)

    assert state.cancel_calls == [("exec", None, "drive")]


@pytest.mark.parametrize("lifecycle", ["cancel-ready", "canceled"])
def test_execution_aware_create_raises_on_terminal_cancel_without_drive(tmp_path, lifecycle: str) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    ops = local_index_ops(state)
    argv_ref = _put_argv_node(db)
    _put_reserved_execution(state, "exec", lifecycle=lifecycle)
    ops._remote.get_active = lambda cache_key, raw=False: {"meta": {"execution_id": "exec"}}
    ops._remote._materialize_manifest = lambda manifest, txn, expected_root_ns: argv_ref

    with pytest.raises(CanceledExecutionError):
        ops.create("user", commit=db.init(), cache_key="ck1", execution_id="exec", db=db)

    assert state.cancel_calls == []
