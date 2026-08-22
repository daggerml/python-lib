from __future__ import annotations

import pytest

from daggerml._core import BadExecutionStatusError, CanceledExecutionError, DmlRepoError
from daggerml._core.db import Ref
from daggerml._core.types import ArgvNode, Dag, Index, ListDatum
from tests._core.helpers import NoopExecutionState, execution_record, local_index_ops, make_db


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


def _put_reserved_execution(
    state: NoopExecutionState, execution_id: str, *, lifecycle: str, argv_ref: Ref | None = None
) -> None:
    state.create_execution_record(
        execution_record(
            execution_id,
            cache_key="ck1",
            lifecycle=lifecycle,
            argv_ref=None if argv_ref is None else argv_ref.to,
        )
    )


def test_put_literal_cancel_pending_execution_raises_without_drive(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    state.create_execution_record(
        execution_record(
            "idx",
            lifecycle="cancel-pending",
            cancelation={"requested_by": "user", "requested_at": 0},
        )
    )
    ops = local_index_ops(state)
    index = _put_index(db)

    with pytest.raises(CanceledExecutionError):
        ops.put_literal(index, 42, db=db)

    assert state.cancel_calls == []


def test_put_literal_canceled_execution_raises_without_drive(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    state.create_execution_record(
        execution_record(
            "idx",
            lifecycle="canceled",
            cancelation={"requested_by": "user", "requested_at": 0},
        )
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
        execution_record("idx", lifecycle=lifecycle)
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
    _put_reserved_execution(state, "exec", lifecycle="pending", argv_ref=argv_ref)
    ops._remote.materialized_ref = argv_ref

    index = ops.create("user", commit=db.init(), cache_key="ck1", execution_id="exec", db=db)

    assert index == Ref("index:exec")
    assert state.read_execution_record("exec")["state"]["lifecycle"] == "running"


@pytest.mark.parametrize("lifecycle", ["running", "succeeded", "failed"])
def test_execution_aware_create_rejects_non_pending_activation_states(tmp_path, lifecycle: str) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    ops = local_index_ops(state)
    _put_reserved_execution(state, "exec", lifecycle=lifecycle)
    ops._remote.get_active = lambda cache_key, raw=False: {"meta": {"execution_id": "exec"}}

    with pytest.raises(BadExecutionStatusError):
        ops.create("user", commit=db.init(), cache_key="ck1", execution_id="exec", db=db)


def test_execution_aware_create_rejects_missing_execution_record(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    ops = local_index_ops(state)
    ops._remote.get_active = lambda cache_key, raw=False: {"meta": {"execution_id": "exec"}}

    with pytest.raises(DmlRepoError):
        ops.create("user", commit=db.init(), cache_key="ck1", execution_id="exec", db=db)


def test_execution_aware_create_rejects_cancel_pending_without_drive(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    ops = local_index_ops(state)
    _put_reserved_execution(state, "exec", lifecycle="cancel-pending")
    ops._remote.get_active = lambda cache_key, raw=False: {"meta": {"execution_id": "exec"}}

    with pytest.raises(CanceledExecutionError):
        ops.create("user", commit=db.init(), cache_key="ck1", execution_id="exec", db=db)

    assert state.cancel_calls == []


def test_execution_aware_create_raises_on_canceled_without_drive(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    ops = local_index_ops(state)
    _put_reserved_execution(state, "exec", lifecycle="canceled")
    ops._remote.get_active = lambda cache_key, raw=False: {"meta": {"execution_id": "exec"}}

    with pytest.raises(CanceledExecutionError):
        ops.create("user", commit=db.init(), cache_key="ck1", execution_id="exec", db=db)

    assert state.cancel_calls == []


@pytest.mark.parametrize("failure", ["materialize", "write"])
def test_execution_aware_create_unlocks_activation_owner_after_setup_failure(
    tmp_path, monkeypatch, failure: str
) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    ops = local_index_ops(state)
    argv_ref = _put_argv_node(db)
    commit = db.init()
    _put_reserved_execution(state, "exec", lifecycle="pending", argv_ref=argv_ref)
    unlocks = []
    monkeypatch.setattr(state, "unlock", lambda execution_id, owner: unlocks.append((execution_id, owner)) or True)
    if failure == "materialize":
        monkeypatch.setattr(
            ops._remote, "materialize_ref", lambda *_: (_ for _ in ()).throw(RuntimeError("materialize failed"))
        )
    else:
        ops._remote.materialized_ref = argv_ref
        monkeypatch.setattr(
            db, "write_with_growth", lambda *_args, **_kw: (_ for _ in ()).throw(RuntimeError("write failed"))
        )

    with pytest.raises(RuntimeError, match=f"{failure} failed"):
        ops.create("user", commit=commit, cache_key="ck1", execution_id="exec", db=db)

    assert unlocks == [("exec", "owner")]
