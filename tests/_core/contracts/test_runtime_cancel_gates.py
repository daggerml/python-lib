from __future__ import annotations

import pytest

from daggerml._core import CancellationError
from daggerml._core.db import Ref
from daggerml._core.types import Dag, Index
from tests._core.helpers import NoopExecutionState, local_index_ops, make_db


def _put_index(db, *, lifecycle: str) -> Ref:
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
                lifecycle=lifecycle,
            ),
            to=Ref("index:idx"),
        )


def test_put_literal_inactive_index_drives_cancel_then_raises(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    state.create_execution_record(
        {
            "execution_id": "idx",
            "cache_key": None,
            "lifecycle": "cancel-pending",
            "updated_at": 0,
            "created_at": 0,
            "spawned_execution_ids": [],
            "child_execution_ids": [],
            "cancellation_requested_by": "user",
        }
    )
    ops = local_index_ops(state)
    index = _put_index(db, lifecycle="inactive")

    with pytest.raises(CancellationError):
        ops.put_literal(index, 42, db=db)

    assert state.cancel_calls == [("idx", None, "drive")]


def test_put_literal_canceled_index_raises_without_drive(tmp_path) -> None:
    db = make_db(tmp_path)
    state = NoopExecutionState()
    ops = local_index_ops(state)
    index = _put_index(db, lifecycle="canceled")

    with pytest.raises(CancellationError):
        ops.put_literal(index, 42, db=db)

    assert state.cancel_calls == []
