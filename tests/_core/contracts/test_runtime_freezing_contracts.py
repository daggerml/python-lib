from __future__ import annotations

import pytest

from daggerml._core import DmlRepoError
from daggerml._core.db import Ref
from daggerml._core.types import ArgvNode, FrozenIndex, Index, ListDatum
from daggerml.api import Dag
from tests._core.helpers import NoopExecutionState, execution_record, local_index_ops, make_local_dml


def test_runtime_freeze_and_unfreeze_preserve_id_and_partial_dag(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    index = dml.runtime.create()
    implementation = dml.runtime.put_literal(index, {"status": "done"}, name="implementation")
    before = dml.runtime.describe(index)

    frozen = dml.runtime.freeze(index, message="Review implementation")
    described = dml.runtime.describe(frozen)

    assert frozen == Ref(f"frozenindex:{index.id()}")
    assert described["id"] == frozen
    assert described["state"] == "frozen"
    assert described["frozen_message"] == "Review implementation"
    assert described["dag"] == before["dag"]
    assert dml.dag.get_node_by_name(described["dag"], "implementation") == implementation
    frozen_dag = Dag(dml=dml, token=frozen)
    assert frozen_dag.implementation.value() == {"status": "done"}
    with pytest.raises(DmlRepoError, match="uncommitted"):
        _ = frozen_dag.result

    active = dml.runtime.unfreeze(frozen)

    assert active == index
    assert dml.runtime.describe(active)["state"] == "active"
    assert dml.runtime.describe(active)["dag"] == before["dag"]

    resumed_dag = Dag(dml=dml, token=active)
    approval = resumed_dag.put("approved", name="review")
    resumed_dag.commit(approval)

    completed_dag = Dag(dml=dml, ref=resumed_dag.ref)
    assert completed_dag.keys() == ["implementation", "review"]
    assert completed_dag.implementation.value() == {"status": "done"}
    assert completed_dag.review.value() == "approved"
    assert completed_dag.result.value() == "approved"


def test_runtime_list_includes_frozen_indexes(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    active = dml.runtime.create()
    frozen = dml.runtime.freeze(dml.runtime.create(), message="Awaiting review")

    listed = {item["id"]: item for item in dml.runtime.list()}

    assert listed[active]["state"] == "active"
    assert listed[active]["frozen_message"] is None
    assert listed[frozen]["state"] == "frozen"
    assert listed[frozen]["frozen_message"] == "Awaiting review"


def test_runtime_freeze_rejects_function_runtime_without_replacing_it(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    index = dml.runtime.create()
    with dml._db.tx(create_if_missing=True) as txn:
        idx = txn.get(index)
        dag = txn.get(idx.dag)
        dag.argv = txn.put(ArgvNode(value=txn.put(ListDatum([]))))
        txn.put(dag, to=idx.dag)

    with pytest.raises(DmlRepoError, match="execution-aware function runtime"):
        dml.runtime.freeze(index)

    with dml._db.tx(readonly=True) as txn:
        assert isinstance(txn.get(index), Index)
        assert not txn.exists(Ref(f"frozenindex:{index.id()}"))


def test_frozen_index_is_retained_by_local_gc(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    index = dml.runtime.create()
    dml.runtime.put_literal(index, "retained", name="implementation")
    frozen = dml.runtime.freeze(index)

    dml.gc()

    with dml._db.tx(readonly=True) as txn:
        assert isinstance(txn.get(frozen), FrozenIndex)
        assert txn.get(txn.get(frozen).dag).names


def test_frozen_runtime_uses_preserved_id_for_cancel_and_graph(tmp_path, monkeypatch) -> None:
    import daggerml._core.dml as dml_mod

    dml = make_local_dml(tmp_path, monkeypatch)
    frozen = dml.runtime.freeze(dml.runtime.create())
    state = NoopExecutionState()
    state.create_execution_record(
        execution_record(frozen.id())
    )
    monkeypatch.setattr(dml_mod, "_exec_state", lambda _dml, cache_key=None: state)
    monkeypatch.setattr(dml_mod, "_index_ops", lambda _dml: local_index_ops(state))

    graph = dml.runtime.describe_graph(frozen)
    dml.runtime.cancel(frozen)

    assert graph["roots"] == [frozen.id()]
    assert state.cancel_calls == [(frozen.id(), "tester", 3)]
