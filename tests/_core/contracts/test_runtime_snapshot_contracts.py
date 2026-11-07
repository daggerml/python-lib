from __future__ import annotations

import json

import pytest

import daggerml.api as api
from daggerml._core import DmlRepoError
from daggerml._core.exec_state import ExecutionState
from daggerml._core.types import ArgvNode, FrozenIndex, Index, ListDatum, Ref
from tests._core.helpers import (
    FakeCasStore,
    FakeExecutionRemote,
    local_index_ops,
    make_local_dml,
    run_parallel,
)


@pytest.fixture
def snapshots(tmp_path, monkeypatch):
    import daggerml._core.dml as dml_mod

    monkeypatch.setattr(api, "_codecs", [(0, 1, api.NodeCodec()), (0, 2, api.ProjectionCodec()),
                                        (0, 3, api.MiscPyTypeCodec())])
    monkeypatch.setattr(api, "_plugins_loaded", True)
    state = object.__new__(ExecutionState)
    state.root_uri = "s3://bucket/root"
    state.n_workers = 1
    state.cache_key = None
    state._store = FakeCasStore()
    state._remote = FakeExecutionRemote()
    ops = local_index_ops(state)
    source = make_local_dml(tmp_path / "source", monkeypatch)
    target = make_local_dml(tmp_path / "target", monkeypatch)
    monkeypatch.setattr(dml_mod, "_index_ops", lambda dml: ops)
    monkeypatch.setattr(dml_mod, "_exec_state", lambda dml, cache_key=None: state)
    return source, target, state, ops


@pytest.mark.parametrize("function_runtime", [False, True])
def test_snapshot_restores_complete_graph_and_execution_identity(snapshots, function_runtime):
    source, target, state, _ = snapshots
    original = api.new("original", dml=source)
    original.commit(original.put({"numbers": [3, 5]}, name="data"))
    draft = api.new("draft", dml=source, tags=["checkpoint"])
    draft.put(api.load("original", dml=source).result["numbers"][1], name="checkpoint")
    if function_runtime:
        def add_argv(txn):
            idx = txn.get(draft.token)
            dag = txn.get(idx.dag)
            dag.argv = txn.put(ArgvNode(value=txn.put(ListDatum([]))))
            dag.nodes.append(dag.argv)
            idx.dag = txn.put(dag)
            txn.put(idx, to=draft.token)

        source._db.write_with_growth(add_argv)
    execution = draft.token
    draft.freeze("handoff")
    snapshot = draft.export_snapshot()
    assert snapshot == draft.export_snapshot()
    assert state.read_execution_record(execution.id())["state"]["lifecycle"] == "suspended"
    resumed = api.resume(snapshot=snapshot, dml=target, name="resumed", message="continue")
    assert resumed.token == execution
    assert resumed["checkpoint"].value() == 5
    assert resumed.tags == ["checkpoint"]
    if function_runtime:
        assert resumed.argv.value() == []
    resumed.commit(resumed.put(6, name="next"))
    assert api.load("resumed", dml=target).result.value() == 6
    assert state.read_execution_record(execution.id())["state"]["result_ref"] == resumed.ref.to
    with pytest.raises(DmlRepoError, match="stale|not suspended"):
        draft.unfreeze()


def test_snapshot_requires_exactly_one_source_and_frozen_export(snapshots):
    source, target, _, _ = snapshots
    dag = api.new(dml=source)
    with pytest.raises(DmlRepoError, match="frozen"):
        dag.export_snapshot()
    dag.freeze()
    with pytest.raises(DmlRepoError, match="exactly one"):
        target.runtime.unfreeze()
    with pytest.raises(DmlRepoError, match="exactly one"):
        target.runtime.unfreeze(dag.token, snapshot=dag.export_snapshot())


@pytest.mark.parametrize("damage", ["json", "schema", "remote", "missing", "identity", "extra"])
def test_invalid_snapshot_does_not_publish_local_runtime(snapshots, damage):
    source, target, state, _ = snapshots
    dag = api.new(dml=source)
    dag.put(42, name="answer")
    dag.freeze()
    data = json.loads(dag.export_snapshot())
    if damage == "schema":
        data["schema"] = True
    elif damage == "remote":
        data["remote_root"] = "s3://other/root"
    elif damage == "missing":
        del data["objects"][next(key for key in data["objects"] if key.startswith("datum-"))]
    elif damage == "identity":
        key = next(key for key in data["objects"] if key.startswith("datum-scalar:"))
        data["objects"][key] = data["objects"][key].replace("42", "43")
    elif damage == "extra":
        data["objects"]["index:unrelated"] = data["objects"][data["root"]]
    snapshot = "{" if damage == "json" else json.dumps(data)
    with pytest.raises(DmlRepoError, match="snapshot"):
        target.runtime.unfreeze(snapshot=snapshot)
    assert target.runtime.list() == []
    assert state.read_execution_record(dag.token.id())["state"]["lifecycle"] == "suspended"


def test_only_one_concurrent_snapshot_claim_succeeds(snapshots):
    source, _, state, _ = snapshots
    dag = api.new(dml=source).freeze()
    checkpoint = state.read_execution_record(dag.token.id())["state"]["runtime_generation"]

    def claim(i):
        try:
            state.resume_runtime(dag.token.id(), checkpoint, f"owner-{i}")
            return True
        except DmlRepoError:
            return False

    assert sum(run_parallel(5, claim)) == 1


def test_stale_runtime_cannot_mutate_or_publish_after_handoff(snapshots):
    source, target, state, _ = snapshots
    dag = api.new(dml=source)
    active = dag.token
    dag.freeze()
    resumed = api.resume(snapshot=dag.export_snapshot(), dml=target)
    # Model an old worker's separate local database retaining the pre-freeze index.
    def restore_stale(txn):
        frozen = txn.get(dag.token)
        txn.put(Index(parents=frozen.parents, tree=frozen.tree, author=frozen.author, message="",
                      dag=frozen.dag), to=active)

    source._db.write_with_growth(restore_stale)
    with pytest.raises(DmlRepoError, match="Stale runtime"):
        source.runtime.put_literal(active, "stale")
    with pytest.raises(DmlRepoError, match="Stale runtime"):
        state.finish_execution(active.id(), Ref("dag:old"), source._db)
    resumed.put("current", name="checkpoint")


@pytest.mark.parametrize("accepted", [False, True])
def test_freeze_and_activation_recover_after_remote_response_loss(snapshots, monkeypatch, accepted):
    source, _, state, _ = snapshots
    dag = api.new(dml=source)
    active = dag.token
    suspend = state.suspend_runtime

    def lose_suspension(*args):
        if accepted:
            suspend(*args)
        raise RuntimeError("response lost")

    monkeypatch.setattr(state, "suspend_runtime", lose_suspension)
    with pytest.raises(RuntimeError, match="response lost"):
        dag.freeze()
    monkeypatch.setattr(state, "suspend_runtime", suspend)
    dag.freeze()
    resume = state.resume_runtime

    def lose_activation(*args):
        if accepted:
            resume(*args)
        raise RuntimeError("response lost")

    monkeypatch.setattr(state, "resume_runtime", lose_activation)
    with pytest.raises(RuntimeError, match="response lost"):
        dag.unfreeze()
    monkeypatch.setattr(state, "resume_runtime", resume)
    dag.unfreeze()
    assert dag.token == active
    dag.put("recovered")


def test_legacy_local_frozen_runtime_can_be_resumed(snapshots):
    source, _, state, _ = snapshots
    active = source.runtime.create()
    source.runtime.put_literal(active, 42, name="answer")
    frozen = Ref(f"frozenindex:{active.id()}")

    def freeze_without_remote_generation(txn):
        idx = txn.get(active)
        txn.put(FrozenIndex(parents=idx.parents, tree=idx.tree, author=idx.author,
                            message=idx.message, dag=idx.dag), to=frozen)
        txn.delete(active)

    source._db.write_with_growth(freeze_without_remote_generation)
    resumed = api.resume(frozen, dml=source)
    assert resumed["answer"].value() == 42
    assert state.read_execution_record(active.id())["state"]["runtime_generation"]


def test_old_snapshot_cannot_claim_a_newer_freeze(snapshots):
    source, target, _, _ = snapshots
    dag = api.new(dml=source).freeze()
    old = dag.export_snapshot()
    dag.unfreeze().put(2, name="next")
    dag.freeze()
    with pytest.raises(DmlRepoError, match="stale|not suspended"):
        api.resume(snapshot=old, dml=target)
    resumed = api.resume(snapshot=dag.export_snapshot(), dml=source)
    assert resumed["next"].value() == 2


def test_cancellation_while_suspended_prevents_restore(snapshots):
    source, target, state, _ = snapshots
    dag = api.new(dml=source).freeze()
    snapshot = dag.export_snapshot()
    source.runtime.cancel(dag.token)
    assert state.read_execution_record(dag.token.id())["state"]["lifecycle"] == "canceled"
    with pytest.raises(DmlRepoError, match="stale|not suspended"):
        target.runtime.unfreeze(snapshot=snapshot)


@pytest.mark.parametrize("staging", ["none", "active-only", "interrupted-claim"])
def test_snapshot_round_trip_replaces_superseded_local_runtime(snapshots, monkeypatch, staging):
    source, target, state, _ = snapshots
    draft = api.new(dml=source)
    draft.put(1, name="checkpoint")
    draft.freeze()
    first = draft.export_snapshot()
    if staging != "none":
        resume = state.resume_runtime

        def interrupted(*args):
            raise RuntimeError("activation interrupted")

        monkeypatch.setattr(state, "resume_runtime", interrupted)
        with pytest.raises(RuntimeError, match="interrupted"):
            draft.unfreeze()
        monkeypatch.setattr(state, "resume_runtime", resume)
        if staging == "active-only":
            source._db.write_with_growth(lambda txn: txn.delete(draft.token))
    other = api.resume(snapshot=first, dml=target)
    other.put(2, name="checkpoint")
    other.freeze()
    second = other.export_snapshot()
    resumed = api.resume(snapshot=second, dml=source)
    assert resumed["checkpoint"].value() == 2
    with source._db.tx(readonly=True) as txn:
        assert not txn.exists(other.token)
    resumed.put(3, name="checkpoint")
    resumed.freeze()
    final = api.resume(snapshot=resumed.export_snapshot(), dml=target)
    assert final["checkpoint"].value() == 3
    final.commit(final["checkpoint"])


@pytest.mark.parametrize("interrupted", [False, True])
def test_snapshot_import_does_not_overwrite_locally_owned_activation(snapshots, monkeypatch, interrupted):
    source, target, state, _ = snapshots
    draft = api.new(dml=source).freeze()
    first = draft.export_snapshot()
    other = api.resume(snapshot=first, dml=target)
    other.put(2, name="checkpoint")
    other.freeze()
    second = other.export_snapshot()
    if interrupted:
        resume = state.resume_runtime

        def lose_response(*args):
            resume(*args)
            raise RuntimeError("response lost")

        monkeypatch.setattr(state, "resume_runtime", lose_response)
        with pytest.raises(RuntimeError, match="response lost"):
            api.resume(snapshot=second, dml=source)
        monkeypatch.setattr(state, "resume_runtime", resume)
        draft = api.Dag(dml=source, token=Ref(f"index:{other.token.id()}"))
    else:
        draft = api.resume(snapshot=second, dml=source)
    draft.put(3, name="unexported-work")
    with source._db.tx(readonly=True) as txn:
        before = txn.get(draft.token)
    with pytest.raises(DmlRepoError, match="stale|not suspended|active"):
        api.resume(snapshot=first, dml=source)
    with source._db.tx(readonly=True) as txn:
        assert txn.get(draft.token) == before
    assert draft["unexported-work"].value() == 3
    assert state.read_execution_record(draft.token.id())["state"]["runtime_generation"] == before.runtime_generation
    if interrupted:
        recovered = api.resume(snapshot=second, dml=source)
        assert recovered["unexported-work"].value() == 3


def test_immutable_snapshot_conflict_rolls_back_frozen_root_replacement(snapshots):
    source, target, _, _ = snapshots
    draft = api.new(dml=source)
    draft.put(1, name="checkpoint")
    draft.freeze()
    first = draft.export_snapshot()
    other = api.resume(snapshot=first, dml=target)
    other.put(2, name="checkpoint")
    other.freeze()
    data = json.loads(other.export_snapshot())
    # Conflict with an immutable object already present in the original project.
    ref = next(ref for ref, payload in data["objects"].items()
               if ref.startswith("datum-scalar:") and '["scalar",1]' in payload)
    data["objects"][ref] = data["objects"][ref].replace('["scalar",1]', '["scalar",99]')
    with pytest.raises(DmlRepoError, match="conflicts with local state"):
        api.resume(snapshot=json.dumps(data), dml=source)
    assert draft.export_snapshot() == first
    assert source.runtime.list()[0]["state"] == "frozen"


def test_snapshot_import_rechecks_checkpoint_before_replacing_local_state(snapshots, monkeypatch):
    source, target, state, _ = snapshots
    draft = api.new(dml=source).freeze()
    first = draft.export_snapshot()
    other = api.resume(snapshot=first, dml=target)
    other.put(2, name="checkpoint")
    other.freeze()
    second = other.export_snapshot()
    read = state.read_execution_record
    reads = 0

    def claim_before_restore(execution_id):
        nonlocal reads
        reads += 1
        if reads == 2:
            checkpoint = read(execution_id)["state"]["runtime_generation"]
            state.resume_runtime(execution_id, checkpoint, "another-owner")
        return read(execution_id)

    monkeypatch.setattr(state, "read_execution_record", claim_before_restore)
    with pytest.raises(DmlRepoError, match="stale|not suspended"):
        api.resume(snapshot=second, dml=source)
    assert draft.export_snapshot() == first
