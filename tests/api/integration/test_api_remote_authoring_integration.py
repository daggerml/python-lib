"""Public authoring, persistence, worker result and cancellation wrappers."""

import pytest

import daggerml.api as api
from daggerml import CancellationError, Dml
from daggerml.contrib.api import funkify
from daggerml.contrib.codecs import DelayedActionCodec


@funkify(uri="script")
def double(dag, number):
    return dag.put(2 * number.value(), name="doubled")


def _codecs(monkeypatch):
    monkeypatch.setattr(
        api,
        "_codecs",
        [
            (1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()),
            (0, 3, api.MiscPyTypeCodec()), (0, 4, api.ProjectionCodec()),
        ],
    )


def test_frozen_imported_projection_and_remote_execution_survive_reopen(runtime_world, monkeypatch):
    _codecs(monkeypatch)
    world = runtime_world
    author = world.publisher
    with api.new("source", dml=author) as dag:
        dag.commit(dag.put({"numbers": [3, 5]}, name="data"))

    draft = api.new("derived", dml=author)
    projection = api.load("source", dml=author).result["numbers"][1]
    draft.put(projection, name="selection")
    draft.freeze("review")
    reopened = Dml(str(world.home / "publisher"), remote_root=world.root, user="publisher")
    resumed = api.resume(draft.token, name="derived", message="resume reviewed work", dml=reopened)
    assert resumed["selection"].value() == 5
    computed = resumed.put(double, name="fn")(resumed["selection"], name="computed", timeout=30_000, sleep=lambda: 0)
    resumed.commit(computed)
    reopened.push()

    observer = world.clone("observer")
    result = api.load("derived", dml=observer)
    assert result["selection"].context(root=True).ref == api.load("source", dml=observer).ref
    assert result.result.value() == 10
    assert result["computed"].context(root=False).tags == []


def test_persisted_error_is_visible_after_remote_clone(runtime_world):
    world = runtime_world
    dag = api.new("error", dml=world.publisher)
    dag.commit(api.Error("failed on purpose", origin="integration", type="ValueError"))
    world.publisher.push()
    observer = world.clone("error-observer")
    with pytest.raises(api.Error, match="failed on purpose") as exc:
        _ = api.load("error", dml=observer).result
    assert exc.value.origin == "integration"


def test_dag_cancel_delegates_to_real_execution_state(runtime_world):
    dag = api.new("canceled", dml=runtime_world.publisher)
    token = dag.token
    with pytest.raises(CancellationError, match="cancelled"):
        dag.cancel()
    assert dag.token is None
    assert runtime_world.publisher.runtime.read_execution_record(token)["state"]["lifecycle"] == "canceled"
