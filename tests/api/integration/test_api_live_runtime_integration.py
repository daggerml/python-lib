from __future__ import annotations

from contextvars import ContextVar

import pytest

import daggerml._core.dml as dml_mod
import daggerml.api as api
from daggerml import Dml
from daggerml._core import Ref
from tests._core.helpers import local_index_ops

pytestmark = pytest.mark.slow


@pytest.fixture(autouse=True)
def isolated_api_defaults(monkeypatch):
    monkeypatch.setattr(api, "_PROCESS_DEFAULT_DML", None)
    monkeypatch.setattr(
        api,
        "_SCOPED_DEFAULT_DML",
        ContextVar("daggerml_integration_scoped_default_dml", default=api._NO_DEFAULT_DML),
    )


@pytest.fixture
def live_dml(tmp_path, monkeypatch):
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))
    Dml.init(str(tmp_path), user="tester", remote_root="s3://bucket/root")
    ops = local_index_ops()
    monkeypatch.setattr(dml_mod, "_index_ops", lambda dml: ops)
    return Dml(str(tmp_path), remote_root="s3://bucket/root", user="tester")


def test_api_live_001__new_put_commit_and_load_values(live_dml):
    dag = api.new("values", message="store values", dml=live_dml)
    scalar = dag.put(42, name="answer")
    dag.put([1, scalar, 3], name="numbers")
    result = dag.put({"answer": scalar, "numbers": dag["numbers"]}, name="payload")
    dag.commit(result)

    loaded = api.load("values", dml=live_dml)
    assert loaded["answer"].value() == 42
    assert loaded["numbers"].value() == [1, 42, 3]
    assert loaded.result.value() == {"answer": 42, "numbers": [1, 42, 3]}


def test_api_live_002__named_result_lookup_differs_from_committed_result(live_dml):
    dag = api.new("result-semantics", dml=live_dml)
    dag.put("named result", name="result")
    final = dag.put("committed result")
    dag.commit(final)

    loaded = api.load("result-semantics", dml=live_dml)
    assert loaded["result"].value() == "named result"
    assert loaded.result.value() == "committed result"


def test_api_live_003__require_imports_committed_source_node(live_dml):
    source = api.new("source", dml=live_dml)
    source.put({"a": 1, "b": [2, 3]}, name="data")
    source.commit(source["data"])

    consumer = api.new("consumer", dml=live_dml)
    imported = consumer.require("source", "data", name="imported")
    consumer.commit(imported)

    loaded = api.load("consumer", dml=live_dml)
    assert loaded["imported"].value() == {"a": 1, "b": [2, 3]}
    assert loaded.result.value() == {"a": 1, "b": [2, 3]}


def test_api_live_004__collection_helpers_use_real_builtins(live_dml):
    dag = api.new("collections", dml=live_dml)
    values = dag.put(["a", "b", "c"], name="values")
    mapping = dag.put({"x": 1}, name="mapping")

    assert values[1].value() == "b"
    assert values[1:3].value() == ["b", "c"]
    assert values.append("d", name="more").value() == ["a", "b", "c", "d"]
    assert values.contains("b").value() is True
    assert "b" in values

    assert mapping.get("x").value() == 1
    assert mapping.get("missing", "fallback").value() == "fallback"
    assert mapping.assoc("y", 2, name="assoc").value() == {"x": 1, "y": 2}


def test_api_live_005__scoped_default_drives_top_level_helpers(live_dml):
    with api.use_default_dml(live_dml):
        dag = api.new("defaulted")
        answer = dag.put(42, name="answer")
        dag.commit(answer)
        loaded = api.load("defaulted")

    assert loaded["answer"].value() == 42


def test_api_live_006__context_manager_commits_error_capture(live_dml):
    dag = api.new("captured-error", dml=live_dml)

    with pytest.raises(RuntimeError, match="boom"):
        with dag:
            raise RuntimeError("boom")

    loaded = api.load("captured-error", dml=live_dml)
    error_ref = live_dml.dag.describe(loaded.ref)["error"]
    assert isinstance(error_ref, Ref)


def test_api_live_007__open_builtin_selection_context_skips_collection_builtins(live_dml):
    dag = api.new("open-selection-context", dml=live_dml)
    answer = dag.put(42, name="answer")
    payload = dag.put({"answer": answer}, name="payload")

    assert payload["answer"].context(root=False) is dag


def test_api_live_008__committed_projection_value_and_context_follow_imported_structure(live_dml):
    source = api.new("projection-source", dml=live_dml)
    answer = source.put(42, name="answer")
    payload = source.put({"answer": answer}, name="payload")
    source.commit(payload)

    consumer = api.new("projection-consumer", dml=live_dml)
    imported = consumer.require("projection-source", "payload", name="payload")
    consumer.commit(imported)

    loaded = api.load("projection-consumer", dml=live_dml)
    projection = loaded.result["answer"]

    assert isinstance(projection, api.Projection)
    assert projection.value() == 42
    assert projection.context(root=False).ref == api.load("projection-source", dml=live_dml).ref
    assert projection.context(root=True).ref == api.load("projection-source", dml=live_dml).ref


def test_api_live_009__committed_projection_supports_nested_traversal(live_dml):
    dag = api.new("projection-nested", dml=live_dml)
    payload = dag.put({"outer": [{"inner": 7}]}, name="payload")
    dag.commit(payload)

    loaded = api.load("projection-nested", dml=live_dml)

    assert loaded.result["outer"][0]["inner"].value() == 7


def test_api_live_010__frozen_dag_can_be_reconstructed_resumed_and_committed(live_dml):
    original = api.new("resumed", dml=live_dml)
    original.put({"status": "done"}, name="implementation")
    original.freeze("awaiting review")

    resumed = api.Dag(dml=original.dml, token=original.token, name=original.name)
    resumed.unfreeze()
    approval = resumed.put("approved", name="review")
    resumed.commit(approval)

    completed = api.Dag(dml=resumed.dml, ref=resumed.ref)
    assert completed.keys() == ["implementation", "review"]
    assert completed.implementation.value() == {"status": "done"}
    assert completed.review.value() == "approved"
    assert completed.result.value() == "approved"


def test_node_context_happy_path(live_dml):
    source = api.new("source", dml=live_dml)
    answer = source.put(42, name="answer")
    source.commit(answer)

    intermediate = api.new("intermediate", dml=live_dml)
    imported = intermediate.require("source")
    intermediate.commit(imported)

    final = api.new("final", dml=live_dml)
    imported_final = final.require("intermediate")
    final.commit(imported_final)

    loaded = api.load("final", dml=live_dml)
    assert loaded.result.context(root=False) == intermediate
    assert loaded.result.context(root=True) == source


@pytest.mark.parametrize("coll,key", [(dict, "foo"), (list, 0)])
def test_node_context_noisy_collection(live_dml, coll, key):
    source = api.new("source", dml=live_dml)
    answer = source.put(42, name="answer")
    source.commit({key: answer} if coll is dict else [answer, 2])

    intermediate = api.new("intermediate", dml=live_dml)
    imported = intermediate.require("source")
    intermediate.commit(imported)

    final = api.new("final", dml=live_dml)
    imported_final = final.require("intermediate")
    final.commit(imported_final)

    proj = api.load("final", dml=live_dml).result[key]
    assert proj.context(root=True) == source
    assert proj.context(root=False) == source


@pytest.mark.parametrize("coll,key", [(dict, "foo"), (list, 0)])
def test_node_context_noisy_collection_access(live_dml, coll, key):
    source = api.new("source", dml=live_dml)
    answer = source.put(42, name="answer")
    source.commit({key: answer} if coll is dict else [answer, 2])

    intermediate = api.new("intermediate", dml=live_dml)
    intermediate.commit(intermediate.require("source")[key])

    final = api.new("final", dml=live_dml)
    imported_final = final.require("intermediate")
    final.commit(imported_final)

    proj = api.load("final", dml=live_dml).result
    assert proj.context(root=False) == intermediate
    assert proj.context(root=True) == source
