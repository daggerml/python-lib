from __future__ import annotations

import copy
import io
import json

import pytest

import daggerml.api as api
from daggerml._core import Dml, DmlRepoError
from daggerml._core.cli import MethodCLI
from daggerml._core.types import ArgvNode, ListDatum, Ref
from tests._core.helpers import make_local_dml


@pytest.fixture
def projects(tmp_path, monkeypatch):
    monkeypatch.setattr(api, "_codecs", [(0, 1, api.NodeCodec()), (0, 2, api.ProjectionCodec()),
                                        (0, 3, api.MiscPyTypeCodec())])
    monkeypatch.setattr(api, "_plugins_loaded", True)
    source = make_local_dml(tmp_path / "source", monkeypatch)
    home = tmp_path / "target"
    home.mkdir()
    target = Dml.init(str(home), remote_root="", user="target")
    return source, target


@pytest.mark.parametrize(("frozen", "function_runtime"), [(False, False), (True, False), (False, True)])
def test_runtimepack_preserves_index_and_root_dag_without_remote_access(projects, monkeypatch, frozen,
                                                                      function_runtime):
    import daggerml._core.dml as dml_mod

    source, target = projects
    with api.new("source", dml=source) as committed:
        committed.commit(committed.put({"numbers": [3, 5]}, name="data"))
    draft = api.new(dml=source, tags=["checkpoint"])
    draft.put(5, name="checkpoint")
    if function_runtime:
        def add_argv(txn):
            idx = txn.get(draft.token)
            dag = txn.get(idx.dag)
            dag.argv = txn.put(ArgvNode(value=txn.put(ListDatum([]))))
            dag.nodes.append(dag.argv)
            idx.dag = txn.put(dag)
            txn.put(idx, to=draft.token)

        source._db.write_with_growth(add_argv)
    if frozen:
        draft.freeze()
    before = source.runtime.describe(draft.token)
    monkeypatch.setattr(dml_mod, "_index_ops", lambda *_: pytest.fail("dump/load must not coordinate execution"))
    monkeypatch.setattr(dml_mod, "_exec_state", lambda *_: pytest.fail("dump/load must not access execution state"))
    monkeypatch.setattr(dml_mod, "_require_s3_client", lambda *_: pytest.fail("dump/load must not access S3"))
    data = draft.dump()
    assert data == draft.dump()
    assert set(data) == {"schema", "id", "commitpack", "frozen", "dag_manifest", "dag_pack"}
    assert data["dag_pack"]["root"] == before["dag"].to
    assert data["dag_manifest"] == {"schema": 1, "root": before["dag"].to, "subdags": [], "imports": []}
    assert data["commitpack"]["commit"][0] == draft.token.to
    with source._db.tx(readonly=True) as txn:
        dag = txn.get(before["dag"])
        assert dag.result is None
        assert dag.error is None
    restored = api.Dag.load(json.loads(json.dumps(data)), dml=target)
    loaded = restored.token
    assert loaded == draft.token
    assert target.runtime.describe(loaded) == before
    assert target.runtime.dump(loaded) == data
    with target._db.tx(readonly=True) as txn:
        assert not txn.exists(committed.ref)
        assert all(not txn.exists(parent) for parent in before["parents"])
    assert restored["checkpoint"].value() == 5
    assert restored.tags == ["checkpoint"]
    if function_runtime:
        assert restored.argv.value() == []


def test_load_overwrites_existing_index_and_replaces_frozen_counterpart(projects):
    source, target = projects
    draft = api.new(dml=source)
    draft.put(1, name="checkpoint")
    old = draft.dump()
    draft.put(2, name="checkpoint")
    new = draft.dump()
    assert source.runtime.load(old) == draft.token
    assert draft["checkpoint"].value() == 1
    assert source.runtime.load(new) == draft.token
    assert draft["checkpoint"].value() == 2
    target.runtime.load(new)
    draft.freeze()
    frozen = draft.dump()
    assert target.runtime.load(frozen) == draft.token
    with target._db.tx(readonly=True) as txn:
        assert not txn.exists(Ref(f"index:{old['id']}"))
    assert target.runtime.load(new) == Ref(f"index:{new['id']}")
    with target._db.tx(readonly=True) as txn:
        assert not txn.exists(draft.token)


@pytest.mark.parametrize("damage", ["schema", "missing", "identity", "extra", "manifest", "commitpack"])
def test_invalid_dump_is_rejected_without_overwriting_existing_index(projects, damage):
    source, target = projects
    draft = api.new(dml=source)
    draft.put(42, name="checkpoint")
    good = draft.dump()
    target.runtime.load(good)
    bad = copy.deepcopy(good)
    objects = bad["dag_pack"]["objects"]
    datum = next(key for key in objects if key.startswith("datum-scalar:"))
    if damage == "schema":
        bad["schema"] = 2
    elif damage == "missing":
        del objects[datum]
    elif damage == "identity":
        objects[datum] = objects[datum].replace("42", "43")
    elif damage == "manifest":
        bad["dag_manifest"]["root"] = "dag:" + "0" * 64
    elif damage == "commitpack":
        bad["commitpack"]["dags"] = {"incorrect": bad["dag_pack"]["root"]}
    else:
        objects["index:unrelated"] = bad["commitpack"]["commit"][1]
    with pytest.raises(DmlRepoError, match="runtime dump"):
        target.runtime.load(bad)
    assert target.runtime.dump(draft.token) == good


def test_cli_dump_stdout_and_load_stdin_or_file(projects, monkeypatch, capsys, tmp_path):
    source, target = projects
    index = source.runtime.create()
    source.runtime.put_literal(index, 42, name="checkpoint")
    cli = MethodCLI(Dml, prog="dml")
    assert cli.run(["--project-home", source._config.project_home, "runtime", "dump", index.to]) == 0
    dumped = capsys.readouterr().out
    assert json.loads(dumped) == source.runtime.dump(index)
    monkeypatch.setattr("sys.stdin", io.StringIO(dumped))
    assert cli.run(["--project-home", target._config.project_home, "runtime", "load", "-"]) == 0
    assert capsys.readouterr().out.strip() == index.to
    assert api.Dag(dml=target, token=index)["checkpoint"].value() == 42
    path = tmp_path / "index.json"
    path.write_text(dumped)
    assert cli.run(["--project-home", target._config.project_home, "runtime", "load", str(path)]) == 0
    assert capsys.readouterr().out.strip() == index.to
