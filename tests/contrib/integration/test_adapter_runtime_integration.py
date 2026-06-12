from __future__ import annotations

import daggerml.api as api
from daggerml import Dml
from daggerml.contrib.api import funkify
from daggerml.contrib.codecs import DelayedActionCodec


@funkify(adapter="local", uri="script")
def add_one(dag, value):
    return dag.put(value.value() + 1, name="result")


def test_contrib_int_005__decorated_local_funk_runs_through_full_pipeline(tmp_path, monkeypatch, remote_env, s3_bucket):
    del remote_env, s3_bucket
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))
    monkeypatch.setattr(
        api,
        "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )

    remote_root = "s3://test-bucket/test-prefix"
    remote_project = "dml://acme/contrib-local-runtime"

    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source_home.mkdir()
    target_home.mkdir()

    Dml.init(str(source_home), user="tester", remote_root=remote_root, remote_project=remote_project)
    source_dml = Dml(str(source_home), remote_root=remote_root, user="tester")

    dag = api.new("contrib-local-runtime", dml=source_dml)
    fn = dag.put(add_one, name="fn")
    result = fn(41, name="out", sleep=lambda: 0, timeout=10_000)
    dag.commit(result)
    source_dml.push()

    Dml.init(str(target_home), user="reviewer", remote_root=remote_root, remote_project=remote_project)
    target_dml = Dml(str(target_home), remote_root=remote_root, user="reviewer")
    target_dml.fetch(remote_project)
    fetched_dag = target_dml.show(revision=f"{remote_project}#main")["dags"]["contrib-local-runtime"]
    target_dml.dag.checkout(fetched_dag, name="contrib-local-runtime")

    loaded = api.load("contrib-local-runtime", dml=target_dml)
    assert loaded["out"].value() == 42
    assert loaded.result.value() == 42

    reused = api.new("contrib-local-runtime-reused", dml=target_dml)
    imported_fn = reused.require("contrib-local-runtime", "fn", name="fn")
    reused_result = imported_fn(99, name="out", sleep=lambda: 0, timeout=10_000)
    reused.commit(reused_result)

    reused_loaded = api.load("contrib-local-runtime-reused", dml=target_dml)
    assert reused_loaded["out"].value() == 100
    assert reused_loaded.result.value() == 100
