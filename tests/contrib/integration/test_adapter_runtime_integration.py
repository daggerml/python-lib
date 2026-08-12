from __future__ import annotations

import daggerml.api as api
from daggerml import Dml
from daggerml.contrib import api as contrib_api
from daggerml.contrib.api import dagclass, funkify
from daggerml.contrib.codecs import DelayedActionCodec


@funkify(adapter="local", uri="script")
def add_one(dag, value):
    return dag.put(value.value() + 1, name="result")


@dagclass
class ConfiguredCalculation:
    offset: int
    scale: int

    def main(self, value):
        return (self.offset.value() + value.value()) * self.scale.value()


def test_contrib_int_005__decorated_local_funk_runs_through_full_pipeline(tmp_path, monkeypatch, remote_env, s3_bucket):
    del remote_env, s3_bucket
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))
    monkeypatch.setattr(
        api,
        "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )

    remote_root = "s3://test-bucket/test-prefix"

    source_home = tmp_path / "source"
    target_home = tmp_path / "target"
    source_home.mkdir()
    target_home.mkdir()

    Dml.init(str(source_home), user="tester", remote_root=remote_root)
    source_dml = Dml(str(source_home), remote_root=remote_root, user="tester")

    dag = api.new("contrib-local-runtime", dml=source_dml)
    fn = dag.put(add_one, name="fn")
    result = fn(41, name="out", sleep=lambda: 0, timeout=10_000)
    dag.commit(result)
    source_dml.push()

    Dml.init(str(target_home), user="reviewer", remote_root=remote_root)
    target_dml = Dml(str(target_home), remote_root=remote_root, user="reviewer")
    target_dml.fetch()
    target_dml.dag.checkout("main", "contrib-local-runtime", remote=True)

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


def test_contrib_int_006__dagclass_method_uses_its_namespace_in_caller_dag(
    tmp_path, monkeypatch, remote_env, s3_bucket
):
    del remote_env, s3_bucket
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))
    monkeypatch.setattr(
        api,
        "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )
    runtime = Dml.init(tmp_path, user="tester", remote_root="s3://test-bucket/test-prefix")
    calculation = ConfiguredCalculation(offset=1, scale=2)

    colliding = api.new("dagclass-colliding-names", dml=runtime)
    colliding.put(4, name="offset")
    colliding.put(5, name="scale")
    fn = colliding.put(calculation.main, name="impl")
    colliding_result = fn(3, name="result", sleep=lambda: 0, timeout=10_000)
    colliding.commit(colliding_result)

    isolated = api.new("dagclass-no-caller-names", dml=runtime)
    fn = isolated.put(calculation.main, name="impl")
    isolated_result = fn(3, name="result", sleep=lambda: 0, timeout=10_000)
    isolated.commit(isolated_result)

    assert colliding_result.value() == 8
    assert isolated_result.value() == 8


def test_contrib_int_007__run_executes_compiled_dagclass_entrypoint(
    tmp_path, monkeypatch, remote_env, s3_bucket
):
    del remote_env, s3_bucket
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))
    monkeypatch.setattr(
        api,
        "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )
    runtime = Dml.init(tmp_path, user="tester", remote_root="s3://test-bucket/test-prefix")

    with api.use_default_dml(runtime):
        contrib_api.run(ConfiguredCalculation(offset=1, scale=2), 3, name="dagclass-api-run")

    assert api.load("dagclass-api-run", dml=runtime).result.value() == 8
