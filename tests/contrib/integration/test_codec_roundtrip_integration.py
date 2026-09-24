"""S3-backed optional codec values consumed by a separate script worker."""

import pytest

import daggerml.api as api
from daggerml import Uri
from daggerml.contrib.api import funkify
from daggerml.contrib.s3 import S3Store


@funkify(uri="script")
def sum_parquet(dag, source):
    import io

    import polars as pl

    from daggerml.contrib.s3 import S3Store

    return int(pl.read_parquet(io.BytesIO(S3Store().get(source.value())))["score"].sum())


def test_polars_plugin_externalizes_parquet_for_worker(runtime_world, monkeypatch):
    pl = pytest.importorskip("polars")
    from daggerml.contrib.status import status

    world = runtime_world
    monkeypatch.setenv("DML_REMOTE_ROOT", world.root)
    monkeypatch.setattr(api, "_codecs", [])
    monkeypatch.setattr(api, "_plugins_loaded", False)
    with api.new("dataset", dml=world.publisher) as dag:
        frame = dag.put(pl.DataFrame({"score": [2, 3, 5]}), name="frame")
        dag.commit(frame)
    assert any("PolarsDataFrameCodec" in str(item) for item in status()["codecs"])
    uri = api.load("dataset", dml=world.publisher).result.value()
    assert isinstance(uri, Uri)
    assert pl.read_parquet(__import__("io").BytesIO(S3Store().get(uri))).to_dict(as_series=False) == {
        "score": [2, 3, 5]
    }
    with api.new("worker-result", dml=world.publisher) as dag:
        source = dag.require("dataset")
        result = dag.put(sum_parquet)(source, sleep=lambda: 0, timeout=30_000)
        dag.commit(result)
    assert api.load("worker-result", dml=world.publisher).result.value() == 10
