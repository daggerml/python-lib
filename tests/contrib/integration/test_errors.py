from __future__ import annotations

import pytest

import daggerml.api as api
from daggerml import Dml
from daggerml.contrib.api import funkify
from daggerml.contrib.codecs import DelayedActionCodec


@funkify
def one_over(dag, value):
    dag.numerator = 1
    return dag.put(1 / value.value(), name="val")


def test_raises(tmp_path, monkeypatch, remote_env, s3_bucket):
    del remote_env, s3_bucket
    remote_root = "s3://test-bucket/test-prefix"
    monkeypatch.setattr(
        api,
        "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )

    source_home = tmp_path / "source"
    source_home.mkdir()

    source_dml = Dml.init(str(source_home), user="tester", remote_root=remote_root)

    dag = api.new("test-raises", dml=source_dml)
    fn = dag.put(one_over, name="fn")
    assert fn(1, sleep=lambda: 0, timeout=10_000).value() == 1

    with pytest.raises(api.Error, match="division by zero"):
        fn(0, sleep=lambda: 0, timeout=10_000, name="err-val")
    dag.commit(fn(2, sleep=lambda: 0, timeout=10_000))
    fin_dag = api.load("test-raises", dml=source_dml)
    with pytest.raises(api.Error, match="division by zero"):
        fin_dag["err-val"]
