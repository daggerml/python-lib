import pytest

from daggerml import codecs, new
from daggerml._internal.types import DmlRepoError
from daggerml.contrib import api
from tests import temporary_dml


def test_ref_returns_delayed_ref():
    value = api.ref("x")
    assert isinstance(value, api.DelayedRef)
    assert value.name == "x"


def test_delayed_action_codec_matches_delayed_ref():
    codec = codecs.DelayedActionCodec()
    assert codec.can_encode(api.DelayedRef("x"))
    assert codec.can_encode(api.DelayedLoad("d0"))
    assert not codec.can_encode("x")


def test_ref_resolves_when_staged():
    with temporary_dml() as dml:
        dag = new(dml=dml, name="d0", message="d0")
        dag.a = 42
        out = dag.put(api.ref("a"))
        assert out.value() == 42


def test_ref_resolves_in_nested_values():
    with temporary_dml() as dml:
        dag = new(dml=dml, name="d0", message="d0")
        dag.a = 7
        out = dag.put({"v": [api.ref("a")]})
        assert out.value() == {"v": [7]}


def test_ref_missing_name_fails():
    with temporary_dml() as dml:
        dag = new(dml=dml, name="d0", message="d0")
        with pytest.raises(DmlRepoError, match="Node 'missing' not found in DAG"):
            dag.put(api.ref("missing"))


def test_load_returns_delayed_load():
    value = api.load("d0")
    assert isinstance(value, api.DelayedLoad)
    assert value.dagname == "d0"
    assert value.nodename is None


def test_load_resolves_result_node_when_nodename_none():
    with temporary_dml() as dml:
        src = new(dml=dml, name="src", message="src")
        src.result_named = 123
        src.commit(999)

        dst = new(dml=dml, name="dst", message="dst")
        out = dst.put(api.load("src"))
        assert out.value() == 999


def test_load_resolves_named_node_when_nodename_set():
    with temporary_dml() as dml:
        src = new(dml=dml, name="src", message="src")
        src.result_named = 123
        src.commit(999)

        dst = new(dml=dml, name="dst", message="dst")
        out = dst.put(api.load("src", "result_named"))
        assert out.value() == 123


def test_load_missing_dag_fails():
    with temporary_dml() as dml:
        dag = new(dml=dml, name="d0", message="d0")
        with pytest.raises(DmlRepoError, match="DAG 'missing' not found"):
            dag.put(api.load("missing"))


def test_load_missing_node_fails():
    with temporary_dml() as dml:
        src = new(dml=dml, name="src", message="src")
        src.commit(1)

        dst = new(dml=dml, name="dst", message="dst")
        with pytest.raises(DmlRepoError, match="Node 'missing' not found in DAG 'src'"):
            dst.put(api.load("src", "missing"))
