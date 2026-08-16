from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import daggerml.api as api
from daggerml._core import DmlRepoError


def test_api_default_001__resolve_without_create_requires_configured_default():
    with pytest.raises(DmlRepoError, match="No default Dml is configured"):
        api._resolve_default_dml(create=False)


def test_api_default_002__implicit_default_is_created_once():
    first = MagicMock(name="first-dml")

    with patch.object(api, "Dml", autospec=True, return_value=first) as dml_cls:
        assert api._resolve_default_dml(create=True) == (first, "implicit")
        assert api.get_default_dml() is first

    dml_cls.assert_called_once_with()
    assert api._resolve_default_dml(create=False) == (first, "process")


def test_api_default_003__process_default_wins_over_implicit(fake_dml):
    api.set_default_dml(fake_dml)

    assert api.get_default_dml() is fake_dml
    assert api._resolve_default_dml(create=False) == (fake_dml, "process")


def test_api_default_004__clear_default_removes_process_default(fake_dml):
    api.set_default_dml(fake_dml)
    api.clear_default_dml()

    with pytest.raises(DmlRepoError, match="No default Dml is configured"):
        api._resolve_default_dml(create=False)


def test_api_default_005__scoped_default_wins_and_restores(fake_dml):
    process = MagicMock(name="process-dml")
    scoped = MagicMock(name="scoped-dml")
    inner = MagicMock(name="inner-scoped-dml")
    api.set_default_dml(process)

    with api.use_default_dml(scoped) as active:
        assert active is scoped
        assert api.get_default_dml() is scoped
        assert api._resolve_default_dml(create=False) == (scoped, "scoped")
        with api.use_default_dml(inner):
            assert api.get_default_dml() is inner
        assert api.get_default_dml() is scoped

    assert api.get_default_dml() is process


def test_api_default_006__status_reports_default_metadata(fake_dml):
    api.set_default_dml(fake_dml)

    assert api.status() == {
        "default": {"source": "process", "has_scoped_override": False, "has_process_default": True},
        "status": {"repo": "ok"},
    }
    fake_dml.status.assert_called_once_with()

    scoped = MagicMock()
    scoped.status.return_value = {"repo": "scoped"}
    with api.use_default_dml(scoped):
        assert api.status() == {
            "default": {"source": "scoped", "has_scoped_override": True, "has_process_default": True},
            "status": {"repo": "scoped"},
        }


def test_api_default_007__new_uses_runtime_create_and_returns_working_dag(fake_dml, refs):
    dag = api.new("demo", message="msg", cache_key="cache", execution_id="exec", dml=fake_dml)

    fake_dml.runtime.create.assert_called_once_with(cache_key="cache", execution=api.Ref("index:exec"))
    assert dag.dml is fake_dml
    assert dag.token == refs.index
    assert dag.ref is None
    assert dag.name == "demo"
    assert dag.message == "msg"


def test_api_default_008__new_uses_active_default(fake_dml):
    api.set_default_dml(fake_dml)

    assert api.new().dml is fake_dml


def test_api_default_009__load_resolves_named_dag(fake_dml, refs):
    dag = api.load("demo", dml=fake_dml)

    fake_dml.show.assert_called_once_with("HEAD", remote=False, dep=None)
    assert dag.dml is fake_dml
    assert dag.ref == refs.dag
    assert dag.name == "demo"


def test_api_default_010__load_missing_dag_raises(fake_dml):
    with pytest.raises(DmlRepoError, match="DAG not found: missing"):
        api.load("missing", dml=fake_dml)


def test_api_default_011__temporary_initializes_and_yields_runtime():
    runtime = MagicMock(name="runtime")
    init = MagicMock(return_value=runtime)

    class FakeDml:
        @classmethod
        def init(cls, **kwargs):
            return init(**kwargs)

    with patch.object(api, "Dml", FakeDml):
        with api.temporary(user="tester") as dml:
            assert dml is runtime

    init.assert_called_once()
    assert init.call_args.kwargs["user"] == "tester"
    assert "project_home" in init.call_args.kwargs


def test_api_default_012__resume_unfreezes_with_explicit_metadata_and_active_default(fake_dml, refs):
    api.set_default_dml(fake_dml)
    fake_dml.runtime.unfreeze.return_value = refs.index

    dag = api.resume(refs.index, name="resumed", message="complete review", tags=None)

    fake_dml.runtime.unfreeze.assert_called_once_with(refs.index)
    assert dag.dml is fake_dml
    assert dag.token == refs.index
    assert dag.ref is None
    assert (dag.name, dag.message, dag.tags) == ("resumed", "complete review", None)

    with pytest.raises(TypeError):
        api.resume(refs.index, name="resumed", message="complete review")
