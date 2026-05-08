from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from daggerml import Dml, clear_default_dml, set_default_dml
from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import api


@pytest.fixture(autouse=True)
def _runtime_setup():
    areg._reset_for_tests()

    @dataclass
    class TestAdapter:
        name: str = "test"
        executable: str = "test-adapter"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=kwargs, sub=sub, adapter="")

        @staticmethod
        def send(*, runnable, argv_ptr, cache_key, execution_id, remote, state, execution_status, cancel_requested_by):
            return {"status": "running", "error": None, "state": {"token": execution_id}}

        @staticmethod
        def cli(argv=None):
            return 0

    areg.register_adapter(TestAdapter())
    with Dml.temporary() as dml:
        set_default_dml(dml)
        yield dml
    clear_default_dml()
    areg._reset_for_tests()


def test_run_executes_entrypoint_and_returns_none(_runtime_setup):
    @api.dagclass
    class RunExample:
        x: Any = 7
        main: Any = api.DelayedRunnable(uri="daggerml:list", adapter="test", sub=None, kwargs={})

    result = api.run(RunExample(), 1, 2, name="run-example")
    assert result is None
    loaded = _runtime_setup.load("run-example")
    assert "x" in loaded.keys()
    assert loaded["x"].value() == 7
    assert "main" in loaded.keys()
    assert "<dagclass-call>" in loaded.keys()
    assert loaded["<dagclass-call>"].value() == [1, 2]
    assert loaded.result.value() == [1, 2]


def test_run_materializes_additional_delayed_runnable_members_by_name(_runtime_setup):
    @api.dagclass
    class RunExample:
        x: Any = 3
        main: Any = api.DelayedRunnable(uri="daggerml:list", adapter="test", sub=None, kwargs={})
        alt: Any = api.DelayedRunnable(uri="daggerml:list", adapter="test", sub=None, kwargs={})

    result = api.run(RunExample(), 9, name="run-funkified")
    assert result is None
    loaded = _runtime_setup.load("run-funkified")
    assert "x" in loaded.keys()
    assert loaded["x"].value() == 3
    assert "main" in loaded.keys()
    assert "alt" in loaded.keys()
    assert loaded["<dagclass-call>"].value() == [9]


def test_run_materializes_same_namespace_refs_in_dependency_order(_runtime_setup):
    @api.dagclass
    class RunExample:
        x: Any = api.ref("y")
        y: Any = 3
        main: Any = api.DelayedRunnable(uri="daggerml:list", adapter="test", sub=None, kwargs={})

    result = api.run(RunExample(), name="run-ref-order")
    assert result is None
    loaded = _runtime_setup.load("run-ref-order")
    assert loaded["y"].value() == 3
    assert loaded["x"].value() == 3


def test_run_entrypoint_override():
    @api.dagclass
    class RunExample:
        main: Any = api.DelayedRunnable(uri="daggerml:list", adapter="test", sub=None, kwargs={})
        alt: Any = api.DelayedRunnable(uri="daggerml:list", adapter="test", sub=None, kwargs={})

    result = api.run(RunExample(), 9, entrypoint="alt", name="run-alt")
    assert result is None


def test_run_rejects_non_dagclass_instance():
    with pytest.raises(DmlRepoError, match="not a dagclass instance"):
        api.run(object())


def test_run_rejects_missing_entrypoint():
    @api.dagclass(entrypoint="missing")
    class RunExample:
        main: Any = api.DelayedRunnable(uri="daggerml:list", adapter="test", sub=None, kwargs={})

    with pytest.raises(DmlRepoError, match="entrypoint not found"):
        api.run(RunExample())


def test_run_rejects_non_delayed_runnable_entrypoint():
    @api.dagclass
    class RunExample:
        main: Any = 1

    with pytest.raises(DmlRepoError, match="entrypoint must be DelayedRunnable"):
        api.run(RunExample())


def test_run_rejects_uncompiled_instance():
    @api.dagclass
    class RunExample:
        main: Any = api.DelayedRunnable(uri="daggerml:list", adapter="test", sub=None, kwargs={})

    obj = RunExample()
    obj.__dagclass_compiled__ = False
    with pytest.raises(DmlRepoError, match="instance is not compiled"):
        api.run(obj)


def test_run_default_name_format_contains_class_separator():
    @api.dagclass
    class RunExample:
        main: Any = api.DelayedRunnable(uri="daggerml:list", adapter="test", sub=None, kwargs={})

    result = api.run(RunExample(), 1)
    assert result is None
    # name format contract includes ::<class-name>
    # ensure DAG can be loaded by discovered name from the same instance class
    default_name = api._default_run_name(RunExample())
    assert "::RunExample" in default_name
