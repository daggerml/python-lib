from __future__ import annotations

from pathlib import Path

import pytest

from daggerml import Runnable, Uri
from daggerml._internal.types import DmlRepoError
from daggerml.contrib import executor_registry as reg


class ExecutorSpec:
    def __init__(self, name: str, adapter: str):
        self.name = name
        self.adapter = adapter

    @staticmethod
    def resolve_runnable(uri, kwargs, sub):
        return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

    @staticmethod
    def start(*, runnable, argv_ptr, cache_key, execution_id, remote, state=None):
        return {"status": "running", "error": None, "state": {"token": execution_id}}

    @staticmethod
    def poll(*, state=None, cache_key=None, execution_id=None, remote=None):
        return {"status": "running", "error": None, "state": state or {}}

    @staticmethod
    def cleanup(*, state=None):
        return None


@pytest.fixture(autouse=True)
def _reset_registry():
    reg._reset_for_tests()
    yield
    reg._reset_for_tests()


def test_register_get_and_list_executor():
    reg.register_executor(ExecutorSpec("custom", "local"))
    loaded = reg.get_executor("local", "custom")
    assert loaded.name == "custom"
    assert loaded.adapter == "local"
    assert reg.list_executors("local") == ["cfn", "custom", "docker", "script", "ssh"]


def test_register_executor_accepts_class_object():
    class CustomExecutor:
        name = "custom"
        adapter = "local"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @staticmethod
        def start(*, runnable, argv_ptr, cache_key, execution_id, remote, state=None):
            return {"status": "running", "error": None, "state": {"token": execution_id}}

        @staticmethod
        def poll(*, state=None, cache_key=None, execution_id=None, remote=None):
            return {"status": "running", "error": None, "state": state or {}}

        @staticmethod
        def cleanup(*, state=None):
            return None

    reg.register_executor(CustomExecutor)
    loaded = reg.get_executor("local", "custom")
    assert loaded is CustomExecutor


def test_register_executor_missing_required_attribute_fails():
    class BadExecutor:
        name = "bad"

    with pytest.raises(DmlRepoError, match="missing required attribute: adapter"):
        reg.register_executor(BadExecutor)


def test_register_executor_missing_required_lifecycle_callable_fails():
    class MissingStartExecutor:
        name = "missing-start"
        adapter = "local"

        @staticmethod
        def poll(*, state):
            return {"status": "running", "error": None, "state": state}

    with pytest.raises(DmlRepoError, match="missing required callables: start, cleanup"):
        reg.register_executor(MissingStartExecutor)


def test_get_unknown_executor_fails():
    with pytest.raises(DmlRepoError, match="Executor 'missing' is not registered for adapter 'local'"):
        reg.get_executor("local", "missing")


class _FakeEntryPoint:
    def __init__(self, name: str, value: str, loaded):
        self.name = name
        self.value = value
        self._loaded = loaded

    def load(self):
        return self._loaded


def test_plugin_loading_contract_variants(monkeypatch):
    def _factory():
        return ExecutorSpec("docker", "local")

    monkeypatch.setattr(
        reg,
        "_entry_points",
        lambda: [
            _FakeEntryPoint("a", "mod:a", ExecutorSpec("script", "local")),
            _FakeEntryPoint("b", "mod:b", [ExecutorSpec("batch", "lambda"), _factory]),
        ],
    )

    assert reg.list_executors() == ["batch", "docker", "script"]
    assert reg.list_executors("local") == ["docker", "script"]
    assert reg.list_executors("lambda") == ["batch"]


def test_plugin_loading_invalid_return_fails(monkeypatch):
    monkeypatch.setattr(reg, "_entry_points", lambda: [_FakeEntryPoint("bad", "mod:bad", object())])

    with pytest.raises(DmlRepoError, match="returned invalid executor registration"):
        reg.load_executor_plugins()


def test_pyproject_declares_builtin_executor_entry_points():
    pyproject = (Path(__file__).resolve().parents[2] / "pyproject.toml").read_text()

    assert '[project.entry-points."daggerml.contrib.executors"]' in pyproject
    assert 'batch = "daggerml.contrib.executors:BatchExecutor"' in pyproject
    assert 'script = "daggerml.contrib.executors:ScriptExecutor"' in pyproject
