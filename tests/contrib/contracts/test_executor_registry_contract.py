from __future__ import annotations

from pathlib import Path

import pytest

from daggerml import Runnable, Uri
from daggerml.api import DmlRepoError
from daggerml.contrib.executors import _base as reg


class ExecutorSpec:
    def __init__(self, name: str, adapter: str):
        self.name = name
        self.adapter = adapter

    @staticmethod
    def resolve_runnable(uri, kwargs, sub):
        return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

    @staticmethod
    def start(**kwargs):
        return {"status": "running", "error": None, "state": {}}

    @staticmethod
    def poll(**kwargs):
        return {"status": "running", "error": None, "state": {}}

    @staticmethod
    def cleanup(**kwargs):
        return None


class FakeEntryPoint:
    def __init__(self, name: str, value: str, loaded):
        self.name = name
        self.value = value
        self._loaded = loaded

    def load(self):
        return self._loaded


@pytest.fixture(autouse=True)
def reset_registry(monkeypatch):
    monkeypatch.setattr(reg, "_EXECUTOR_SPECS", {})
    monkeypatch.setattr(reg, "_PLUGINS_LOADED", False)


def test_contrib_ereg_001__registration_get_and_list_are_deterministic():
    reg._EXECUTOR_SPECS[("local", "custom")] = ExecutorSpec("custom", "local")
    assert reg.get_executor("local", "custom").name == "custom"
    assert "custom" in reg.list_executors("local")


def test_contrib_ereg_002__missing_executor_lookup_raises_repo_error():
    with pytest.raises(DmlRepoError, match="is not registered"):
        reg.get_executor("local", "missing-start")


def test_contrib_ereg_003__plugin_entries_load_declared_executor_specs(monkeypatch):
    monkeypatch.setattr(
        reg,
        "_entry_points",
        lambda group: [
            FakeEntryPoint("a", "mod:a", ExecutorSpec("script", "local")),
            FakeEntryPoint("b", "mod:b", ExecutorSpec("batch", "lambda")),
        ],
    )
    reg.load_executor_plugins()
    assert reg.list_executors("local") == ["script"]
    assert reg.list_executors("lambda") == ["batch"]


def test_contrib_ereg_004__builtin_executor_entry_points_remain_declared_in_pyproject():
    pyproject = (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text()
    assert '[project.entry-points."daggerml.contrib.executors"]' in pyproject
    assert 'batch = "daggerml.contrib.executors:BatchExecutor"' in pyproject
    assert 'script = "daggerml.contrib.executors:ScriptExecutor"' in pyproject
