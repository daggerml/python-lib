from __future__ import annotations

from pathlib import Path

import pytest

from daggerml.api import DmlRepoError
from daggerml.contrib import adapters as reg


class AdapterSpec:
    def __init__(self, name: str):
        self.name = name
        self.executable = f"{name}-exec"

    @staticmethod
    def resolve_runnable(uri, kwargs, sub):
        return (uri, kwargs, sub)

    @staticmethod
    def send(**kwargs):
        return {"status": "retry", "error": None, "adapter_state": {}}

    @staticmethod
    def cli(argv=None):
        return 0


class FakeEntryPoint:
    def __init__(self, name: str, value: str, loaded):
        self.name = name
        self.value = value
        self._loaded = loaded

    def load(self):
        return self._loaded


@pytest.fixture(autouse=True)
def reset_registry(monkeypatch):
    monkeypatch.setattr(reg, "_ADAPTER_SPECS", {})
    monkeypatch.setattr(reg, "_PLUGINS_LOADED", False)


def test_contrib_areg_002__plugin_entries_load_declared_adapter_specs(monkeypatch):
    monkeypatch.setattr(
        reg,
        "_entry_points",
        lambda group: [
            FakeEntryPoint("a", "mod:a", AdapterSpec("local")),
            FakeEntryPoint("b", "mod:b", AdapterSpec("docker")),
        ],
    )
    reg.load_adapter_plugins()
    assert reg.list_adapters() == ["docker", "local"]
    assert reg.get_adapter("local").executable == "local-exec"


def test_contrib_areg_003__invalid_plugin_values_fail_with_repo_error(monkeypatch):
    monkeypatch.setattr(reg, "_entry_points", lambda group: [FakeEntryPoint("bad", "mod:bad", object())])
    with pytest.raises(DmlRepoError, match=r"Adapter plugin 'bad \(mod:bad\)' failed"):
        reg.load_adapter_plugins()


def test_contrib_areg_004__builtin_adapter_entry_points_remain_declared_in_pyproject():
    pyproject = (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text()
    assert '[project.entry-points."daggerml.contrib.adapters"]' in pyproject
    assert 'local = "daggerml.contrib.adapters:LocalAdapter"' in pyproject
    assert 'lambda = "daggerml.contrib.adapters:LambdaAdapter"' in pyproject
