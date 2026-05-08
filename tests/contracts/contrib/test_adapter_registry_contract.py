from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib import adapter_registry as reg
from daggerml.contrib.adapters import LambdaAdapter


@dataclass
class AdapterSpec:
    name: str
    executable: str = "x"

    def resolve_runnable(self, uri, kwargs, sub):
        return (uri, kwargs, sub)

    @staticmethod
    def send(*, runnable, argv_ptr, cache_key, execution_id, remote, state, execution_status, cancel_requested_by):
        return {"status": "running", "error": None, "state": {"token": execution_id}}

    @staticmethod
    def cli(argv=None):
        return 0


@pytest.fixture(autouse=True)
def _reset_registry():
    reg._reset_for_tests()
    yield
    reg._reset_for_tests()


def test_register_get_and_list_adapter():
    reg.register_adapter(AdapterSpec("custom"))
    loaded = reg.get_adapter("custom")
    assert loaded.name == "custom"
    assert reg.list_adapters() == ["custom", "lambda", "local"]


def test_register_adapter_accepts_class_object():
    class CustomAdapter:
        name = "custom"
        executable = "custom-exec"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return (uri, kwargs, sub)

        @staticmethod
        def send(*, runnable, argv_ptr, cache_key, execution_id, remote, state, execution_status, cancel_requested_by):
            return {"status": "running", "error": None, "state": {"token": execution_id}}

        @staticmethod
        def cli(argv=None):
            return 0

    reg.register_adapter(CustomAdapter)
    loaded = reg.get_adapter("custom")
    assert loaded is CustomAdapter


def test_register_adapter_missing_required_attribute_fails():
    class BadAdapter:
        name = "bad"
        executable = "bad-exec"

    with pytest.raises(DmlRepoError, match="missing required attribute: resolve_runnable"):
        reg.register_adapter(BadAdapter)


def test_get_unknown_adapter_fails():
    with pytest.raises(DmlRepoError, match="Adapter 'missing' is not registered"):
        reg.get_adapter("missing")


class _FakeEntryPoint:
    def __init__(self, name: str, value: str, loaded):
        self.name = name
        self.value = value
        self._loaded = loaded

    def load(self):
        return self._loaded


def test_plugin_loading_contract_variants(monkeypatch):
    def _factory():
        return AdapterSpec("batch")

    monkeypatch.setattr(
        reg,
        "_entry_points",
        lambda: [
            _FakeEntryPoint("a", "mod:a", AdapterSpec("local")),
            _FakeEntryPoint("b", "mod:b", [AdapterSpec("docker"), _factory]),
        ],
    )

    assert reg.list_adapters() == ["batch", "docker", "local"]


def test_plugin_loading_invalid_return_fails(monkeypatch):
    monkeypatch.setattr(reg, "_entry_points", lambda: [_FakeEntryPoint("bad", "mod:bad", object())])

    with pytest.raises(DmlRepoError, match="returned invalid adapter registration"):
        reg.load_adapter_plugins()


def test_lambda_adapter_invokes_runnable_target(monkeypatch):
    seen = {}

    class _Payload:
        def read(self):
            return (
                b'{"status":"succeeded","error":null,'
                b'"dag_id":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}'
            )

    class _Client:
        def invoke(self, **kwargs):
            seen.update(kwargs)
            return {"Payload": _Payload()}

    monkeypatch.setattr("daggerml.contrib.adapters.get_client", lambda name: _Client())

    result = LambdaAdapter.send(
        runnable=Runnable(target=Uri("lambda-fn"), adapter="dml-lambda-adapter", kwargs={}),
        argv_ptr="ptr",
        cache_key="ck",
        execution_id="exec-ck",
        remote={},
        state=None,
        execution_status=None,
        cancel_requested_by=None,
    )

    assert result == {"status": "succeeded", "error": None, "dag_id": "d" * 64}
    assert seen["FunctionName"] == "lambda-fn"


def test_pyproject_declares_builtin_adapter_entry_points():
    pyproject = (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text()

    assert '[project.entry-points."daggerml.contrib.adapters"]' in pyproject
    assert 'local = "daggerml.contrib.adapters:LocalAdapter"' in pyproject
    assert 'lambda = "daggerml.contrib.adapters:LambdaAdapter"' in pyproject
