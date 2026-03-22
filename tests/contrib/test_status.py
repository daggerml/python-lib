from __future__ import annotations

from typing import Any, cast

import pytest

from daggerml import Runnable, Uri
from daggerml._internal import codec as codec_mod
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib import status as cstatus
from daggerml.contrib.executor_state import LocalState


class _FakeEntryPoint:
    def __init__(self, group: str, name: str, value: str, loaded):
        self.group = group
        self.name = name
        self.value = value
        self._loaded = loaded

    def load(self):
        if isinstance(self._loaded, Exception):
            raise self._loaded
        return self._loaded


@pytest.fixture(autouse=True)
def _reset_state():
    areg._reset_for_tests()
    ereg._reset_for_tests()
    with codec_mod._lock:
        old_codecs = list(codec_mod._literal_codecs)
        old_seq = codec_mod._literal_codec_seq
        old_loaded = codec_mod._plugins_loaded
        codec_mod._literal_codecs = []
        codec_mod._literal_codec_seq = 0
        codec_mod._plugins_loaded = False
    yield
    areg._reset_for_tests()
    ereg._reset_for_tests()
    with codec_mod._lock:
        codec_mod._literal_codecs = old_codecs
        codec_mod._literal_codec_seq = old_seq
        codec_mod._plugins_loaded = old_loaded


def test_status_reports_runtime_registrations(monkeypatch):
    monkeypatch.setattr(areg, "_entry_points", lambda: [])
    monkeypatch.setattr(ereg, "_entry_points", lambda: [])
    monkeypatch.setattr(codec_mod, "_entry_points", lambda: [])

    class CustomAdapter:
        name = "custom"
        executable = "custom-exec"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return (uri, kwargs, sub)

        @staticmethod
        def send(*, runnable, argv_ptr, cache_key, remote):
            return {"status": "running", "error": None}

        @staticmethod
        def cli(argv=None):
            return 0

    class CustomExecutor:
        name = "custom"
        adapter = "local"
        state_class = LocalState

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @staticmethod
        def start(*, runnable, argv_ptr, cache_key, remote, state=None):
            return {"status": "running", "error": None}

        @staticmethod
        def poll(*, state=None):
            return {"status": "running", "error": None}

        @staticmethod
        def gc(*, state=None):
            return None

    class CustomCodec:
        def can_encode(self, value):
            return False

        def encode(self, value, ctx):
            return value

    areg.register_adapter(CustomAdapter())
    ereg.register_executor(CustomExecutor)
    codec_mod.register_codec(CustomCodec(), priority=7)

    result = cast(dict[str, Any], cstatus.status())

    assert result["schema_version"] == 1
    assert result["summary"] == {
        "has_errors": False,
        "diagnostic_count": 0,
        "adapter_registration_count": 1,
        "adapter_effective_count": 1,
        "executor_registration_count": 1,
        "executor_effective_count": 1,
        "codec_registration_count": 1,
        "codec_effective_count": 1,
    }

    adapter = result["adapters"][0]
    assert adapter["key"] == "custom"
    assert adapter["fqn"].endswith("CustomAdapter")
    assert adapter["effective"] is True
    assert adapter["implements"] == {"resolve_runnable": True, "send": True, "cli": True}
    assert set(adapter.keys()) == {"key", "fqn", "effective", "implements"}

    executor = result["executors"][0]
    assert executor["key"] == "local:custom"
    assert executor["fqn"].endswith("CustomExecutor")
    assert executor["implements"]["state_class"] is True
    assert executor["implements"]["state_class_lock"] is True

    codec = result["codecs"][0]
    assert codec["fqn"].endswith("CustomCodec")
    assert codec["effective"] is True

    assert result["diagnostics"] == []


def test_status_reports_best_effort_plugin_failures(monkeypatch):
    class PluginAdapter:
        name = "plugin"
        executable = "plugin-exec"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return (uri, kwargs, sub)

        @staticmethod
        def send(*, runnable, argv_ptr, cache_key, remote):
            return {"status": "running", "error": None}

        @staticmethod
        def cli(argv=None):
            return 0

    class PluginExecutor:
        name = "script"
        adapter = "local"
        state_class = LocalState

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @staticmethod
        def start(*, runnable, argv_ptr, cache_key, remote, state=None):
            return {"status": "running", "error": None}

        @staticmethod
        def poll(*, state=None):
            return {"status": "running", "error": None}

        @staticmethod
        def gc(*, state=None):
            return None

    class PluginCodec:
        def can_encode(self, value):
            return False

        def encode(self, value, ctx):
            return value

    monkeypatch.setattr(
        areg,
        "_entry_points",
        lambda: [
            _FakeEntryPoint(areg.ADAPTER_ENTRYPOINT_GROUP, "bad", "pkg.bad:adapter", RuntimeError("nope")),
            _FakeEntryPoint(areg.ADAPTER_ENTRYPOINT_GROUP, "plugin", "pkg.good:adapter", PluginAdapter),
        ],
    )
    monkeypatch.setattr(
        ereg,
        "_entry_points",
        lambda: [_FakeEntryPoint(ereg.EXECUTOR_ENTRYPOINT_GROUP, "script", "pkg.exec:executor", PluginExecutor)],
    )
    monkeypatch.setattr(
        codec_mod,
        "_entry_points",
        lambda: [
            _FakeEntryPoint(
                codec_mod.LITERAL_CODEC_ENTRYPOINT_GROUP, "codec", "pkg.codec:factory", lambda: (PluginCodec(), 5)
            )
        ],
    )

    result = cast(dict[str, Any], cstatus.status())

    assert result["summary"]["has_errors"] is True
    assert result["summary"]["adapter_registration_count"] == 1
    assert result["summary"]["adapter_effective_count"] == 1
    assert result["summary"]["executor_registration_count"] == 1
    assert result["summary"]["codec_registration_count"] == 1

    assert result["adapters"][0]["key"] == "plugin"
    assert result["adapters"][0]["fqn"].endswith("PluginAdapter")
    assert result["executors"][0]["key"] == "local:script"
    assert result["executors"][0]["fqn"].endswith("PluginExecutor")
    assert result["codecs"][0]["key"].startswith("5:0:")
    assert result["codecs"][0]["key"].endswith("PluginCodec")
    assert result["codecs"][0]["fqn"].endswith("PluginCodec")
    assert [item["code"] for item in result["diagnostics"]] == ["entry_point_load_failed"]


def test_status_loads_codec_plugins_into_runtime_registry(monkeypatch):
    class PluginCodec:
        def can_encode(self, value):
            return False

        def encode(self, value, ctx):
            return value

    monkeypatch.setattr(areg, "_entry_points", lambda: [])
    monkeypatch.setattr(ereg, "_entry_points", lambda: [])
    monkeypatch.setattr(
        codec_mod,
        "_entry_points",
        lambda: [
            _FakeEntryPoint(
                codec_mod.LITERAL_CODEC_ENTRYPOINT_GROUP, "codec", "pkg.codec:factory", lambda: (PluginCodec(), 5)
            )
        ],
    )

    result = cast(dict[str, Any], cstatus.status())

    assert result["summary"]["codec_registration_count"] == 1
    assert result["codecs"][0]["fqn"].endswith("PluginCodec")
    assert codec_mod._plugins_loaded is True
    assert len(codec_mod._literal_codecs) == 1


def test_status_reports_codec_loader_errors(monkeypatch):
    monkeypatch.setattr(areg, "_entry_points", lambda: [])
    monkeypatch.setattr(ereg, "_entry_points", lambda: [])
    monkeypatch.setattr(
        codec_mod,
        "_entry_points",
        lambda: [
            _FakeEntryPoint(
                codec_mod.LITERAL_CODEC_ENTRYPOINT_GROUP,
                "codec",
                "pkg.codec:factory",
                RuntimeError("codec boom"),
            )
        ],
    )
    monkeypatch.setattr(
        codec_mod,
        "ensure_literal_codec_plugins_loaded",
        lambda: (_ for _ in ()).throw(
            RuntimeError("Literal codec plugin 'codec (pkg.codec:factory)' failed: codec boom")
        ),
    )

    result = cast(dict[str, Any], cstatus.status())

    assert result["summary"]["has_errors"] is True
    assert result["summary"]["codec_registration_count"] == 0
    assert result["diagnostics"] == [
        {
            "severity": "error",
            "scope": "codec",
            "code": "entry_point_load_failed",
            "message": "Literal codec plugin 'codec (pkg.codec:factory)' failed: codec boom",
            "source": {
                "kind": "entry_point",
                "group": codec_mod.LITERAL_CODEC_ENTRYPOINT_GROUP,
                "name": "codec",
                "value": "pkg.codec:factory",
            },
            "key": None,
        }
    ]
