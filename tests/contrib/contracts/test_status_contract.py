from __future__ import annotations

from daggerml.contrib import status as status_mod
from daggerml.contrib.adapters import LambdaAdapter, LocalAdapter
from daggerml.contrib.executors import BatchExecutor, DockerExecutor, ScriptExecutor, SshExecutor


def test_contrib_status_001__executor_diagnostics_include_explicit_cleanup_requirement(monkeypatch):
    class IncompleteExecutor:
        resolve_runnable = staticmethod(lambda *args: None)
        start = staticmethod(lambda **kwargs: None)
        poll = staticmethod(lambda **kwargs: None)
        cancel = staticmethod(lambda **kwargs: None)

    monkeypatch.setattr(status_mod.ereg, "load_executor_plugins", lambda: None)
    monkeypatch.setattr(status_mod.ereg, "_EXECUTOR_SPECS", {("local", "incomplete"): IncompleteExecutor})

    diagnostics = []
    registrations = status_mod._executor_status(diagnostics)

    assert registrations == [
        {
            "key": "local:incomplete",
            "fqn": f"{IncompleteExecutor.__module__}:{IncompleteExecutor.__qualname__}",
            "effective": False,
            "implements": {
                "resolve_runnable": True,
                "start": True,
                "poll": True,
                "cleanup": False,
                "cancel": True,
            },
        }
    ]
    assert diagnostics == [
        {
            "severity": "error",
            "scope": "executor",
            "code": "required_operation_missing",
            "message": "local:incomplete is missing required operations: cleanup",
        }
    ]


def test_contrib_status_002__builtins_report_new_operation_surface() -> None:
    for adapter in (LocalAdapter, LambdaAdapter):
        registration = status_mod._registration("adapter", adapter.name, adapter)
        assert registration["effective"] is True
        assert registration["implements"] == {
            "resolve_runnable": True,
            "send": True,
            "cli": True,
        }
        assert "poll" not in registration["implements"]

    for executor in (ScriptExecutor, DockerExecutor, BatchExecutor, SshExecutor):
        registration = status_mod._registration("executor", f"{executor.adapter}:{executor.name}", executor)
        assert registration["effective"] is True
        assert registration["implements"] == {
            "resolve_runnable": True,
            "start": True,
            "poll": True,
            "cleanup": True,
            "cancel": True,
        }
        assert "gc" not in registration["implements"]
