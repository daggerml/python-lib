from __future__ import annotations

import json

import pytest

from daggerml.contrib import status as status_mod
from daggerml.contrib.adapters import LambdaAdapter, LocalAdapter
from daggerml.contrib.executors import BatchExecutor, DockerExecutor, ScriptExecutor, SshExecutor


def test_executor_diagnostics_include_explicit_cleanup_requirement(monkeypatch):
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


def test_builtins_report_new_operation_surface() -> None:
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


def test_cli_prints_status_as_json(monkeypatch, capsys):
    report = {"summary": {"has_errors": True}, "diagnostics": [{"message": "plugin failure"}]}
    monkeypatch.setattr(status_mod, "status", lambda: report)

    assert status_mod.cli(["status"]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == report
    assert captured.err == ""


@pytest.mark.parametrize("args", [["--help"], ["status", "--help"]])
def test_cli_help_does_not_load_plugins(monkeypatch, capsys, args):
    def unexpected_status():
        raise AssertionError("help must not load plugins")

    monkeypatch.setattr(status_mod, "status", unexpected_status)
    with pytest.raises(SystemExit) as error:
        status_mod.cli(args)
    assert error.value.code == 0
    assert "Report adapter, executor, and codec" in capsys.readouterr().out


@pytest.mark.parametrize("args", [[], ["unknown"], ["status", "extra"], ["status", "--unknown"]])
def test_cli_rejects_invalid_arguments(monkeypatch, args):
    def unexpected_status():
        raise AssertionError("invalid arguments must not load plugins")

    monkeypatch.setattr(status_mod, "status", unexpected_status)
    with pytest.raises(SystemExit) as error:
        status_mod.cli(args)
    assert error.value.code == 2
