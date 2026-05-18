from __future__ import annotations

import json
from io import StringIO
from unittest.mock import patch

import pytest

from daggerml._cli import MethodCLI


class _NamespaceExampleNamespace:
    def __init__(self, project_home: str):
        self.project_home = project_home

    def render(self, payload: list[int], enabled: bool = False):
        return {"project_home": self.project_home, "payload": payload, "enabled": enabled}


class _NamespaceExample:
    def __init__(self, project_home: str):
        self.project_home = project_home

    @property
    def namespace(self) -> _NamespaceExampleNamespace:
        return _NamespaceExampleNamespace(self.project_home)


def test_method_cli_calls_root_classmethod_without_instantiating_root():
    class Example:
        def __init__(self):
            raise AssertionError("root should not be instantiated")

        @classmethod
        def init(cls, value: int = 1):
            return {"kind": cls.__name__, "value": value}

    cli = MethodCLI(Example, prog="example")

    with patch("sys.stdout", new_callable=StringIO) as stdout:
        assert cli.run(["init", "--value", "7"]) == 0

    assert json.loads(stdout.getvalue()) == {"kind": "Example", "value": 7}


def test_method_cli_resolves_constructor_args_namespaces_and_json_inputs():
    cli = MethodCLI(_NamespaceExample, prog="example")

    with patch("sys.stdout", new_callable=StringIO) as stdout:
        assert cli.run(["--project-home", "/tmp/repo", "namespace", "render", "[1, 2]", "--enabled"]) == 0

    assert json.loads(stdout.getvalue()) == {"enabled": True, "payload": [1, 2], "project_home": "/tmp/repo"}


def test_method_cli_only_exposes_classmethods_on_root_class():
    class Namespace:
        @classmethod
        def init(cls):
            return {"kind": cls.__name__}

    class Example:
        def __init__(self):
            pass

        @property
        def namespace(self) -> Namespace:
            return Namespace()

    cli = MethodCLI(Example, prog="example")

    with pytest.raises(SystemExit):
        cli.parser.parse_args(["namespace", "init"])


def test_method_cli_main_reports_exceptions_to_stderr():
    class Example:
        def __init__(self):
            pass

        def explode(self):
            raise RuntimeError("boom")

    cli = MethodCLI(Example, prog="example")

    with patch("sys.stderr", new_callable=StringIO) as stderr:
        assert cli.main(["explode"]) == 1

    assert "error: boom" in stderr.getvalue()
