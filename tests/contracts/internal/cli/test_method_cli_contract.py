from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from daggerml._cli import MethodCLI
from daggerml._internal import dml_dumps, dml_loads


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


def test_method_cli_reads_exact_any_parameter_from_stdin_by_default():
    class Example:
        def __init__(self):
            pass

        def render(self, payload: Any):
            return {"payload": payload}

    cli = MethodCLI(Example, prog="example")

    with patch("sys.stdin", StringIO(dml_dumps({"alpha": [1, 2]}))), patch(
        "sys.stdout", new_callable=StringIO
    ) as stdout:
        assert cli.run(["render"]) == 0

    assert json.loads(stdout.getvalue()) == {"payload": {"alpha": [1, 2]}}


def test_method_cli_reads_exact_any_parameter_from_file_and_writes_exact_any_output(tmp_path: Path):
    class Example:
        def __init__(self):
            pass

        def render(self, payload: Any) -> Any:
            return {"wrapped": payload}

    cli = MethodCLI(Example, prog="example")
    payload_path = tmp_path / "payload.dml"
    payload_path.write_text(dml_dumps({"alpha": [1, 2]}), encoding="utf-8")

    with patch("sys.stdout", new_callable=StringIO) as stdout:
        assert cli.run(["render", str(payload_path)]) == 0

    assert dml_loads(stdout.getvalue()) == {"wrapped": {"alpha": [1, 2]}}


def test_method_cli_reads_defaulted_exact_any_parameter_from_stdin_when_option_is_omitted():
    class Example:
        def __init__(self):
            pass

        def render(self, payload: Any = None):
            return {"payload": payload}

    cli = MethodCLI(Example, prog="example")

    with patch("sys.stdin", StringIO(dml_dumps({"beta": [3, 4]}))), patch(
        "sys.stdout", new_callable=StringIO
    ) as stdout:
        assert cli.run(["render"]) == 0

    assert json.loads(stdout.getvalue()) == {"payload": {"beta": [3, 4]}}


def test_method_cli_treats_semantically_invalid_exact_any_input_as_parse_failure(tmp_path: Path):
    class Example:
        def __init__(self):
            pass

        def render(self, payload: Any):
            return {"payload": payload}

    cli = MethodCLI(Example, prog="example")
    payload_path = tmp_path / "payload.dml"
    payload_path.write_text('{"__dml__":{"t":"Unknown"}}', encoding="utf-8")

    with patch("sys.stderr", new_callable=StringIO) as stderr, pytest.raises(SystemExit) as exc_info:
        cli.main(["render", str(payload_path)])

    assert exc_info.value.code == 2
    assert (
        "error: argument payload: expected DML-serialized input: unknown dml tag type: 'Unknown'"
        in stderr.getvalue()
    )


def test_method_cli_does_not_treat_optional_any_as_exact_any_transport(tmp_path: Path):
    class Example:
        def __init__(self):
            pass

        def render(self, payload: Any | None = None):
            return {"payload": payload}

    cli = MethodCLI(Example, prog="example")
    payload_path = tmp_path / "payload.dml"
    payload_path.write_text(dml_dumps({"alpha": [1, 2]}), encoding="utf-8")

    with patch("sys.stdout", new_callable=StringIO) as stdout:
        assert cli.run(["render", "--payload", str(payload_path)]) == 0

    assert json.loads(stdout.getvalue()) == {"payload": str(payload_path)}
