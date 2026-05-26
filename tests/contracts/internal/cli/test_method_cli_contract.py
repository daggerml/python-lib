from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from daggerml._cli import MethodCLI
from daggerml._internal import Error, Ref, dml_dumps, dml_loads

DEFAULT_NODE_REF = Ref("node:default")


class _NamespaceExampleNamespace:
    def __init__(self, project_home: str):
        self.project_home = project_home

    def render(self, payload: list[int], enabled: bool = False) -> dict[str, Any]:
        return {"project_home": self.project_home, "payload": payload, "enabled": enabled}


class _NamespaceExample:
    def __init__(self, project_home: str):
        self.project_home = project_home

    @property
    def namespace(self) -> _NamespaceExampleNamespace:
        return _NamespaceExampleNamespace(self.project_home)


class _VariadicNamespace:
    def invalidate(self, *cache_keys: str) -> dict[str, list[str]]:
        return {"cache_keys": list(cache_keys)}


class _VariadicNamespaceExample:
    def __init__(self):
        pass

    @property
    def admin(self) -> _VariadicNamespace:
        return _VariadicNamespace()


class _MixedVariadicNamespace:
    def invalidate(self, scope: str, *cache_keys: str) -> dict[str, str | list[str]]:
        return {"scope": scope, "cache_keys": list(cache_keys)}


class _MixedVariadicNamespaceExample:
    def __init__(self):
        pass

    @property
    def admin(self) -> _MixedVariadicNamespace:
        return _MixedVariadicNamespace()


def test_method_cli_calls_root_classmethod_without_instantiating_root():
    class Example:
        def __init__(self):
            raise AssertionError("root should not be instantiated")

        @classmethod
        def init(cls, value: int = 1) -> dict[str, Any]:
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



def test_method_cli_expands_variadic_positional_arguments_for_nested_namespace_commands():
    cli = MethodCLI(_VariadicNamespaceExample, prog="example")

    with patch("sys.stdout", new_callable=StringIO) as stdout:
        assert cli.run(["admin", "invalidate", "ck1", "ck2"]) == 0

    assert json.loads(stdout.getvalue()) == {"cache_keys": ["ck1", "ck2"]}


def test_method_cli_preserves_required_positionals_before_variadic_arguments():
    cli = MethodCLI(_MixedVariadicNamespaceExample, prog="example")

    with patch("sys.stdout", new_callable=StringIO) as stdout:
        assert cli.run(["admin", "invalidate", "repo", "ck1", "ck2"]) == 0

    assert json.loads(stdout.getvalue()) == {"scope": "repo", "cache_keys": ["ck1", "ck2"]}


def test_method_cli_treats_semantically_invalid_exact_any_input_as_parse_failure(tmp_path: Path):
    class Example:
        def __init__(self):
            pass

        def render(self, payload: Any):
            return {"payload": payload}

    cli = MethodCLI(Example, prog="example")
    payload_path = tmp_path / "payload.dml"
    payload_path.write_text('["unknown",null]', encoding="utf-8")

    with patch("sys.stderr", new_callable=StringIO) as stderr, pytest.raises(SystemExit) as exc_info:
        cli.main(["render", str(payload_path)])

    assert exc_info.value.code == 2
    assert (
        "error: argument payload: expected DML-serialized input: unknown DML envelope type: 'unknown'"
        in stderr.getvalue()
    )


def test_method_cli_treats_optional_any_union_as_dml_transport(tmp_path: Path):
    class Example:
        def __init__(self):
            pass

        def render(self, payload: Any | None = None) -> dict[str, Any]:
            return {"payload": payload}

    cli = MethodCLI(Example, prog="example")
    payload_path = tmp_path / "payload.dml"
    payload_path.write_text(dml_dumps({"alpha": [1, 2]}), encoding="utf-8")

    with patch("sys.stdout", new_callable=StringIO) as stdout:
        assert cli.run(["render", "--payload", str(payload_path)]) == 0

    assert json.loads(stdout.getvalue()) == {"payload": {"alpha": [1, 2]}}


def test_method_cli_parses_ref_or_error_positionals_as_first_transport_by_default():
    class Example:
        def __init__(self):
            pass

        def commit(self, value: Ref | Error) -> dict[str, str]:
            return {"type": type(value).__name__, "value": str(value)}

    cli = MethodCLI(Example, prog="example")

    with patch("sys.stdout", new_callable=StringIO) as stdout:
        assert cli.run(["commit", "node:abc"]) == 0

    assert json.loads(stdout.getvalue()) == {"type": "Ref", "value": "Ref(node:abc)"}


def test_method_cli_uses_selector_for_multi_transport_positionals():
    class Example:
        def __init__(self):
            pass

        def get(self, value: str | Ref) -> dict[str, str]:
            return {"type": type(value).__name__, "value": str(value)}

    cli = MethodCLI(Example, prog="example")

    with patch("sys.stdout", new_callable=StringIO) as stdout:
        assert cli.run(["get", "node:abc", "--value-type", "ref"]) == 0

    assert json.loads(stdout.getvalue()) == {"type": "Ref", "value": "Ref(node:abc)"}


def test_method_cli_defaults_multi_transport_positionals_to_first_member():
    class Example:
        def __init__(self):
            pass

        def get(self, value: str | Ref) -> dict[str, str]:
            return {"type": type(value).__name__, "value": str(value)}

    cli = MethodCLI(Example, prog="example")

    with patch("sys.stdout", new_callable=StringIO) as stdout:
        assert cli.run(["get", "examples/demo"]) == 0

    assert json.loads(stdout.getvalue()) == {"type": "str", "value": "examples/demo"}


def test_method_cli_generates_typed_union_flags_for_multi_transport_options(tmp_path: Path):
    class Example:
        def __init__(self):
            pass

        def commit(self, value: Ref | Error = DEFAULT_NODE_REF) -> dict[str, str]:
            return {"type": type(value).__name__, "value": str(value)}

    cli = MethodCLI(Example, prog="example")
    payload_path = tmp_path / "err.dml"
    payload_path.write_text(
        dml_dumps(Error(message="boom", origin="dml", type="runtimeerror", stack=[])),
        encoding="utf-8",
    )

    with patch("sys.stdout", new_callable=StringIO) as stdout:
        assert cli.run(["commit", "--value-dml", str(payload_path)]) == 0

    assert json.loads(stdout.getvalue()) == {"type": "Error", "value": "boom"}


def test_method_cli_rejects_conflicting_typed_union_flags(tmp_path: Path):
    class Example:
        def __init__(self):
            pass

        def commit(self, value: Ref | Error = DEFAULT_NODE_REF) -> dict[str, str]:
            return value

    cli = MethodCLI(Example, prog="example")
    payload_path = tmp_path / "err.dml"
    payload_path.write_text(
        dml_dumps(Error(message="boom", origin="dml", type="runtimeerror", stack=[])),
        encoding="utf-8",
    )

    with patch("sys.stderr", new_callable=StringIO) as stderr, pytest.raises(SystemExit) as exc_info:
        cli.main(["commit", "--value-ref", "node:abc", "--value-dml", str(payload_path)])

    assert exc_info.value.code == 2
    assert "not allowed with argument" in stderr.getvalue()
