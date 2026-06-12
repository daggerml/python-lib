from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Literal

from daggerml._cli import MethodCLI
from daggerml._core import Dml, Error, Ref, dml_dumps


class _SerdeFixture:
    def ref_or_str(self, value: Ref | str) -> str:
        return f"{type(value).__name__}:{value if isinstance(value, str) else value.to}"

    def nullable_text(self, value: str | int | None) -> str:
        if value is None:
            return "NoneType:None"
        return f"{type(value).__name__}:{value}"

    def ref_or_error(self, value: Ref | Error) -> str:
        if isinstance(value, Error):
            return f"Error:{value.message}"
        return f"Ref:{value.to}"

    def any_error_or_ref(self, value: Any | Error | Ref) -> str:
        if isinstance(value, Ref):
            return f"Ref:{value.to}"
        if isinstance(value, Error):
            return f"Error:{value.message}"
        return f"{type(value).__name__}:{value}"

    def list_or_ref(self, value: list[str] | Ref) -> str:
        if isinstance(value, Ref):
            return f"Ref:{value.to}"
        return json.dumps(value)

    def emit_ref_or_error(self, kind: Literal["ref", "error"]) -> Ref | Error:
        if kind == "error":
            return Error("boom", origin="test", type="ValueError")
        return Ref("node:abc")

    def emit_any_error_or_ref(self, kind: Literal["ref", "int"]) -> Any | Error | Ref:
        if kind == "ref":
            return Ref("node:abc")
        return 7

    def emit_none(self) -> None:
        return None


def _command_help(cli: MethodCLI, *path: str) -> str:
    parser = cli.parser
    for name in path:
        subparsers = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
        parser = subparsers.choices[name]
    return parser.format_help()


def test_cli_sp_001__public_union_commands_drop_type_selectors() -> None:
    cli = MethodCLI(Dml, prog="dml")

    checkout_help = _command_help(cli, "checkout")
    config_set_help = _command_help(cli, "config", "set")
    runtime_commit_help = _command_help(cli, "runtime", "commit")

    assert "--revision-type" not in checkout_help
    assert "--value-type" not in config_set_help
    assert "--value-type" not in runtime_commit_help


def test_cli_sp_002__ref_or_str_prefers_string(capsys) -> None:
    cli = MethodCLI(_SerdeFixture, prog="fixture")

    assert cli.run(["ref-or-str", "node:abc"]) == 0

    assert capsys.readouterr().out == "str:node:abc\n"


def test_cli_sp_003__nullable_string_preserves_null_and_prefers_string(capsys) -> None:
    cli = MethodCLI(_SerdeFixture, prog="fixture")

    assert cli.run(["nullable-text", "123"]) == 0
    assert capsys.readouterr().out == "str:123\n"

    assert cli.run(["nullable-text", "null"]) == 0
    assert capsys.readouterr().out == "NoneType:None\n"


def test_cli_sp_004__ref_or_error_tries_dml_before_ref(tmp_path: Path, capsys) -> None:
    payload = tmp_path / "error.dml"
    payload.write_text(dml_dumps(Error("boom", origin="test", type="ValueError")), encoding="utf-8")
    cli = MethodCLI(_SerdeFixture, prog="fixture")

    assert cli.run(["ref-or-error", str(payload)]) == 0
    assert capsys.readouterr().out == "Error:boom\n"

    assert cli.run(["ref-or-error", "node:abc"]) == 0
    assert capsys.readouterr().out == "Ref:node:abc\n"


def test_cli_sp_005__any_error_or_ref_accepts_any_dml_value(tmp_path: Path, capsys) -> None:
    payload = tmp_path / "value.dml"
    payload.write_text(dml_dumps(7), encoding="utf-8")
    cli = MethodCLI(_SerdeFixture, prog="fixture")

    assert cli.run(["any-error-or-ref", str(payload)]) == 0
    assert capsys.readouterr().out == "int:7\n"


def test_cli_sp_006__collection_or_ref_uses_json_file_before_ref(tmp_path: Path, capsys) -> None:
    payload = tmp_path / "value.json"
    payload.write_text(json.dumps(["a", "b"]), encoding="utf-8")
    cli = MethodCLI(_SerdeFixture, prog="fixture")

    assert cli.run(["list-or-ref", str(payload)]) == 0
    assert capsys.readouterr().out == '["a", "b"]\n'

    assert cli.run(["list-or-ref", "node:abc"]) == 0
    assert capsys.readouterr().out == "Ref:node:abc\n"


def test_cli_sp_007__union_output_uses_runtime_compatible_serializer(capsys) -> None:
    cli = MethodCLI(_SerdeFixture, prog="fixture")

    assert cli.run(["emit-ref-or-error", "ref"]) == 0
    assert capsys.readouterr().out == "node:abc\n"

    assert cli.run(["emit-ref-or-error", "error"]) == 0
    assert capsys.readouterr().out == dml_dumps(Error("boom", origin="test", type="ValueError")) + "\n"


def test_cli_sp_008__any_union_output_prefers_dml_serializer(capsys) -> None:
    cli = MethodCLI(_SerdeFixture, prog="fixture")

    assert cli.run(["emit-any-error-or-ref", "ref"]) == 0
    assert capsys.readouterr().out == dml_dumps(Ref("node:abc")) + "\n"

    assert cli.run(["emit-any-error-or-ref", "int"]) == 0
    assert capsys.readouterr().out == dml_dumps(7) + "\n"


def test_cli_sp_009__none_return_prints_nothing(capsys) -> None:
    cli = MethodCLI(_SerdeFixture, prog="fixture")

    assert cli.run(["emit-none"]) == 0

    assert capsys.readouterr().out == ""
