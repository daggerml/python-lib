from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import ClassVar
from unittest.mock import patch

import pytest

from daggerml._cli import MethodCLI
from daggerml._core import Dml


class _ClassmethodFixture:
    calls: ClassVar[list[dict[str, str | None]]] = []

    def __init__(self, shared: str | None = None, different: str | None = None, flag: bool = False) -> None:
        self.shared = shared
        self.different = different
        self.flag = flag

    @classmethod
    def build(
        cls,
        shared: str | None = None,
        different: str = "method-default",
        local: str | None = None,
    ) -> dict[str, str | None]:
        payload = {"shared": shared, "different": different, "local": local}
        cls.calls.append(payload)
        return payload


def _command_parser(cli: MethodCLI, *path: str) -> argparse.ArgumentParser:
    parser = cli.parser
    for name in path:
        subparsers = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
        parser = subparsers.choices[name]
    return parser


def _command_help(cli: MethodCLI, *path: str) -> str:
    return _command_parser(cli, *path).format_help()


def test_cli_cm_001__same_name_same_type_classmethod_param_is_root_only(capsys) -> None:
    _ClassmethodFixture.calls.clear()
    cli = MethodCLI(_ClassmethodFixture, prog="fixture")

    root_help = cli.parser.format_help()
    build_help = _command_help(cli, "build")

    assert "--shared SHARED" in root_help
    assert "--shared" not in build_help

    assert cli.run(["--shared", "root-value", "build"]) == 0

    assert _ClassmethodFixture.calls == [{"shared": "root-value", "different": "method-default", "local": None}]
    assert capsys.readouterr().out == '{"different":"method-default","local":null,"shared":"root-value"}\n'


def test_cli_cm_002__same_name_different_type_classmethod_param_stays_command_local() -> None:
    cli = MethodCLI(_ClassmethodFixture, prog="fixture")

    root_help = cli.parser.format_help()
    build_help = _command_help(cli, "build")

    assert "--different DIFFERENT" in root_help
    assert "--different DIFFERENT" in build_help


def test_cli_cm_003__dml_init_intersected_options_are_root_only() -> None:
    cli = MethodCLI(Dml, prog="dml")

    root_help = cli.parser.format_help()
    init_help = _command_help(cli, "init")

    assert "--db-path DB_PATH" in root_help
    assert "--db-map-size-headroom DB_MAP_SIZE_HEADROOM" in root_help
    assert "--db-map-size-max DB_MAP_SIZE_MAX" in root_help
    assert "--default-branch-name DEFAULT_BRANCH_NAME" in root_help
    assert "--remote-project REMOTE_PROJECT" not in root_help
    assert "--remote-root REMOTE_ROOT" in root_help
    assert "--remote-prune-age-seconds REMOTE_PRUNE_AGE_SECONDS" in root_help
    assert "--remote-fetch-workers REMOTE_FETCH_WORKERS" in root_help
    assert "--user USER" in root_help
    assert "--config-home CONFIG_HOME" in root_help
    assert "--db-path" not in init_help
    assert "--db-map-size-headroom" not in init_help
    assert "--db-map-size-max" not in init_help
    assert "--default-branch-name" not in init_help
    assert "--remote-project" not in init_help
    assert "--remote-root" not in init_help
    assert "--remote-prune-age-seconds" not in init_help
    assert "--remote-fetch-workers" not in init_help
    assert "--user" not in init_help
    assert "--config-home" not in init_help
    assert "--project-home PROJECT_HOME" in init_help

    with pytest.raises(SystemExit):
        cli.parser.parse_args(["init", "--remote-root", "s3://bucket/project"])


def test_cli_cm_004__constructor_metavars_do_not_expose_init_prefix() -> None:
    cli = MethodCLI(Dml, prog="dml")

    root_help = cli.parser.format_help()

    assert "_INIT_" not in root_help
    assert "--remote-root REMOTE_ROOT" in root_help
    assert "--project-home PROJECT_HOME" in root_help


def test_cli_cm_005__intersected_root_values_are_dispatched_by_parameter_name(capsys) -> None:
    _ClassmethodFixture.calls.clear()
    cli = MethodCLI(_ClassmethodFixture, prog="fixture")

    assert cli.run(["--shared", "root-value", "build", "--different", "method-value", "--local", "leaf-value"]) == 0

    assert _ClassmethodFixture.calls == [
        {"shared": "root-value", "different": "method-value", "local": "leaf-value"}
    ]
    assert capsys.readouterr().out == '{"different":"method-value","local":"leaf-value","shared":"root-value"}\n'


def test_cli_cm_006__dml_init_serializes_projected_status_payload(tmp_path: Path, capsys) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    cli = MethodCLI(Dml, prog="dml")

    assert cli.run(["init", "--project-home", str(repo)]) == 0

    expected = Dml(project_home=str(repo)).status()
    assert json.loads(capsys.readouterr().out) == expected


def test_cli_cm_007__dml_clone_serializes_projected_status_payload(tmp_path: Path, capsys) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()

    def _fake_clone(
        cls,
        project_uri: str,
        project_home: str = ".",
        *,
        db_path: str | None = None,
        db_map_size_headroom: int | None = None,
        db_map_size_max: int | None = None,
        default_branch_name: str | None = None,
        remote_root: str | None = None,
        remote_prune_age_seconds: int | None = None,
        remote_fetch_workers: int | None = None,
        user: str | None = None,
        config_home: str | None = None,
    ) -> Dml:
        del project_uri
        del db_path, db_map_size_headroom, db_map_size_max, default_branch_name
        del remote_root, remote_prune_age_seconds, remote_fetch_workers, user, config_home
        return Dml.init(project_home=project_home)

    with patch.object(Dml, "clone", new=classmethod(_fake_clone)):
        cli = MethodCLI(Dml, prog="dml")

        assert cli.run(["clone", "dml://acme/demo#main", "--project-home", str(repo)]) == 0

    expected = Dml(project_home=str(repo)).status()
    assert json.loads(capsys.readouterr().out) == expected
