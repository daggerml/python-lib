from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from daggerml._core import Dml
from daggerml._core.cli import MethodCLI, _external_commands


class Commands:
    def __init__(self, project_home: str | None = None):
        raise AssertionError("external commands must not instantiate the root")

    @classmethod
    def builtin(cls) -> str:
        return "builtin"


@pytest.fixture
def plugin_path(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path))
    return tmp_path


def executable(directory, name, source):
    path = directory / f"daggerml-cli-{name}"
    path.write_text(f"#!{sys.executable}\n{source}\n")
    path.chmod(0o755)
    return path


def test_discovery_filters_names_permissions_and_does_not_execute(plugin_path):
    marker = plugin_path / "executed"
    executable(plugin_path, "foo", f"open({str(marker)!r}, 'w').close()")
    executable(plugin_path, "bad.name", "raise AssertionError")
    executable(plugin_path, "", "raise AssertionError")
    blocked = executable(plugin_path, "blocked", "raise AssertionError")
    blocked.chmod(0o644)
    (plugin_path / "daggerml-cli-directory").mkdir()
    cli = MethodCLI(Commands, external_commands=True)
    assert set(_external_commands()) == {"foo"}
    assert "External command (daggerml-cli-foo)." in cli.parser.format_help()
    assert not marker.exists()


def test_path_precedence_and_missing_directory(plugin_path, tmp_path, monkeypatch):
    second = tmp_path / "second"
    second.mkdir()
    first = executable(plugin_path, "foo", "pass")
    executable(second, "foo", "pass")
    monkeypatch.setenv("PATH", os.pathsep.join([str(tmp_path / "missing"), str(plugin_path), str(second)]))
    assert _external_commands()["foo"] == str(first)


def test_builtin_cannot_be_shadowed(plugin_path, capsys):
    executable(plugin_path, "builtin", "raise AssertionError('shadowed')")
    assert MethodCLI(Commands, external_commands=True).run(["builtin"]) == 0
    assert capsys.readouterr().out == "builtin\n"


def test_extension_receives_unparsed_arguments_and_context(plugin_path, monkeypatch, capfd):
    executable(plugin_path, "foo", """
import json, os, sys
print(json.dumps({'args': sys.argv[1:], 'context': json.loads(os.environ['DML_CLI_CONTEXT']),
                  'project': os.environ['DML_PROJECT_HOME'], 'config': os.environ['DML_CONFIG_HOME'],
                  'headroom': os.environ['DML_DEFAULT_DB_MAP_SIZE_HEADROOM'],
                  'max': os.environ['DML_DEFAULT_DB_MAP_SIZE_MAX'],
                  'branch': os.environ['DML_DEFAULT_BRANCH_NAME'], 'user': os.environ['DML_USER']}))
sys.exit(17)
""")
    monkeypatch.setenv("DML_PROJECT_HOME", "wrong-project")
    monkeypatch.setenv("DML_USER", "inherited-user")
    cli = MethodCLI(Dml, external_commands=True)
    args = ["--help", "--unknown=literal", "; touch not-a-shell", "--", "--project-home", "plugin-value"]
    assert cli.run([
        "--project-home", "chosen-project", "--config-home=chosen-config", "-vv",
        "--db-map-size-headroom", "123", "--db-map-size-max", "456", "--default-branch-name", "chosen-branch",
        "foo", *args,
    ]) == 17
    received = json.loads(capfd.readouterr().out)
    assert received["args"] == args
    assert received["context"] == {
        "version": 1,
        "options": {"project_home": "chosen-project", "config_home": "chosen-config",
                    "db_map_size_headroom": 123, "db_map_size_max": 456, "default_branch_name": "chosen-branch"},
        "verbosity": 2,
    }
    assert received["project"] == "chosen-project"
    assert received["config"] == "chosen-config"
    assert received["headroom"] == "123"
    assert received["max"] == "456"
    assert received["branch"] == "chosen-branch"
    assert received["user"] == "inherited-user"


def test_root_help_does_not_run_extension(plugin_path, capsys):
    executable(plugin_path, "foo", "raise AssertionError('executed')")
    with pytest.raises(SystemExit) as error:
        MethodCLI(Commands, external_commands=True).run(["--help"])
    assert error.value.code == 0
    assert "External command (daggerml-cli-foo)." in capsys.readouterr().out


@pytest.mark.parametrize("args", [["missing"], ["--unknown", "foo"], ["--project-home"], []])
def test_invalid_root_arguments_are_rejected(plugin_path, args):
    executable(plugin_path, "foo", "raise AssertionError('executed')")
    with pytest.raises(SystemExit) as error:
        MethodCLI(Commands, external_commands=True).run(args)
    assert error.value.code == 2


def test_external_commands_are_opt_in(plugin_path):
    executable(plugin_path, "foo", "pass")
    with pytest.raises(SystemExit):
        MethodCLI(Commands).run(["foo"])


def test_core_config_no_longer_exposes_contrib_flag():
    with pytest.raises(SystemExit) as error:
        MethodCLI(Dml).parser.parse_args(["config", "show", "--contrib"])
    assert error.value.code == 2


def test_real_cli_preserves_stdio_and_exit_status(plugin_path):
    executable(plugin_path, "echo", """
import sys
sys.stdout.write(sys.stdin.read())
sys.stderr.write('plugin diagnostic\\n')
sys.exit(23)
""")
    result = subprocess.run(
        [sys.executable, "-c", "from daggerml._core.cli import cli; cli()", "echo"],
        input="stdin payload\n", text=True, capture_output=True, check=False, timeout=30,
    )
    assert result.returncode == 23
    assert result.stdout == "stdin payload\n"
    assert result.stderr == "plugin diagnostic\n"


@pytest.mark.skipif(os.name != "posix", reason="POSIX signal exit status")
def test_signal_exit_is_reported(plugin_path):
    executable(plugin_path, "signal", "import os, signal; os.kill(os.getpid(), signal.SIGTERM)")
    assert MethodCLI(Commands, external_commands=True).run(["signal"]) == 143


def test_core_runtime_has_no_imports_outside_core():
    import ast

    import daggerml._core

    root = Path(daggerml._core.__file__).parent
    for file in root.glob("*.py"):
        for node in ast.walk(ast.parse(file.read_text())):
            modules = [node.module or ""] if isinstance(node, ast.ImportFrom) else (
                [alias.name for alias in node.names] if isinstance(node, ast.Import) else []
            )
            for module in modules:
                assert (
                    not module.startswith("daggerml.")
                    or module == "daggerml._core"
                    or module.startswith("daggerml._core.")
                ), (file, module)
