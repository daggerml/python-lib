"""Unit tests for base CLI functionality."""

import builtins
import json
import os
import re
import sys
from argparse import Namespace
from io import StringIO
from unittest.mock import Mock, call, patch

import pytest
from hypothesis import given, settings

from daggerml._cli.base import (
    apply_help_config,
    build_error_payload,
    build_help_epilog,
    execute_command,
    get_ops_object,
    get_repo_path,
    normalize_error_message,
    output_error,
    output_json,
    parse_ref,
    setup_logging,
)
from daggerml._internal.types import NAMESPACES
from tests.contracts.internal.test_types_contract import _refs


class TestGetRepoPath:
    def test_project_home_arg_provided(self):
        assert get_repo_path("/path/from/arg") == "/path/from/arg"

    @patch.dict(os.environ, {"DML_PROJECT_HOME": "/env/path"})
    def test_env_var_fallback(self):
        assert get_repo_path(None) == "/env/path"

    @patch.dict(os.environ, {}, clear=True)
    def test_no_repo_defaults_to_cwd(self, tmp_path):
        old = os.getcwd()
        os.chdir(tmp_path)
        try:
            assert get_repo_path(None) == str(tmp_path)
        finally:
            os.chdir(old)


def test_get_ops_object_returns_shared_dml_boundary():
    dml = Mock()
    assert get_ops_object(dml, "status") is dml
    assert get_ops_object(dml, "dag") is dml
    assert get_ops_object(dml, "admin") is dml


@given(ref=_refs(*NAMESPACES))
@settings(max_examples=1)
def test_parse_ref_roundtrips_ref_string(ref):
    assert parse_ref(ref.to) == ref


class TestSetupLogging:
    @patch("logging.basicConfig")
    def test_silent_default(self, mock_config):
        setup_logging(0)
        mock_config.assert_called_once_with(level=30, stream=sys.stderr, format="%(levelname)s: %(message)s")


class TestOutputJson:
    @patch("sys.stdout", new_callable=StringIO)
    def test_compact_json(self, mock_stdout):
        output_json({"key": "value", "number": 42})
        assert mock_stdout.getvalue() == '{"key":"value","number":42}\n'


class TestOutputError:
    @patch("sys.stderr", new_callable=StringIO)
    def test_error_with_command(self, mock_stderr):
        output_error(RuntimeError("runtime error"), "show")
        parsed = json.loads(mock_stderr.getvalue().strip())
        assert parsed == {"error": "show: runtime error", "type": "RuntimeError", "command": "show"}


class TestHelpHelpers:
    def test_build_help_epilog_formats_examples(self):
        assert build_help_epilog(["dml branch", "dml admin index list"]) == (
            "Examples:\n  dml branch\n  dml admin index list"
        )

    def test_apply_help_config_sets_description_and_epilog(self):
        from argparse import ArgumentParser

        parser = ArgumentParser(prog="dml")
        apply_help_config(parser, description="Hello", examples=["dml --help"])
        assert parser.description == "Hello"
        assert "Examples:" in (parser.epilog or "")


class TestTopLevelCliParsing:
    def test_cli_accepts_project_home_and_remote_uri(self):
        from daggerml._cli import cli

        old_argv = sys.argv
        sys.argv = ["dml", "--project-home", "/repo", "--remote-uri", "s3://bucket/project", "status"]
        try:
            with patch("daggerml._cli.base.execute_command") as mock_execute:
                cli()
        finally:
            sys.argv = old_argv

        args = mock_execute.call_args.args[0]
        assert args.project_home == "/repo"
        assert args.runtime_remote_uri == "s3://bucket/project"

    def test_cli_keeps_top_level_and_init_remote_uri_distinct(self):
        from daggerml._cli import cli

        old_argv = sys.argv
        sys.argv = ["dml", "--remote-uri", "s3://bucket/runtime", "init", "--remote-uri", "s3://bucket/project", "demo"]
        try:
            with patch("daggerml._cli.base.execute_command") as mock_execute:
                cli()
        finally:
            sys.argv = old_argv

        args = mock_execute.call_args.args[0]
        assert args.runtime_remote_uri == "s3://bucket/runtime"
        assert args.remote_uri == "s3://bucket/project"

    def test_cli_help_lists_new_surface_and_not_legacy_commands(self):
        from daggerml._cli import cli

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.argv = ["dml", "--help"]
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        try:
            with pytest.raises(SystemExit):
                cli()
            out = sys.stdout.getvalue()
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr

        assert "status" in out
        assert "show" in out
        assert "admin" in out
        assert re.search(r"^\s+commit\s", out, re.MULTILINE) is None
        assert re.search(r"^\s+head\s", out, re.MULTILINE) is None
        assert re.search(r"^\s+cache\s", out, re.MULTILINE) is None
        assert re.search(r"^\s+remote\s", out, re.MULTILINE) is None
        assert re.search(r"^\s+contrib\s", out, re.MULTILINE) is None

    def test_cli_rejects_legacy_repo_flag(self):
        from daggerml._cli import cli

        old_argv = sys.argv
        old_stderr = sys.stderr
        sys.argv = ["dml", "--repo", "/repo", "status"]
        sys.stderr = StringIO()
        try:
            with pytest.raises(SystemExit):
                cli()
            err = sys.stderr.getvalue()
            assert "--project-home" in err
            assert "--repo" not in err
        finally:
            sys.argv = old_argv
            sys.stderr = old_stderr


class TestErrorNormalization:
    def test_normalize_error_message_includes_command_context(self):
        msg = normalize_error_message(RuntimeError("boom"), command="dag get")
        assert msg.startswith("dag get:")

    def test_normalize_error_message_invalid_ref_mentions_namespace_format(self):
        msg = normalize_error_message(ValueError("Invalid Ref format"), command="dag get")
        assert "expected namespace:id" in msg

    def test_build_error_payload_preserves_schema_fields(self):
        payload = build_error_payload(ValueError("Invalid Ref format"), command="dag get")
        assert set(payload.keys()) == {"error", "type", "command"}


class TestExecuteCommand:
    @patch("daggerml._cli.base.Dml")
    @patch("daggerml._cli.base.get_repo_path")
    def test_successful_execution(self, mock_get_path, mock_dml):
        mock_get_path.return_value = "/repo/path"
        mock_dml.return_value.config.show.return_value = {"remote": {"uri": "s3://test-bucket/test-prefix"}}
        args = Namespace(
            project_home=None,
            runtime_remote_uri=None,
            op="show",
            func=Mock(return_value={"result": "ok"}),
        )

        with patch("daggerml._cli.base.output_json") as mock_output:
            execute_command(args)

        assert mock_dml.call_args_list == [
            call(project_home="/repo/path", remote_uri=None),
            call(project_home="/repo/path", remote_uri="s3://test-bucket/test-prefix"),
        ]
        args.func.assert_called_once_with(mock_dml.return_value, args)
        mock_output.assert_called_once_with({"result": "ok"})

    @patch("daggerml._cli.base.get_repo_path")
    @patch("daggerml._cli.base.Dml")
    def test_init_executes_without_opening_repo(self, mock_dml, mock_get_path):
        args = Namespace(op="init", func=Mock(return_value={"result": "ok"}))
        with patch("daggerml._cli.base.output_json") as mock_output:
            execute_command(args)

        mock_get_path.assert_not_called()
        mock_dml.assert_not_called()
        args.func.assert_called_once_with(args)
        mock_output.assert_called_once_with({"result": "ok"})

    @patch("daggerml._cli.base.Dml")
    @patch("daggerml._cli.base.get_repo_path")
    @patch("daggerml._cli.base.output_error")
    def test_execution_error(self, mock_error, mock_get_path, mock_dml):
        mock_get_path.return_value = "/repo/path"
        mock_dml.side_effect = FileNotFoundError("repo not found")
        args = Namespace(project_home=None, runtime_remote_uri=None, op="show", func=Mock())

        execute_command(args)

        mock_error.assert_called_once()
        assert isinstance(mock_error.call_args[0][0], FileNotFoundError)
        assert mock_error.call_args[0][1] == "show"


def test_non_remote_commands_do_not_import_boto3(monkeypatch, tmp_path):
    for mod in ["boto3", "daggerml._internal.ops.remote"]:
        if mod in sys.modules:
            monkeypatch.delitem(sys.modules, mod, raising=False)

    orig_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "boto3" or name.startswith("boto3."):
            raise ImportError("blocked boto3 import")
        return orig_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    old_argv = sys.argv
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.argv = ["dml", "--project-home", str(tmp_path), "config", "show"]
    sys.stdout = StringIO()
    sys.stderr = StringIO()
    try:
        from daggerml._cli import cli

        cli()
    finally:
        sys.argv = old_argv
        sys.stdout = old_stdout
        sys.stderr = old_stderr
