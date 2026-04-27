"""Unit tests for base CLI functionality."""

import json
import os
import sys
from argparse import Namespace
from io import StringIO
from unittest.mock import Mock, patch

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
from tests._internal.test_types import _refs


class TestGetRepoPath:
    """Test repository path resolution."""

    def test_repo_arg_provided(self):
        """Test --repo flag takes priority."""
        assert get_repo_path("/path/from/arg") == "/path/from/arg"

    @patch.dict(os.environ, {"DML_PROJECT_HOME": "/env/path"})
    def test_env_var_fallback(self):
        """Test DML_PROJECT_HOME env var when no --repo."""
        assert get_repo_path(None) == "/env/path"

    @patch.dict(os.environ, {"DML_PROJECT_HOME": "/env/repo"})
    def test_dml_repo_resolution(self):
        """Test DML_PROJECT_HOME resolution."""
        assert get_repo_path(None) == "/env/repo"

    @patch.dict(os.environ, {}, clear=True)
    def test_cwd_fallback(self, tmp_path):
        """Test current working directory fallback."""
        with patch("os.getcwd", return_value=str(tmp_path)):
            assert get_repo_path(None) == str(tmp_path)


class TestGetOpsObject:
    """Test ops object mapping."""

    def test_commit_ops(self):
        """Test mapping for commit operations."""
        ops = Mock()
        result = get_ops_object(ops, "commit")
        # get_ops_object should call the callable method and return result
        assert result == ops.commit.return_value

    def test_head_ops(self):
        """Test mapping for head operations."""
        ops = Mock()
        result = get_ops_object(ops, "head")
        assert result == ops.head.return_value

    def test_index_ops(self):
        """Test mapping for index operations."""
        ops = Mock()
        result = get_ops_object(ops, "index")
        assert result == ops.index.return_value

    def test_dag_ops(self):
        """Test mapping for dag operations."""
        ops = Mock()
        result = get_ops_object(ops, "dag")
        assert result == ops.dag.return_value

    def test_node_ops(self):
        """Test mapping for node operations."""
        ops = Mock()
        result = get_ops_object(ops, "node")
        assert result == ops.node.return_value

    def test_cache_ops(self):
        """Test mapping for cache operations."""
        ops = Mock()
        result = get_ops_object(ops, "cache")
        assert result == ops.cache.return_value

    def test_gc_ops(self):
        """Test mapping for gc operations."""
        ops = Mock()
        result = get_ops_object(ops, "gc")
        assert result == ops.gc.return_value

    def test_non_callable_attribute(self):
        """Test that non-callable attributes are returned as-is."""
        ops = Mock()
        # Create a non-callable attribute
        ops.some_attr = "not_callable"
        result = get_ops_object(ops, "some_attr")
        assert result == "not_callable"


class TestParseRef:
    """Test ref parsing (placeholder)."""

    @given(ref=_refs(*NAMESPACES))
    @settings(max_examples=1)
    def test_basic_ref(self, ref):
        """Test basic ref string passthrough."""
        # ref = "some-ref-string"
        assert parse_ref(ref.to) == ref


class TestSetupLogging:
    """Test logging configuration."""

    @patch("logging.basicConfig")
    def test_silent_default(self, mock_config):
        """Test default silent logging."""
        setup_logging(0)
        mock_config.assert_called_once_with(
            level=30,  # WARNING
            stream=sys.stderr,
            format="%(levelname)s: %(message)s",
        )

    @patch("logging.basicConfig")
    def test_info_level(self, mock_config):
        """Test INFO level for -v."""
        setup_logging(1)
        mock_config.assert_called_once_with(
            level=20,  # INFO
            stream=sys.stderr,
            format="%(levelname)s: %(message)s",
        )

    @patch("logging.basicConfig")
    def test_debug_level(self, mock_config):
        """Test DEBUG level for -vv and above."""
        setup_logging(2)
        mock_config.assert_called_once_with(
            level=10,  # DEBUG
            stream=sys.stderr,
            format="%(levelname)s: %(message)s",
        )


class TestOutputJson:
    """Test JSON output to stdout."""

    @patch("sys.stdout", new_callable=StringIO)
    def test_compact_json(self, mock_stdout):
        """Test compact JSON output."""
        data = {"key": "value", "number": 42}
        output_json(data)
        output = mock_stdout.getvalue()
        assert output == '{"key":"value","number":42}\n'

    @patch("sys.stdout", new_callable=StringIO)
    def test_valid_json(self, mock_stdout):
        """Test output is valid JSON."""
        data = [1, 2, {"nested": True}]
        output_json(data)
        output = mock_stdout.getvalue().strip()
        parsed = json.loads(output)
        assert parsed == data


class TestOutputError:
    """Test error output to stderr."""

    @patch("sys.stderr", new_callable=StringIO)
    def test_error_dict_structure(self, mock_stderr):
        """Test error output structure."""
        error = ValueError("test error")
        output_error(error)
        output = mock_stderr.getvalue()
        parsed = json.loads(output.strip())
        assert parsed == {"error": "test error", "type": "ValueError"}

    @patch("sys.stderr", new_callable=StringIO)
    def test_error_with_command(self, mock_stderr):
        """Test error output with command."""
        error = RuntimeError("runtime error")
        output_error(error, "commit")
        output = mock_stderr.getvalue()
        parsed = json.loads(output.strip())
        assert parsed == {"error": "commit: runtime error", "type": "RuntimeError", "command": "commit"}


class TestHelpHelpers:
    def test_build_help_epilog_formats_examples(self):
        assert build_help_epilog(["dml head list", "dml cache list --limit 10"]) == (
            "Examples:\n  dml head list\n  dml cache list --limit 10"
        )

    def test_apply_help_config_sets_description_and_epilog(self):
        from argparse import ArgumentParser

        parser = ArgumentParser(prog="dml")
        apply_help_config(parser, description="Hello", examples=["dml --help"])
        assert parser.description == "Hello"
        assert "Examples:" in (parser.epilog or "")


class TestErrorNormalization:
    def test_normalize_error_message_includes_command_context(self):
        msg = normalize_error_message(RuntimeError("boom"), command="cache put")
        assert msg.startswith("cache put:")

    def test_normalize_error_message_invalid_ref_mentions_namespace_format(self):
        msg = normalize_error_message(ValueError("Invalid Ref format"), command="dag describe")
        assert "expected namespace:id" in msg

    def test_normalize_error_message_repo_path_has_recovery_hint(self):
        msg = normalize_error_message(FileNotFoundError("/nope"), command="head list")
        assert "--repo" in msg or "DML_PROJECT_HOME" in msg

    def test_build_error_payload_preserves_schema_fields(self):
        payload = build_error_payload(ValueError("Invalid Ref format"), command="node get")
        assert set(payload.keys()) == {"error", "type", "command"}
        assert payload["type"] == "ValueError"


class TestExecuteCommand:
    """Test command execution."""

    @patch("daggerml._cli.base.DmlOps.open")
    @patch("daggerml._cli.base.get_repo_path")
    def test_successful_execution(self, mock_get_path, mock_open):
        """Test successful command execution."""
        mock_get_path.return_value = "/repo/path"
        mock_ops = Mock()
        mock_ops_obj = Mock()
        mock_ops.commit = mock_ops_obj
        mock_open.return_value.__enter__.return_value = mock_ops
        mock_open.return_value.__exit__.return_value = None

        args = Namespace(repo=None, op="commit", func=Mock(return_value={"result": "ok"}))

        with patch("daggerml._cli.base.output_json") as mock_output:
            execute_command(args)

        mock_get_path.assert_called_once_with(None)
        mock_open.assert_called_once()
        open_call = mock_open.call_args
        assert open_call.args[0] == "/repo/path"
        assert open_call.kwargs.get("remote_root") == "s3://test-bucket/test-prefix"
        # get_ops_object now calls the method, so it receives the result of mock_ops.commit()
        # which is a new Mock object (since calling Mock returns a new Mock)
        args.func.assert_called_once()
        # Get the actual arguments passed to args.func
        call_args = args.func.call_args
        # The first arg should be the result of calling mock_ops.commit()
        assert call_args[0][0] == mock_ops_obj.return_value
        mock_output.assert_called_once_with({"result": "ok"})

    @patch("daggerml._cli.base.DmlOps.open")
    @patch("daggerml._cli.base.get_repo_path")
    @patch.dict(os.environ, {"DML_REMOTE_URI": "s3://bucket/project"})
    def test_execution_passes_remote_context_to_dmlops(self, mock_get_path, mock_open):
        """Test execute_command forwards resolved remote context into DmlOps."""
        mock_get_path.return_value = "/repo/path"
        mock_ops = Mock()
        mock_ops_obj = Mock()
        mock_ops.commit = mock_ops_obj
        mock_open.return_value.__enter__.return_value = mock_ops
        mock_open.return_value.__exit__.return_value = None

        args = Namespace(repo=None, op="commit", func=Mock(return_value={"result": "ok"}))

        with patch("daggerml._cli.base.output_json"):
            execute_command(args)

        mock_open.assert_called_once_with(
            "/repo/path",
            remote_root="s3://bucket/project",
        )

    @patch("daggerml._cli.base.DmlOps.open")
    @patch("daggerml._cli.base.get_repo_path")
    @patch.dict(os.environ, {}, clear=True)
    def test_execution_allows_empty_remote_context(self, mock_get_path, mock_open):
        mock_get_path.return_value = "/repo/path"
        mock_ops = Mock()
        mock_ops.commit = Mock()
        mock_open.return_value.__enter__.return_value = mock_ops
        mock_open.return_value.__exit__.return_value = None

        args = Namespace(repo=None, op="commit", func=Mock(return_value={"result": "ok"}))

        with patch("daggerml._cli.base.output_json"):
            execute_command(args)

        mock_open.assert_called_once_with("/repo/path", remote_root="")

    @patch("daggerml._cli.base.get_repo_path")
    @patch("daggerml._cli.base.DmlOps.open")
    def test_init_executes_without_opening_repo(self, mock_open, mock_get_path):
        """Test top-level init command bypasses DmlOps.open/get_repo_path."""
        args = Namespace(op="init", func=Mock(return_value={"result": "ok"}))
        with patch("daggerml._cli.base.output_json") as mock_output:
            execute_command(args)

        mock_get_path.assert_not_called()
        mock_open.assert_not_called()
        args.func.assert_called_once_with(args)
        mock_output.assert_called_once_with({"result": "ok"})

    @patch("daggerml._cli.base.DmlOps.open")
    @patch("daggerml._cli.base.get_repo_path")
    @patch("daggerml._cli.base.output_error")
    def test_execution_error(self, mock_error, mock_get_path, mock_open):
        """Test error handling in execution."""
        mock_get_path.return_value = "/repo/path"
        mock_open.side_effect = FileNotFoundError("repo not found")

        args = Namespace(repo=None, op="commit", func=Mock())

        execute_command(args)

        mock_error.assert_called_once()
        error_arg = mock_error.call_args[0][0]
        assert isinstance(error_arg, FileNotFoundError)
        assert mock_error.call_args[0][1] == "commit"


class TestTopLevelHelp:
    def test_dml_help_lists_operations(self):
        from daggerml._cli import cli

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.argv = ["dml", "--help"]
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        try:
            try:
                cli()
            except SystemExit:
                pass
            out = sys.stdout.getvalue()
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr

        for op in ["init", "commit", "head", "index", "cache", "dag", "node", "remote", "gc", "contrib"]:
            assert op in out
        assert "--remote-root" in out
