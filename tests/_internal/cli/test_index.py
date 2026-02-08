"""Unit and integration tests for index CLI functionality."""

import json
import tempfile
from argparse import ArgumentParser, Namespace
from unittest.mock import Mock

from daggerml._cli.index import (
    execute_index_delete,
    execute_index_describe,
    execute_index_list,
    setup_index_parser,
)


class TestSetupIndexParser:
    """Test index parser setup."""

    def test_parser_creation(self):
        """Test that parser is created with supported subcommands."""
        parser = ArgumentParser()
        setup_index_parser(parser)
        args = parser.parse_args(["list"])
        assert args.method == "list"
        args = parser.parse_args(["describe", "index:abc"])
        assert args.method == "describe"
        args = parser.parse_args(["delete", "index:abc"])
        assert args.method == "delete"


class TestExecuteIndexHandlers:
    """Test index handler functions."""

    def test_execute_index_list(self):
        """Test execute_index_list handler."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ops.list.return_value = [Ref("index:abc"), Ref("index:def")]

        result = execute_index_list(mock_ops, Namespace())

        mock_ops.list.assert_called_once_with()
        assert result == ["index:abc", "index:def"]

    def test_execute_index_describe(self):
        """Test execute_index_describe handler."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        payload = {"id": "abc123", "dag": Ref("dag:xyz")}
        mock_ops.describe.return_value = payload

        args = Namespace(index_ref="index:abc123")
        result = execute_index_describe(mock_ops, args)

        mock_ops.describe.assert_called_once_with(Ref("index:abc123"))
        assert result == payload

    def test_execute_index_delete(self):
        """Test execute_index_delete handler."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        args = Namespace(index_ref="index:abc")
        result = execute_index_delete(mock_ops, args)

        mock_ops.delete.assert_called_once_with(Ref("index:abc"))
        assert result is None

class TestIndexCLIIntegration:
    """Integration tests for index CLI commands."""

    def setup_method(self):
        """Set up temporary repository for tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.repo_path = self.temp_dir

    def teardown_method(self):
        """Clean up temporary repository."""
        import shutil

        shutil.rmtree(self.temp_dir)

    def run_cli_command(self, args):
        """Helper to run CLI command and capture output."""
        import sys
        from io import StringIO

        from daggerml._cli import cli

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr

        sys.argv = ["dml", "--repo", self.repo_path] + args
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        sys.stdout = stdout_capture
        sys.stderr = stderr_capture

        try:
            cli()
            return stdout_capture.getvalue(), stderr_capture.getvalue()
        except SystemExit:
            return stdout_capture.getvalue(), stderr_capture.getvalue()
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def test_index_list_empty_repo(self):
        """Test index list on empty repository."""
        stdout, stderr = self.run_cli_command(["index", "list"])
        assert not stderr
        assert json.loads(stdout.strip()) == []
