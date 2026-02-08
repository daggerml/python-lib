"""Unit and integration tests for head CLI functionality."""

import json
import tempfile
from argparse import ArgumentParser, Namespace
from unittest.mock import Mock

import pytest

from daggerml._cli.head import (
    execute_head_create,
    execute_head_delete,
    execute_head_list,
    setup_head_parser,
)


class TestSetupHeadParser:
    """Test head parser setup."""

    def test_parser_creation(self):
        """Test that parser is created with subcommands."""
        parser = ArgumentParser()
        setup_head_parser(parser)
        # Test that parsing works for each subcommand
        args = parser.parse_args(["list"])
        assert args.subcommand == "list"
        args = parser.parse_args(["create", "branch"])
        assert args.subcommand == "create"
        args = parser.parse_args(["delete", "head:branch"])
        assert args.subcommand == "delete"

    def test_list_parser_args(self):
        """Test list subcommand arguments."""
        parser = ArgumentParser()
        setup_head_parser(parser)
        args = parser.parse_args(["list"])
        assert args.subcommand == "list"

    def test_create_parser_args(self):
        """Test create subcommand arguments."""
        parser = ArgumentParser()
        setup_head_parser(parser)
        args = parser.parse_args(["create", "branch", "--from", "main"])
        assert args.subcommand == "create"
        assert args.branch_name == "branch"
        assert args.from_head == "main"

    def test_create_parser_args_no_from(self):
        """Test create subcommand without --from."""
        parser = ArgumentParser()
        setup_head_parser(parser)
        args = parser.parse_args(["create", "branch"])
        assert args.subcommand == "create"
        assert args.branch_name == "branch"
        assert args.from_head is None

    def test_delete_parser_args(self):
        """Test delete subcommand arguments."""
        parser = ArgumentParser()
        setup_head_parser(parser)
        args = parser.parse_args(["delete", "head:branch"])
        assert args.subcommand == "delete"
        assert args.head_ref == "head:branch"


class TestExecuteHeadHandlers:
    """Test head handler functions."""

    def test_execute_head_list(self):
        """Test execute_head_list handler."""
        mock_ops = Mock()
        mock_ref1 = Mock()
        mock_ref1.__str__ = Mock(return_value="head:main")
        mock_ref2 = Mock()
        mock_ref2.__str__ = Mock(return_value="head:feature")
        mock_ops.list.return_value = [mock_ref1, mock_ref2]

        args = Namespace()
        result = execute_head_list(mock_ops, args)

        mock_ops.list.assert_called_once_with()
        assert result == ["head:main", "head:feature"]

    def test_execute_head_create_with_from(self):
        """Test execute_head_create handler with --from."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ref = Ref("head:feature")
        mock_ops.create.return_value = mock_ref

        args = Namespace(branch_name="feature", from_head="main")
        result = execute_head_create(mock_ops, args)

        # Should parse "main" as "head:main"
        mock_ops.create.assert_called_once_with("feature", Ref("head:main"))
        assert result == {"head": "head:feature"}

    def test_execute_head_create_without_from(self):
        """Test execute_head_create handler without --from."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ref = Ref("head:branch")
        mock_ops.create.return_value = mock_ref

        args = Namespace(branch_name="branch", from_head=None)
        result = execute_head_create(mock_ops, args)

        mock_ops.create.assert_called_once_with("branch", None)
        assert result == {"head": "head:branch"}

    def test_execute_head_delete_invalid_ref(self):
        """Test execute_head_delete handler with invalid ref."""
        mock_ops = Mock()

        args = Namespace(head_ref="commit:abc")
        with pytest.raises(ValueError, match="Head reference must start with 'head:'"):
            execute_head_delete(mock_ops, args)

        mock_ops.delete.assert_not_called()


class TestHeadCLIIntegration:
    """Integration tests for head CLI commands."""

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
            # CLI calls sys.exit on error
            return stdout_capture.getvalue(), stderr_capture.getvalue()
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def test_head_list_empty_repo(self):
        """Test head list on empty repository."""
        stdout, stderr = self.run_cli_command(["head", "list"])
        # For empty repo, should succeed with empty list
        assert not stderr
        assert json.loads(stdout.strip()) == []

    def test_head_create_without_from(self):
        """Test head create without --from."""
        # Create a head without specifying --from
        stdout, stderr = self.run_cli_command(["head", "create", "feature"])
        # Should succeed and return the new head info
        assert not stderr
        result = json.loads(stdout.strip())
        assert result == {"head": "head:feature"}

    def test_head_create_with_from(self):
        """Test head create with --from."""
        # First create a base head
        self.run_cli_command(["head", "create", "main"])
        # Then create from it
        stdout, stderr = self.run_cli_command(["head", "create", "feature", "--from", "main"])
        assert not stderr
        result = json.loads(stdout.strip())
        assert result == {"head": "head:feature"}

    def test_head_delete(self):
        """Test head delete."""
        # Create a head first
        self.run_cli_command(["head", "create", "temp"])
        # Then delete it
        stdout, stderr = self.run_cli_command(["head", "delete", "head:temp"])
        # Should succeed with null output
        assert not stderr
        assert json.loads(stdout.strip()) is None

    def test_head_delete_invalid(self):
        """Test head delete with invalid ref."""
        stdout, stderr = self.run_cli_command(["head", "delete", "invalid:ref"])
        # Should fail
        assert stderr
        error_data = json.loads(stderr.strip())
        assert "error" in error_data
