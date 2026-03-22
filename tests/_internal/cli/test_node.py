"""Unit and integration tests for node CLI functionality."""

import json
import shutil
import sys
import tempfile
from argparse import ArgumentParser, Namespace
from io import StringIO
from unittest.mock import Mock

from daggerml._cli import cli
from daggerml._cli.node import (
    execute_node_get,
    execute_node_unroll,
    setup_node_parser,
)
from daggerml._internal._db import Ref
from daggerml._internal.ops import DmlOps
from daggerml._internal.ops.head import HeadOps
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.types import DEFAULT_HEAD


class TestSetupNodeParser:
    """Test node parser setup."""

    def test_parser_creation(self):
        """Test that parser is created with subcommands."""
        parser = ArgumentParser()
        setup_node_parser(parser)
        # Test that parsing works for each subcommand
        args = parser.parse_args(["get", "node:abc123"])
        assert args.method == "get"
        assert args.node == "node:abc123"
        args = parser.parse_args(["unroll", "node:def456"])
        assert args.method == "unroll"
        assert args.node == "node:def456"

    def test_get_parser_args(self):
        """Test get subcommand arguments."""
        parser = ArgumentParser()
        setup_node_parser(parser)
        args = parser.parse_args(["get", "node:abc123"])
        assert args.method == "get"
        assert args.node == "node:abc123"

    def test_unroll_parser_args(self):
        """Test unroll subcommand arguments."""
        parser = ArgumentParser()
        setup_node_parser(parser)
        args = parser.parse_args(["unroll", "node:def456"])
        assert args.method == "unroll"
        assert args.node == "node:def456"


class TestExecuteNodeHandlers:
    """Test node handler functions."""

    def test_execute_node_get(self):
        """Test execute_node_get handler."""
        mock_ops = Mock()
        mock_ops.get.return_value = {"key": "value"}

        args = Namespace(node="node:abc123")
        result = execute_node_get(mock_ops, args)

        mock_ops.get.assert_called_once()
        # The argument should be a Ref object

        call_args = mock_ops.get.call_args[0]
        assert isinstance(call_args[0], Ref)
        assert call_args[0].to == "node:abc123"
        assert result == {"key": "value"}

    def test_execute_node_unroll(self):
        """Test execute_node_unroll handler."""
        mock_ops = Mock()
        mock_ops.unroll.return_value = [{"key": "value"}, "string", 42]

        args = Namespace(node="node:def456")
        result = execute_node_unroll(mock_ops, args)

        mock_ops.unroll.assert_called_once()
        # The argument should be a Ref object

        call_args = mock_ops.unroll.call_args[0]
        assert isinstance(call_args[0], Ref)
        assert call_args[0].to == "node:def456"
        assert result == [{"key": "value"}, "string", 42]


class TestNodeCLIIntegration:
    """Integration tests for node CLI commands."""

    def create_node(self, temp_db, value):
        """Helper to create a node with given value and return its ref."""

        heads = HeadOps(temp_db).list()
        if not heads:
            raise RuntimeError("No heads found")
        head_ref = heads[0]  # Use the first head

        # Create index based on the head
        index_ref = IndexOps(temp_db).create(head=head_ref)
        # Put literal
        node_ref = IndexOps(temp_db).put_literal(index_ref, value)
        # Commit
        IndexOps(temp_db).commit(index_ref, node_ref)
        return node_ref.to

    def create_node_in_repo(self, repo_path, value):
        """Helper to create a node with given value in the repo at repo_path."""

        with DmlOps.open(repo_path) as repo:
            heads = repo.head().list()
            if not heads:
                repo.head().create(DEFAULT_HEAD.to)
            head_ref = heads[0]
            index_ref = repo.index().create(head=head_ref)
            node_ref = repo.index().put_literal(index_ref, value)
            repo.index().commit(index_ref, node_ref)
            return node_ref.to

    def run_cli_command(self, repo_path, args):
        """Helper to run CLI command and capture output."""

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr

        sys.argv = ["dml", "--repo", repo_path] + args
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

    def test_repo_flag_support(self, temp_db_fn):
        """Test --repo flag support."""

        heads = HeadOps(temp_db_fn).list()
        if not heads:
            HeadOps(temp_db_fn).create(DEFAULT_HEAD.to)
        repo_path = temp_db_fn.path
        # Use a different temp repo

        other_temp_dir = tempfile.mkdtemp()
        other_repo_path = f"{other_temp_dir}/repo"
        try:
            # Initialize the other repo

            DmlOps.create(other_repo_path).close()

            # Create node in other repo
            with DmlOps.open(other_repo_path) as repo:
                heads = repo.head().list()
                head_ref = heads[0]
                index_ref = repo.index().create(head=head_ref)
                node_ref = repo.index().put_literal(index_ref, "test")
                repo.index().commit(index_ref, node_ref)

            # Access it with --repo flag
            stdout, stderr = self.run_cli_command(repo_path, ["--repo", other_repo_path, "node", "get", node_ref.to])
            assert not stderr
            result = json.loads(stdout.strip())
            assert result == "test"
        finally:
            shutil.rmtree(other_temp_dir)

    def test_verbose_logging(self, temp_db_fn):
        """Test -v verbose logging shows details to stderr."""

        heads = HeadOps(temp_db_fn).list()
        if not heads:
            HeadOps(temp_db_fn).create(DEFAULT_HEAD.to)
        repo_path = temp_db_fn.path
        node_ref = self.create_node(temp_db_fn, "test")
        stdout, stderr = self.run_cli_command(repo_path, ["-v", "node", "get", node_ref])
        assert not stderr or "error" not in stderr.lower()  # Allow verbose output but no errors
        result = json.loads(stdout.strip())
        assert result == "test"

    def test_help_system_shows_methods(self, temp_db_fn):
        """Test help system shows both get and unroll methods."""

        heads = HeadOps(temp_db_fn).list()
        if not heads:
            HeadOps(temp_db_fn).create(DEFAULT_HEAD.to)
        repo_path = temp_db_fn.path
        stdout, stderr = self.run_cli_command(repo_path, ["node", "--help"])
        assert not stderr
        help_text = stdout
        assert "Examples:" in help_text
        assert "get" in help_text
        assert "unroll" in help_text

    def test_node_get_help_mentions_ref_format(self, temp_db_fn):
        heads = HeadOps(temp_db_fn).list()
        if not heads:
            HeadOps(temp_db_fn).create(DEFAULT_HEAD.to)
        repo_path = temp_db_fn.path

        stdout, stderr = self.run_cli_command(repo_path, ["node", "get", "--help"])
        assert not stderr
        assert "node:<id>" in stdout
        assert "Examples:" in stdout
