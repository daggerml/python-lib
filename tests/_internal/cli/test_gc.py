"""Unit and integration tests for gc CLI functionality."""

import json
import shutil
import tempfile
from argparse import ArgumentParser, Namespace
from io import StringIO
from unittest.mock import Mock

import pytest

from daggerml._cli import cli
from daggerml._cli.gc import (
    execute_gc_list_orphans,
    execute_gc_run,
    parse_heads,
    setup_gc_parser,
)
from daggerml._internal import DmlOps
from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.types import ScalarDatum


class TestSetupGcParser:
    """Test gc parser setup."""

    def test_parser_creation(self):
        """Test that parser is created with subcommands."""
        parser = ArgumentParser()
        setup_gc_parser(parser)
        args = parser.parse_args(["run"])
        assert args.method == "run"
        args = parser.parse_args(["list-orphans"])
        assert args.method == "list-orphans"

    def test_list_orphans_parser_heads_args(self):
        """Test list-orphans --heads argument handling."""
        parser = ArgumentParser()
        setup_gc_parser(parser)
        args = parser.parse_args(["list-orphans"])
        assert args.heads is None
        args = parser.parse_args(["list-orphans", "--heads"])
        assert args.heads == []
        args = parser.parse_args(["list-orphans", "--heads", "head:main", "head:feature"])
        assert args.heads == ["head:main", "head:feature"]


class TestParseHeads:
    """Test heads parsing helper."""

    def test_parse_heads_none(self):
        """Test parse_heads returns None when omitted."""
        assert parse_heads(None) is None

    def test_parse_heads_empty(self):
        """Test parse_heads returns empty list for explicit empty heads."""
        assert parse_heads([]) == []

    def test_parse_heads_valid(self):
        """Test parse_heads returns Ref list for valid refs."""
        refs = parse_heads(["head:main", "index:default"])
        assert refs is not None
        assert [ref.to for ref in refs] == ["head:main", "index:default"]

    def test_parse_heads_invalid(self):
        """Test parse_heads raises for invalid refs."""
        with pytest.raises(ValueError):
            parse_heads(["invalid-ref"])


class TestExecuteGcHandlers:
    """Test gc handler functions."""

    def test_execute_gc_run(self):
        """Test execute_gc_run handler."""
        mock_ops = Mock()
        mock_ops.gc.return_value = {"datum-scalar": 1}

        result = execute_gc_run(mock_ops, Namespace())

        mock_ops.gc.assert_called_once_with()
        assert result == {"datum-scalar": 1}

    def test_execute_gc_list_orphans(self):
        """Test execute_gc_list_orphans handler."""
        mock_ops = Mock()
        mock_ops.list_orphans.return_value = [Ref("datum-scalar:abc")]

        args = Namespace(heads=["head:main"])
        result = execute_gc_list_orphans(mock_ops, args)

        mock_ops.list_orphans.assert_called_once_with([Ref("head:main")])
        assert result == [Ref("datum-scalar:abc")]


class TestGcCLIIntegration:
    """Integration tests for gc CLI commands."""

    def setup_method(self):
        """Set up temporary repository for tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.repo_path = self.temp_dir
        self.dml_ops = DmlOps.open(self.repo_path)
        self.base_ops = BaseOps(self.dml_ops._db)

    def teardown_method(self):
        """Clean up temporary repository."""
        self.dml_ops.close()
        shutil.rmtree(self.temp_dir)

    def run_cli_command(self, args):
        """Helper to run CLI command and capture output."""
        import sys

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

    def _put_orphan_datum(self, value):
        with self.base_ops._tx(readonly=False) as txn:
            return txn.put(ScalarDatum(data=value))

    def test_gc_run_returns_stats(self):
        """Test gc run removes orphans and returns stats."""
        self._put_orphan_datum("orphan")
        stdout, stderr = self.run_cli_command(["gc", "run"])
        assert not stderr
        result = json.loads(stdout.strip())
        assert result["datum-scalar"] == 1

    def test_gc_list_orphans_with_heads(self):
        """Test gc list-orphans with explicit heads."""
        self.dml_ops.head().create("main")
        self.dml_ops.head().create("feature", Ref("head:main"))
        orphan_ref = self._put_orphan_datum("orphan")
        stdout, stderr = self.run_cli_command(["gc", "list-orphans", "--heads", "head:main", "head:feature"])
        assert not stderr
        result = json.loads(stdout.strip())
        assert set(result) == {f"Ref({orphan_ref.to})"}
