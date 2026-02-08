"""Unit and integration tests for cache CLI functionality."""

import json
import os
import tempfile
from argparse import ArgumentParser, Namespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from daggerml._cli.cache import (
    execute_cache_clear,
    execute_cache_delete,
    execute_cache_get,
    execute_cache_list,
    execute_cache_put,
    setup_cache_parser,
)
from daggerml._internal import DmlOps
from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.ops.cache import CacheOps
from daggerml._internal.types import ArgvNode, Dag, DictDatum, KwargvNode, ListDatum, ScalarDatum


@pytest.fixture(autouse=True)
def _use_moto_env(aws_server, s3):
    os.environ.update(aws_server["envvars"])
    os.environ["DML_REMOTE_ROOT"] = "s3://test-bucket/test-prefix"
    os.environ["DML_REMOTE_CACHE"] = f"test-cache-{uuid4().hex}"
    yield


class TestSetupCacheParser:
    """Test cache parser setup."""

    def test_parser_creation(self):
        """Test that parser is created with subcommands."""
        parser = ArgumentParser()
        setup_cache_parser(parser)
        # Test that parsing works for each subcommand
        args = parser.parse_args(["put", "dag:abc"])
        assert args.subcommand == "put"
        args = parser.parse_args(["get", "node-argv:def"])
        assert args.subcommand == "get"
        args = parser.parse_args(["delete", "node-argv:def"])
        assert args.subcommand == "delete"
        args = parser.parse_args(["list"])
        assert args.subcommand == "list"
        args = parser.parse_args(["clear"])
        assert args.subcommand == "clear"

    def test_put_parser_args(self):
        """Test put subcommand arguments."""
        parser = ArgumentParser()
        setup_cache_parser(parser)
        args = parser.parse_args(["put", "dag:abc123"])
        assert args.subcommand == "put"
        assert args.dag_ref == "dag:abc123"

    def test_get_parser_args(self):
        """Test get subcommand arguments."""
        parser = ArgumentParser()
        setup_cache_parser(parser)
        args = parser.parse_args(["get", "node-argv:def456"])
        assert args.subcommand == "get"
        assert args.argv_ref == "node-argv:def456"

    def test_delete_parser_args(self):
        """Test delete subcommand arguments."""
        parser = ArgumentParser()
        setup_cache_parser(parser)
        args = parser.parse_args(["delete", "node-argv:def456"])
        assert args.subcommand == "delete"
        assert args.argv_ref == "node-argv:def456"

    def test_list_parser_args(self):
        """Test list subcommand arguments."""
        parser = ArgumentParser()
        setup_cache_parser(parser)
        args = parser.parse_args(["list", "--limit", "10"])
        assert args.subcommand == "list"
        assert args.limit == 10
        args = parser.parse_args(["list"])
        assert args.limit is None

    def test_clear_parser_args(self):
        """Test clear subcommand arguments."""
        parser = ArgumentParser()
        setup_cache_parser(parser)
        args = parser.parse_args(["clear"])
        assert args.subcommand == "clear"


class TestExecuteCacheHandlers:
    """Test cache handler functions."""

    def test_execute_cache_put(self):
        """Test execute_cache_put handler."""
        mock_ops = Mock()
        mock_ref = Ref("cache:cache123")
        mock_ops.put.return_value = mock_ref

        args = Namespace(dag_ref="dag:abc123")
        result = execute_cache_put(mock_ops, args)

        mock_ops.put.assert_called_once_with(Ref("dag:abc123"))
        assert result == "cache:cache123"

    def test_execute_cache_get_found(self):
        """Test execute_cache_get when entry found."""
        mock_ops = Mock()
        mock_ref = Ref("dag:dag123")
        mock_ops.get.return_value = mock_ref

        args = Namespace(argv_ref="node-argv:def456")
        result = execute_cache_get(mock_ops, args)

        mock_ops.get.assert_called_once_with(Ref("node-argv:def456"))
        assert result == "dag:dag123"

    def test_execute_cache_get_not_found(self):
        """Test execute_cache_get when entry not found."""
        mock_ops = Mock()
        mock_ops.get.return_value = None

        args = Namespace(argv_ref="node-argv:def456")
        result = execute_cache_get(mock_ops, args)

        mock_ops.get.assert_called_once_with(Ref("node-argv:def456"))
        assert result is None

    def test_execute_cache_delete(self):
        """Test execute_cache_delete handler."""
        mock_ops = Mock()
        mock_ops.delete.return_value = True

        args = Namespace(argv_ref="node-argv:def456")
        result = execute_cache_delete(mock_ops, args)

        mock_ops.delete.assert_called_once_with(Ref("node-argv:def456"))
        assert result is True

    def test_execute_cache_list(self):
        """Test execute_cache_list handler."""
        mock_ops = Mock()
        mock_cache_ref1 = Ref("cache:argv1")
        dag_ref1 = Ref("dag:dag1")
        mock_cache_ref2 = Ref("cache:argv2")
        dag_ref2 = Ref("dag:dag2")
        mock_ops.list.return_value = [(mock_cache_ref1, dag_ref1), (mock_cache_ref2, dag_ref2)]

        args = Namespace(limit=10)
        result = execute_cache_list(mock_ops, args)

        mock_ops.list.assert_called_once_with(10)
        assert result == [
            ["cache:argv1", {"dag": "dag:dag1"}],
            ["cache:argv2", {"dag": "dag:dag2"}],
        ]

    def test_execute_cache_clear(self):
        """Test execute_cache_clear handler."""
        mock_ops = Mock()
        mock_ops.clear.return_value = 5

        args = Namespace()
        result = execute_cache_clear(mock_ops, args)

        mock_ops.clear.assert_called_once()
        assert result == 5


class TestCacheCLIIntegration:
    """Integration tests for cache CLI commands."""

    def setup_method(self):
        """Set up temporary repository for tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.repo_path = f"{self.temp_dir}/repo"
        self.dml_ops = DmlOps.create(self.repo_path)
        self.repo = BaseOps(self.dml_ops._db)
        self.ops = CacheOps(self.dml_ops._db)
        self.argv_datum_ids = {}

    def teardown_method(self):
        """Clean up temporary repository."""
        import shutil

        shutil.rmtree(self.temp_dir)

    def _put_datum(self, data):
        with self.repo._tx(readonly=False) as txn:
            return txn.put(ScalarDatum(data=data))

    def _put_argv(self, datum_ref):
        with self.repo._tx(readonly=False) as txn:
            argv_datum_ref = txn.put(ListDatum(data=[datum_ref]))
            argv_node_ref = txn.put(ArgvNode(value=argv_datum_ref))
            kwargv_datum_ref = txn.put(DictDatum(data={}))
            txn.put(KwargvNode(value=kwargv_datum_ref))
            self.argv_datum_ids[argv_node_ref.to] = argv_datum_ref.id()
            return argv_node_ref

    def _cache_ref(self, argv_ref):
        return Ref(f"cache:{self.argv_datum_ids[argv_ref.to]}")

    def _put_dag(self, argv_ref=None):
        with self.repo._tx(readonly=False) as txn:
            dag = Dag(nodes=[argv_ref] if argv_ref else [], names={}, result=None, argv=argv_ref)
            return txn.put(dag)

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

    def test_cache_list_empty_repo(self):
        """Test cache list on empty repository."""
        stdout, stderr = self.run_cli_command(["cache", "list"])
        # Should return empty list
        assert not stderr
        assert json.loads(stdout.strip()) == []

    def test_cache_put_stores_entry_and_returns_cache_ref(self):
        """Test dml cache put dag:abc123 stores cache entry and returns cache ref string."""
        datum_ref = self._put_datum("test")
        argv_ref = self._put_argv(datum_ref)
        dag_ref = self._put_dag(argv_ref)
        stdout, stderr = self.run_cli_command(["cache", "put", dag_ref.to])
        assert not stderr
        result = json.loads(stdout.strip())
        assert isinstance(result, str)
        assert result.startswith("cache:")
        # Verify it was stored by getting it
        stdout2, stderr2 = self.run_cli_command(["cache", "get", argv_ref.to])
        assert not stderr2
        result2 = json.loads(stdout2.strip())
        assert result2 == dag_ref.to

    def test_cache_get_returns_cached_dag_ref_or_null(self):
        """Test dml cache get node-argv:def456 returns cached dag ref string or null."""
        # First put something
        datum_ref = self._put_datum("test")
        argv_ref = self._put_argv(datum_ref)
        dag_ref = self._put_dag(argv_ref)
        self.run_cli_command(["cache", "put", dag_ref.to])
        # Now get
        stdout, stderr = self.run_cli_command(["cache", "get", argv_ref.to])
        assert not stderr
        result = json.loads(stdout.strip())
        assert result == dag_ref.to
        # Get non-existent
        argv_ref2 = self._put_argv(self._put_datum("other"))
        stdout2, stderr2 = self.run_cli_command(["cache", "get", argv_ref2.to])
        assert not stderr2
        result2 = json.loads(stdout2.strip())
        assert result2 is None

    def test_cache_delete_removes_entry_and_returns_true_false(self):
        """Test dml cache delete node-argv:def456 removes cache entry and returns true/false."""
        datum_ref = self._put_datum("test")
        argv_ref = self._put_argv(datum_ref)
        dag_ref = self._put_dag(argv_ref)
        self.run_cli_command(["cache", "put", dag_ref.to])
        # Delete existing
        stdout, stderr = self.run_cli_command(["cache", "delete", argv_ref.to])
        assert not stderr
        result = json.loads(stdout.strip())
        assert result is True
        # Delete again
        stdout2, stderr2 = self.run_cli_command(["cache", "delete", argv_ref.to])
        assert not stderr2
        result2 = json.loads(stdout2.strip())
        assert result2 is False

    def test_cache_list_returns_json_array_of_pairs(self):
        """Test dml cache list --limit 10 returns JSON array of [argv_ref, dag_ref] pairs."""
        # Add some entries
        entries = []
        for i in range(3):
            datum_ref = self._put_datum(f"test{i}")
            argv_ref = self._put_argv(datum_ref)
            dag_ref = self._put_dag(argv_ref)
            self.run_cli_command(["cache", "put", dag_ref.to])
            entries.append([self._cache_ref(argv_ref).to, {"dag": dag_ref.to}])
        # List
        stdout, stderr = self.run_cli_command(["cache", "list", "--limit", "10"])
        assert not stderr
        result = json.loads(stdout.strip())
        assert isinstance(result, list)
        assert len(result) == 3
        # Since order may vary, check set
        normalized_result = {(pair[0], pair[1]["dag"]) for pair in result}
        normalized_entries = {(pair[0], pair[1]["dag"]) for pair in entries}
        assert normalized_result == normalized_entries

    def test_cache_clear_removes_all_and_returns_count(self):
        """Test dml cache clear removes all cache entries and returns count as integer."""
        # Add some
        count = 2
        for i in range(count):
            datum_ref = self._put_datum(f"test{i}")
            argv_ref = self._put_argv(datum_ref)
            dag_ref = self._put_dag(argv_ref)
            self.run_cli_command(["cache", "put", dag_ref.to])
        # Clear
        stdout, stderr = self.run_cli_command(["cache", "clear"])
        assert not stderr
        result = json.loads(stdout.strip())
        assert result == count
        # Check empty
        stdout2, stderr2 = self.run_cli_command(["cache", "list"])
        assert not stderr2
        result2 = json.loads(stdout2.strip())
        assert result2 == []

    def test_invalid_ref_arguments_return_error(self):
        """Test invalid Ref arguments return appropriate error messages."""
        stdout, stderr = self.run_cli_command(["cache", "put", "invalid"])
        assert stderr
        error_data = json.loads(stderr.strip())
        assert "error" in error_data

    def test_wrong_namespace_refs_return_dm_repo_error(self):
        """Test wrong namespace refs (e.g., commit:abc for dag_ref) return DmlRepoError."""
        stdout, stderr = self.run_cli_command(["cache", "put", "commit:abc"])
        assert stderr
        error_data = json.loads(stderr.strip())
        assert "error" in error_data
        # Probably "Invalid reference" or something

    def test_cache_help_displays_correctly(self):
        """Test help text displays correctly for cache subcommands."""
        stdout, stderr = self.run_cli_command(["cache", "--help"])
        assert not stderr
        assert "cache" in stdout.lower()
        assert "Examples:" in stdout
        assert "put" in stdout
        assert "get" in stdout
        assert "delete" in stdout
        assert "list" in stdout
        assert "clear" in stdout

    def test_cache_put_help_includes_ref_format_and_example(self):
        stdout, stderr = self.run_cli_command(["cache", "put", "--help"])
        assert not stderr
        assert "dag:<id>" in stdout
        assert "Examples:" in stdout
        assert "dml cache put" in stdout

    def test_operations_integrate_with_base_cli_framework_and_json_output(self):
        """Test operations integrate properly with base CLI framework and JSON output."""
        # This is covered by the above tests, as they use run_cli_command which checks json output
        datum_ref = self._put_datum("test")
        argv_ref = self._put_argv(datum_ref)
        dag_ref = self._put_dag(argv_ref)
        stdout, stderr = self.run_cli_command(["cache", "put", dag_ref.to])
        assert not stderr
        result = json.loads(stdout.strip())
        assert isinstance(result, str)
