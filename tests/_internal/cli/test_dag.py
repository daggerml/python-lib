"""Unit and integration tests for dag CLI functionality."""

import json
import shutil
import sys
import tempfile
from argparse import ArgumentParser, Namespace
from io import StringIO
from unittest.mock import Mock

from daggerml._cli import cli
from daggerml._cli.dag import (
    execute_dag_describe,
    execute_dag_get_argv,
    execute_dag_get_kwargv,
    execute_dag_get_node,
    execute_dag_list,
    setup_dag_parser,
)
from daggerml._internal import DmlOps
from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.types import ArgvNode, Dag, DictDatum, KwargvNode, ListDatum, LiteralNode, ScalarDatum


class TestSetupDagParser:
    """Test dag parser setup."""

    def test_parser_creation(self):
        """Test that parser is created with subcommands."""
        parser = ArgumentParser()
        setup_dag_parser(parser)
        args = parser.parse_args(["list"])
        assert args.method == "list"
        args = parser.parse_args(["describe", "dag:abc123"])
        assert args.method == "describe"
        args = parser.parse_args(["get-node", "dag:abc123", "node-name"])
        assert args.method == "get-node"
        args = parser.parse_args(["get-argv", "dag:abc123"])
        assert args.method == "get-argv"
        args = parser.parse_args(["get-kwargv", "dag:abc123"])
        assert args.method == "get-kwargv"

    def test_describe_parser_args(self):
        """Test describe subcommand arguments."""
        parser = ArgumentParser()
        setup_dag_parser(parser)
        args = parser.parse_args(["describe", "dag:abc"])
        assert args.dag_ref == "dag:abc"

    def test_get_node_parser_args(self):
        """Test get-node subcommand arguments."""
        parser = ArgumentParser()
        setup_dag_parser(parser)
        args = parser.parse_args(["get-node", "dag:abc", "output"])
        assert args.dag_ref == "dag:abc"
        assert args.name == "output"

    def test_get_argv_parser_args(self):
        """Test get-argv subcommand arguments."""
        parser = ArgumentParser()
        setup_dag_parser(parser)
        args = parser.parse_args(["get-argv", "dag:abc"])
        assert args.dag_ref == "dag:abc"

    def test_get_kwargv_parser_args(self):
        """Test get-kwargv subcommand arguments."""
        parser = ArgumentParser()
        setup_dag_parser(parser)
        args = parser.parse_args(["get-kwargv", "dag:abc"])
        assert args.dag_ref == "dag:abc"


class TestExecuteDagHandlers:
    """Test dag handler functions."""

    def test_execute_dag_list(self):
        """Test execute_dag_list handler."""
        mock_ops = Mock()
        mock_ops.list.return_value = [{"id": "abc"}]

        result = execute_dag_list(mock_ops, Namespace())

        mock_ops.list.assert_called_once_with()
        assert result == [{"id": "abc"}]

    def test_execute_dag_describe(self):
        """Test execute_dag_describe handler."""
        mock_ops = Mock()
        mock_ops.describe.return_value = {"id": "abc"}

        args = Namespace(dag_ref="dag:abc")
        result = execute_dag_describe(mock_ops, args)

        mock_ops.describe.assert_called_once_with(Ref("dag:abc"))
        assert result == {"id": "abc"}

    def test_execute_dag_get_node(self):
        """Test execute_dag_get_node handler."""
        mock_ops = Mock()
        mock_ops.get_node.return_value = Ref("node:out")

        args = Namespace(dag_ref="dag:abc", name="output")
        result = execute_dag_get_node(mock_ops, args)

        mock_ops.get_node.assert_called_once_with(Ref("dag:abc"), "output")
        assert result == "node:out"

    def test_execute_dag_get_argv(self):
        """Test execute_dag_get_argv handler."""
        mock_ops = Mock()
        mock_ops.get_argv.return_value = Ref("node:argv")

        args = Namespace(dag_ref="dag:abc")
        result = execute_dag_get_argv(mock_ops, args)

        mock_ops.get_argv.assert_called_once_with(Ref("dag:abc"))
        assert result == "node:argv"

    def test_execute_dag_get_kwargv(self):
        """Test execute_dag_get_kwargv handler."""
        mock_ops = Mock()
        mock_ops.get_kwargv.return_value = Ref("node:kwargv")

        args = Namespace(dag_ref="dag:abc")
        result = execute_dag_get_kwargv(mock_ops, args)

        mock_ops.get_kwargv.assert_called_once_with(Ref("dag:abc"))
        assert result == "node:kwargv"


class TestDagCLIIntegration:
    """Integration tests for dag CLI commands."""

    def setup_method(self):
        """Set up temporary repository for tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.repo_path = f"{self.temp_dir}/repo"
        self.repo = DmlOps.create(self.repo_path)
        self.base_ops = BaseOps(self.repo._db)

    def teardown_method(self):
        """Clean up temporary repository."""
        self.repo.close()
        shutil.rmtree(self.temp_dir)

    def run_cli_command(self, args):
        """Helper to run CLI command and capture output."""
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

    def create_dag(self):
        """Create a finished DAG in the repo."""
        with self.base_ops._tx(readonly=False) as txn:
            datum_ref = txn.put(ScalarDatum(data=1))
            node_ref = txn.put(LiteralNode(value=datum_ref))
            argv_datum_ref = txn.put(ListDatum(data=[]))
            argv_node_ref = txn.put(ArgvNode(value=argv_datum_ref))
            kwargv_datum_ref = txn.put(DictDatum(data={}))
            kwargv_node_ref = txn.put(KwargvNode(value=kwargv_datum_ref))
            dag_ref = txn.put(
                Dag(
                    nodes=[node_ref, argv_node_ref, kwargv_node_ref],
                    names={"result": node_ref},
                    result=node_ref,
                    argv=argv_node_ref,
                )
            )
        return dag_ref, node_ref, argv_node_ref, kwargv_node_ref

    def test_dag_list(self):
        """Test dag list returns metadata."""
        dag_ref, node_ref, argv_node_ref, _ = self.create_dag()

        stdout, stderr = self.run_cli_command(["dag", "list"])
        assert not stderr
        result = json.loads(stdout.strip())
        assert isinstance(result, list)
        entry = next(item for item in result if item["id"] == dag_ref.id())
        assert entry["result"] == f"Ref({node_ref.to})"
        assert entry["argv"] == f"Ref({argv_node_ref.to})"

    def test_dag_describe(self):
        """Test dag describe returns expected fields."""
        dag_ref, node_ref, argv_node_ref, _ = self.create_dag()

        stdout, stderr = self.run_cli_command(["dag", "describe", dag_ref.to])
        assert not stderr
        result = json.loads(stdout.strip())
        assert result["id"] == dag_ref.id()
        assert result["result"] == f"Ref({node_ref.to})"
        assert result["argv"] == f"Ref({argv_node_ref.to})"

    def test_dag_get_node(self):
        """Test dag get-node returns node reference."""
        dag_ref, node_ref, _, _ = self.create_dag()

        stdout, stderr = self.run_cli_command(["dag", "get-node", dag_ref.to, "result"])
        assert not stderr
        result = json.loads(stdout.strip())
        assert result == node_ref.to

    def test_dag_get_argv(self):
        """Test dag get-argv returns argv reference."""
        dag_ref, _, argv_node_ref, _ = self.create_dag()

        stdout, stderr = self.run_cli_command(["dag", "get-argv", dag_ref.to])
        assert not stderr
        result = json.loads(stdout.strip())
        assert result == argv_node_ref.to

    def test_dag_get_kwargv(self):
        """Test dag get-kwargv returns kwargv reference."""
        dag_ref, _, _, kwargv_node_ref = self.create_dag()

        stdout, stderr = self.run_cli_command(["dag", "get-kwargv", dag_ref.to])
        assert not stderr
        result = json.loads(stdout.strip())
        assert result == kwargv_node_ref.to

    def test_dag_invalid_ref_returns_error(self):
        """Test invalid dag ref returns JSON error."""
        stdout, stderr = self.run_cli_command(["dag", "describe", "invalid-ref"])
        assert stderr
        error_data = json.loads(stderr.strip())
        assert "error" in error_data

    def test_dag_help_shows_methods(self):
        """Test dag help lists method names."""
        stdout, stderr = self.run_cli_command(["dag", "--help"])
        assert not stderr
        assert "Examples:" in stdout
        assert "list" in stdout
        assert "describe" in stdout
        assert "get-node" in stdout
        assert "get-argv" in stdout
        assert "get-kwargv" in stdout

    def test_dag_describe_help_mentions_ref_format(self):
        stdout, stderr = self.run_cli_command(["dag", "describe", "--help"])
        assert not stderr
        assert "dag:<id>" in stdout
        assert "Examples:" in stdout
