"""Unit tests for dag CLI functionality."""

from argparse import ArgumentParser, Namespace
from unittest.mock import Mock

from daggerml._cli.dag import (
    execute_dag_describe,
    execute_dag_get_argv,
    execute_dag_get_kwargv,
    execute_dag_get_node,
    execute_dag_list,
    setup_dag_parser,
)
from daggerml._internal._db import Ref


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
