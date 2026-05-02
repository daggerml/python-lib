"""Unit tests for node CLI functionality."""

from argparse import ArgumentParser, Namespace
from unittest.mock import Mock

from daggerml._cli.node import (
    execute_node_get,
    execute_node_unroll,
    setup_node_parser,
)
from daggerml._internal._db import Ref


class TestSetupNodeParser:
    """Test node parser setup."""

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
