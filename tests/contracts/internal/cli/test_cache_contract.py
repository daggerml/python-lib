"""Unit tests for cache CLI functionality."""

from argparse import ArgumentParser, Namespace
from unittest.mock import Mock

from daggerml._cli.cache import (
    execute_cache_clear,
    execute_cache_delete,
    execute_cache_get,
    execute_cache_list,
    setup_cache_parser,
)
from daggerml._internal._db import Ref


class TestSetupCacheParser:
    """Test cache parser setup."""

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
        dag_ref1 = Ref("dag:dag1")
        dag_ref2 = Ref("dag:dag2")
        mock_ops.list.return_value = [("argv1", dag_ref1), ("argv2", dag_ref2)]

        args = Namespace(limit=10)
        result = execute_cache_list(mock_ops, args)

        mock_ops.list.assert_called_once_with(10)
        assert result == [
            {"cache_key": "argv1", "dag": "dag:dag1"},
            {"cache_key": "argv2", "dag": "dag:dag2"},
        ]

    def test_execute_cache_clear(self):
        """Test execute_cache_clear handler."""
        mock_ops = Mock()
        mock_ops.clear.return_value = 5

        args = Namespace()
        result = execute_cache_clear(mock_ops, args)

        mock_ops.clear.assert_called_once()
        assert result == 5
