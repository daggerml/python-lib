"""Unit tests for index CLI functionality."""

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
