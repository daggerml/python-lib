"""Unit tests for index CLI functionality."""

from argparse import Namespace
from unittest.mock import Mock

from daggerml._cli.index import (
    execute_index_delete,
    execute_index_describe,
    execute_index_list,
)


class TestSetupIndexParser:
    """Test index parser setup."""


class TestExecuteIndexHandlers:
    """Test index handler functions."""

    def test_execute_index_list(self):
        """Test execute_index_list handler."""
        mock_ops = Mock()
        mock_ops.list_indexes.return_value = ["abc", "def"]

        result = execute_index_list(mock_ops, Namespace())

        mock_ops.list_indexes.assert_called_once_with()
        assert result == ["abc", "def"]

    def test_execute_index_describe(self):
        """Test execute_index_describe handler."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        payload = {"id": "abc123", "dag": Ref("dag:xyz")}
        mock_ops.describe.return_value = payload

        args = Namespace(index_id="abc123")
        result = execute_index_describe(mock_ops, args)

        mock_ops.describe.assert_called_once_with("abc123")
        assert result == payload

    def test_execute_index_delete(self):
        """Test execute_index_delete handler."""
        mock_ops = Mock()
        args = Namespace(index_id="abc")
        result = execute_index_delete(mock_ops, args)

        mock_ops.delete.assert_called_once_with("abc")
        assert result is None
