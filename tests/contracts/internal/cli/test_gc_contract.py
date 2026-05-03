"""Unit tests for gc CLI functionality."""

from argparse import ArgumentParser, Namespace
from unittest.mock import Mock

import pytest

from daggerml._cli.gc import (
    execute_gc_list_orphans,
    execute_gc_run,
    parse_heads,
    setup_gc_parser,
)
from daggerml._internal._db import Ref
from daggerml._internal.types import DmlRepoError


class TestSetupGcParser:
    """Test gc parser setup."""

    def test_list_orphans_parser_heads_args(self):
        """Test list-orphans --heads argument handling."""
        parser = ArgumentParser()
        setup_gc_parser(parser)
        args = parser.parse_args(["list-orphans"])
        assert args.heads is None
        args = parser.parse_args(["list-orphans", "--heads"])
        assert args.heads == []
        args = parser.parse_args(["list-orphans", "--heads", "main", "feature"])
        assert args.heads == ["main", "feature"]


class TestParseHeads:
    """Test heads parsing helper."""

    def test_parse_heads_none(self):
        """Test parse_heads returns None when omitted."""
        assert parse_heads(Mock(), None) is None

    def test_parse_heads_empty(self):
        """Test parse_heads returns empty list for explicit empty heads."""
        assert parse_heads(Mock(), []) == []

    def test_parse_heads_valid(self):
        """Test parse_heads returns Ref list for valid refs."""
        ops = Mock()
        ops.head.return_value.get_branch_commit.side_effect = [Ref("commit:main"), DmlRepoError("missing")]
        ops.head.return_value.get_index_commit.return_value = Ref("commit:index")
        refs = parse_heads(ops, ["main", "default"])
        assert refs is not None
        assert [ref.to for ref in refs] == ["commit:main", "commit:index"]

    def test_parse_heads_invalid(self):
        """Test parse_heads raises for invalid refs."""
        ops = Mock()
        ops.head.return_value.get_branch_commit.side_effect = DmlRepoError("missing")
        ops.head.return_value.get_index_commit.side_effect = DmlRepoError("missing")
        with pytest.raises(ValueError):
            parse_heads(ops, ["invalid-ref"])


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
        mock_ops.head.return_value.get_branch_commit.return_value = Ref("commit:abc")
        mock_ops.list_orphans.return_value = [Ref("datum-scalar:abc")]

        args = Namespace(heads=["main"])
        result = execute_gc_list_orphans(mock_ops, args)

        mock_ops.list_orphans.assert_called_once_with([Ref("commit:abc")])
        assert result == [Ref("datum-scalar:abc")]
