"""Unit tests for head CLI functionality."""

from argparse import ArgumentParser, Namespace
from unittest.mock import Mock

import pytest

from daggerml._cli.head import (
    execute_head_create,
    execute_head_delete,
    execute_head_list,
    setup_head_parser,
)


class TestSetupHeadParser:
    """Test head parser setup."""

    def test_list_parser_args(self):
        """Test list subcommand arguments."""
        parser = ArgumentParser()
        setup_head_parser(parser)
        args = parser.parse_args(["list"])
        assert args.subcommand == "list"

    def test_create_parser_args(self):
        """Test create subcommand arguments."""
        parser = ArgumentParser()
        setup_head_parser(parser)
        args = parser.parse_args(["create", "branch", "--from", "main"])
        assert args.subcommand == "create"
        assert args.branch_name == "branch"
        assert args.from_head == "main"

    def test_create_parser_args_no_from(self):
        """Test create subcommand without --from."""
        parser = ArgumentParser()
        setup_head_parser(parser)
        args = parser.parse_args(["create", "branch"])
        assert args.subcommand == "create"
        assert args.branch_name == "branch"
        assert args.from_head is None

    def test_delete_parser_args(self):
        """Test delete subcommand arguments."""
        parser = ArgumentParser()
        setup_head_parser(parser)
        args = parser.parse_args(["delete", "head:branch"])
        assert args.subcommand == "delete"
        assert args.head_ref == "head:branch"


class TestExecuteHeadHandlers:
    """Test head handler functions."""

    def test_execute_head_list(self):
        """Test execute_head_list handler."""
        mock_ops = Mock()
        mock_ref1 = Mock()
        mock_ref1.__str__ = Mock(return_value="head:main")
        mock_ref2 = Mock()
        mock_ref2.__str__ = Mock(return_value="head:feature")
        mock_ops.list.return_value = [mock_ref1, mock_ref2]

        args = Namespace()
        result = execute_head_list(mock_ops, args)

        mock_ops.list.assert_called_once_with()
        assert result == ["head:main", "head:feature"]

    def test_execute_head_create_with_from(self):
        """Test execute_head_create handler with --from."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ref = Ref("head:feature")
        mock_ops.create.return_value = mock_ref

        args = Namespace(branch_name="feature", from_head="main")
        result = execute_head_create(mock_ops, args)

        # Should parse "main" as "head:main"
        mock_ops.create.assert_called_once_with("feature", Ref("head:main"))
        assert result == {"head": "head:feature"}

    def test_execute_head_create_without_from(self):
        """Test execute_head_create handler without --from."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ref = Ref("head:branch")
        mock_ops.create.return_value = mock_ref

        args = Namespace(branch_name="branch", from_head=None)
        result = execute_head_create(mock_ops, args)

        mock_ops.create.assert_called_once_with("branch", None)
        assert result == {"head": "head:branch"}

    def test_execute_head_delete_invalid_ref(self):
        """Test execute_head_delete handler with invalid ref."""
        mock_ops = Mock()

        args = Namespace(head_ref="commit:abc")
        with pytest.raises(ValueError, match="Head reference must start with 'head:'"):
            execute_head_delete(mock_ops, args)

        mock_ops.delete.assert_not_called()
