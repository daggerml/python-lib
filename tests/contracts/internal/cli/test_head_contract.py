"""Unit tests for head CLI functionality."""

from argparse import ArgumentParser, Namespace
from unittest.mock import Mock

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
        args = parser.parse_args(["delete", "branch"])
        assert args.subcommand == "delete"
        assert args.branch_name == "branch"


class TestExecuteHeadHandlers:
    """Test head handler functions."""

    def test_execute_head_list(self):
        """Test execute_head_list handler."""
        mock_ops = Mock()
        mock_ops.list_branches.return_value = ["main", "feature"]

        args = Namespace()
        result = execute_head_list(mock_ops, args)

        mock_ops.list_branches.assert_called_once_with()
        assert result == ["main", "feature"]

    def test_execute_head_create_with_from(self):
        """Test execute_head_create handler with --from."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ops.get_branch_commit.return_value = Ref("commit:abc")
        mock_ops.create_branch.return_value = "feature"

        args = Namespace(branch_name="feature", from_head="main")
        result = execute_head_create(mock_ops, args)

        mock_ops.get_branch_commit.assert_called_once_with("main")
        mock_ops.create_branch.assert_called_once_with("feature", Ref("commit:abc"))
        assert result == {"branch": "feature"}

    def test_execute_head_create_without_from(self):
        """Test execute_head_create handler without --from."""

        mock_ops = Mock()
        mock_ops.create_branch.return_value = "branch"

        args = Namespace(branch_name="branch", from_head=None)
        result = execute_head_create(mock_ops, args)

        mock_ops.create_branch.assert_called_once_with("branch", None)
        assert result == {"branch": "branch"}

    def test_execute_head_delete(self):
        """Test execute_head_delete handler."""
        mock_ops = Mock()

        args = Namespace(branch_name="feature")
        assert execute_head_delete(mock_ops, args) is None
        mock_ops.delete_branch.assert_called_once_with("feature")
