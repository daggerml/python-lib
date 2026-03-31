"""Unit tests for commit CLI functionality."""

from argparse import ArgumentParser, Namespace
from unittest.mock import Mock

from daggerml._cli.commit import (
    execute_commit_delete_dag,
    execute_commit_describe,
    execute_commit_get_dag,
    execute_commit_list,
    execute_commit_merge,
    execute_commit_rebase,
    setup_commit_parser,
)


class TestSetupCommitParser:
    """Test commit parser setup."""

    def test_parser_creation(self):
        """Test that parser is created with subcommands."""
        parser = ArgumentParser()
        setup_commit_parser(parser)
        # Test that parsing works for each subcommand
        args = parser.parse_args(["list", "head"])
        assert args.subcommand == "list"
        args = parser.parse_args(["merge", "c1", "c2", "--user", "u"])
        assert args.subcommand == "merge"
        args = parser.parse_args(["rebase", "s", "t", "--user", "u"])
        assert args.subcommand == "rebase"
        args = parser.parse_args(["get-dag", "c", "n"])
        assert args.subcommand == "get-dag"
        args = parser.parse_args(["describe", "c"])
        assert args.subcommand == "describe"
        args = parser.parse_args(["delete-dag", "n", "h", "--user", "u"])
        assert args.subcommand == "delete-dag"

    def test_list_parser_args(self):
        """Test list subcommand arguments."""
        parser = ArgumentParser()
        setup_commit_parser(parser)
        args = parser.parse_args(["list", "head-ref", "--limit", "5"])
        assert args.subcommand == "list"
        assert args.head == "head-ref"
        assert args.limit == 5

    def test_merge_parser_args(self):
        """Test merge subcommand arguments."""
        parser = ArgumentParser()
        setup_commit_parser(parser)
        args = parser.parse_args(["merge", "commit1", "commit2", "--user", "alice"])
        assert args.subcommand == "merge"
        assert args.commit1 == "commit1"
        assert args.commit2 == "commit2"
        assert args.user == "alice"

    def test_rebase_parser_args(self):
        """Test rebase subcommand arguments."""
        parser = ArgumentParser()
        setup_commit_parser(parser)
        args = parser.parse_args(["rebase", "source", "target", "--user", "bob"])
        assert args.subcommand == "rebase"
        assert args.source == "source"
        assert args.target == "target"
        assert args.user == "bob"

    def test_get_dag_parser_args(self):
        """Test get-dag subcommand arguments."""
        parser = ArgumentParser()
        setup_commit_parser(parser)
        args = parser.parse_args(["get-dag", "commit", "my-dag"])
        assert args.subcommand == "get-dag"
        assert args.commit == "commit"
        assert args.name == "my-dag"

    def test_delete_dag_parser_args(self):
        """Test delete-dag subcommand arguments."""
        parser = ArgumentParser()
        setup_commit_parser(parser)
        args = parser.parse_args(["delete-dag", "my-dag", "head", "--user", "alice"])
        assert args.subcommand == "delete-dag"
        assert args.name == "my-dag"
        assert args.head == "head"
        assert args.user == "alice"

    def test_describe_parser_args(self):
        """Test describe subcommand arguments."""
        parser = ArgumentParser()
        setup_commit_parser(parser)
        args = parser.parse_args(["describe", "commit:abc123"])
        assert args.subcommand == "describe"
        assert args.commit == "commit:abc123"


class TestExecuteCommitHandlers:
    """Test commit handler functions."""

    def test_execute_commit_list(self):
        """Test execute_commit_list handler."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ref1 = Mock()
        mock_ref1.__str__ = Mock(return_value="commit:abc123")
        mock_ref2 = Mock()
        mock_ref2.__str__ = Mock(return_value="commit:def456")
        mock_ops.list.return_value = [mock_ref1, mock_ref2]

        args = Namespace(head="head-ref", limit=10)
        result = execute_commit_list(mock_ops, args)

        mock_ops.list.assert_called_once_with(Ref("head-ref"), 10)
        assert result == ["commit:abc123", "commit:def456"]

    def test_execute_commit_merge(self):
        """Test execute_commit_merge handler."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ref = Mock()
        mock_ref.__str__ = Mock(return_value="commit:merge123")
        mock_ops.merge.return_value = mock_ref

        args = Namespace(commit1="commit1", commit2="commit2", user="alice")
        result = execute_commit_merge(mock_ops, args)

        mock_ops.merge.assert_called_once_with(Ref("commit1"), Ref("commit2"), "alice")
        assert result == "commit:merge123"

    def test_execute_commit_rebase(self):
        """Test execute_commit_rebase handler."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ref = Mock()
        mock_ref.__str__ = Mock(return_value="commit:rebase123")
        mock_ops.rebase.return_value = mock_ref

        args = Namespace(source="source", target="target", user="bob")
        result = execute_commit_rebase(mock_ops, args)

        mock_ops.rebase.assert_called_once_with(Ref("source"), Ref("target"), "bob")
        assert result == "commit:rebase123"

    def test_execute_commit_get_dag_found(self):
        """Test execute_commit_get_dag when DAG exists."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ref = Mock()
        mock_ref.__str__ = Mock(return_value="dag:dag123")
        mock_ops.get_dag.return_value = mock_ref

        args = Namespace(commit="commit", name="my-dag")
        result = execute_commit_get_dag(mock_ops, args)

        mock_ops.get_dag.assert_called_once_with(Ref("commit"), "my-dag")
        assert result == "dag:dag123"

    def test_execute_commit_get_dag_not_found(self):
        """Test execute_commit_get_dag when DAG not found."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ops.get_dag.return_value = None

        args = Namespace(commit="commit", name="my-dag")
        result = execute_commit_get_dag(mock_ops, args)

        mock_ops.get_dag.assert_called_once_with(Ref("commit"), "my-dag")
        assert result is None

    def test_execute_commit_delete_dag(self):
        """Test execute_commit_delete_dag handler."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        mock_ref = Mock()
        mock_ref.__str__ = Mock(return_value="commit:delete123")
        mock_ops.delete_dag.return_value = mock_ref

        args = Namespace(name="my-dag", head="head", user="alice")
        result = execute_commit_delete_dag(mock_ops, args)

        mock_ops.delete_dag.assert_called_once_with("my-dag", Ref("head"), "alice")
        assert result == "commit:delete123"

    def test_execute_commit_describe(self):
        """Test execute_commit_describe handler."""
        from daggerml._internal._db import Ref

        mock_ops = Mock()
        payload = {"id": "abc123", "message": "m"}
        mock_ops.describe.return_value = payload

        args = Namespace(commit="commit:abc123")
        result = execute_commit_describe(mock_ops, args)

        mock_ops.describe.assert_called_once_with(Ref("commit:abc123"))
        assert result == payload
