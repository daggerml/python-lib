from argparse import Namespace
from unittest.mock import Mock, patch

from daggerml._cli.dag import execute_dag_checkout


@patch("daggerml._cli.dag.parse_ref")
@patch("daggerml._cli.dag.DmlProjectConfig.load")
@patch("daggerml._internal.ops.commit.CommitOps")
def test_execute_dag_checkout_uses_selected_repo_context(mock_commit_ops_cls, mock_project_load, mock_parse_ref):
    ops = Mock(path="/repo/from-flag", _db=Mock())
    mock_project_load.return_value = Mock(branch="feature")
    commit_ops = mock_commit_ops_cls.return_value
    commit_ops.resolve_commitish.return_value = "commit:1"
    commit_ops.checkout_dag.return_value = "commit:2"
    mock_parse_ref.return_value = "head:feature"

    result = execute_dag_checkout(
        ops,
        Namespace(
            commitish="origin/main",
            source_name="train",
            target_name=None,
            replace=False,
            head=None,
            branch=None,
            user="alice",
        ),
    )

    mock_project_load.assert_called_once_with("/repo/from-flag")
    commit_ops.resolve_commitish.assert_called_once_with(
        "origin/main",
        current_branch="feature",
        project_dir="/repo/from-flag",
    )
    mock_parse_ref.assert_called_once_with("head:feature")
    assert result == "commit:2"
