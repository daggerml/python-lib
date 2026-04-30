from argparse import Namespace
from unittest.mock import Mock, patch

import pytest

from daggerml._cli.dag import execute_dag_checkout
from daggerml._internal.types import DmlRepoError


@patch("daggerml._cli.dag.parse_ref")
@patch("daggerml._cli.dag.DmlProjectConfig.load")
@patch("daggerml._cli.dag.DmlConfig.resolve")
@patch("daggerml._internal.ops.commit.CommitOps")
def test_execute_dag_checkout_uses_selected_repo_context(
    mock_commit_ops_cls, mock_cfg_resolve, mock_project_load, mock_parse_ref
):
    ops = Mock(path="/repo/from-flag", _db=Mock())
    mock_project_load.return_value = Mock(branch="feature")
    mock_cfg_resolve.return_value = Mock(user="alice")
    commit_ops = mock_commit_ops_cls.return_value
    commit_ops.resolve_revision_ref.return_value = "commit:1"
    commit_ops.checkout_dag.return_value = "commit:2"
    mock_parse_ref.return_value = "head:feature"

    result = execute_dag_checkout(
        ops,
        Namespace(
            revision="origin/main",
            source_name="train",
            target_name=None,
            replace=False,
            head=None,
            branch=None,
            user=None,
        ),
    )

    mock_project_load.assert_called_once_with("/repo/from-flag")
    mock_cfg_resolve.assert_called_once_with(explicit={"project.home": "/repo/from-flag"})
    commit_ops.resolve_revision_ref.assert_called_once_with(
        "origin/main",
        current_branch="feature",
        project_dir="/repo/from-flag",
    )
    mock_parse_ref.assert_called_once_with("head:feature")
    assert result == "commit:2"


@patch("daggerml._cli.dag.parse_ref")
@patch("daggerml._cli.dag.DmlProjectConfig.load")
@patch("daggerml._cli.dag.DmlConfig.resolve")
@patch("daggerml._internal.ops.commit.CommitOps")
def test_execute_dag_checkout_prefers_explicit_user(
    mock_commit_ops_cls, mock_cfg_resolve, mock_project_load, mock_parse_ref
):
    ops = Mock(path="/repo/from-flag", _db=Mock())
    mock_project_load.return_value = Mock(branch="feature")
    mock_cfg_resolve.return_value = Mock(user="resolved")
    commit_ops = mock_commit_ops_cls.return_value
    commit_ops.resolve_revision_ref.return_value = "commit:1"
    commit_ops.checkout_dag.return_value = "commit:2"
    mock_parse_ref.return_value = "head:feature"

    execute_dag_checkout(
        ops,
        Namespace(
            revision="origin/main",
            source_name="train",
            target_name=None,
            replace=False,
            head=None,
            branch=None,
            user="explicit",
        ),
    )

    commit_ops.checkout_dag.assert_called_once_with(
        "head:feature",
        "commit:1",
        "train",
        target_name=None,
        replace=False,
        user="explicit",
    )


@patch("daggerml._cli.dag.DmlProjectConfig.load")
@patch("daggerml._cli.dag.DmlConfig.resolve")
@patch("daggerml._internal.ops.commit.CommitOps")
def test_execute_dag_checkout_requires_user_if_not_resolved(mock_commit_ops_cls, mock_cfg_resolve, mock_project_load):
    ops = Mock(path="/repo/from-flag", _db=Mock())
    mock_project_load.return_value = Mock(branch="feature")
    mock_cfg_resolve.return_value = Mock(user=None)
    _ = mock_commit_ops_cls.return_value

    with pytest.raises(DmlRepoError, match="user is required for dag checkout"):
        execute_dag_checkout(
            ops,
            Namespace(
                revision="origin/main",
                source_name="train",
                target_name=None,
                replace=False,
                head=None,
                branch=None,
                user=None,
            ),
        )
