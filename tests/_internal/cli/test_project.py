from argparse import Namespace
from unittest.mock import Mock, patch

import pytest

from daggerml._cli.project import ProjectAliasHandlers


@pytest.mark.parametrize("method_name", ["fetch", "pull", "push"])
@patch("daggerml._cli.project.create_s3_client")
@patch("daggerml._cli.project.require_boto3")
@patch("daggerml._cli.project._ops_remote")
@patch("daggerml._cli.project.DmlProjectConfig.load")
def test_remote_aliases_use_selected_repo_path(
    mock_load, mock_ops_remote, mock_require_boto3, mock_create_s3_client, method_name
):
    ops = Mock(path="/repo/from-flag")
    project = Mock(branch="main", uri="dml://alice/demo")
    mock_load.return_value = project
    mock_require_boto3.return_value = object()
    mock_create_s3_client.return_value = object()
    remote_ops = mock_ops_remote.return_value
    remote_ops.fetch_uri.return_value = "commit:1"
    remote_ops.pull_uri_into_head.return_value = "commit:1"
    remote_ops.push_project_branch.return_value = "commit:1"
    args = Namespace(remote_or_uri="origin", branch=None, head="head:main", user="alice", create=False, force=False)

    result = getattr(ProjectAliasHandlers, method_name)(ops, args)

    mock_load.assert_called_once_with("/repo/from-flag")
    assert result == "commit:1"


@pytest.mark.parametrize("method_name", ["merge", "revert"])
def test_commitish_aliases_use_selected_repo_path_for_commitish_resolution(method_name):
    ops = Mock(path="/repo/from-flag")
    commit_ops = ops.commit.return_value
    commit_ops.resolve_commitish.return_value = "commit:1"
    commit_ops.merge_into_head.return_value = "commit:2"
    commit_ops.revert.return_value = "commit:2"

    result = getattr(ProjectAliasHandlers, method_name)(
        ops,
        Namespace(commitish="origin/main", head="head:main", user="alice"),
    )

    commit_ops.resolve_commitish.assert_called_once_with("origin/main", project_dir="/repo/from-flag")
    assert result == "commit:2"
