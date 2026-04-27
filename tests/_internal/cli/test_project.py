from argparse import Namespace
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from daggerml._cli.project import ProjectAliasHandlers, execute_clone
from daggerml._internal.types import DmlRepoError


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
def test_revision_aliases_use_selected_repo_path_for_revision_resolution(method_name):
    ops = Mock(path="/repo/from-flag")
    commit_ops = ops.commit.return_value
    commit_ops.resolve_revision_ref.return_value = "commit:1"
    commit_ops.merge_into_head.return_value = "commit:2"
    commit_ops.revert.return_value = "commit:2"

    result = getattr(ProjectAliasHandlers, method_name)(
        ops,
        Namespace(revision="origin/main", head="head:main", user="alice"),
    )

    commit_ops.resolve_revision_ref.assert_called_once_with("origin/main", project_dir="/repo/from-flag")
    assert result == "commit:2"


def test_checkout_alias_reports_attached_mode_for_branch(tmp_path):
    ops = Mock(path=str(tmp_path))
    with patch("daggerml._cli.project._load_project_config") as mock_project, patch(
        "daggerml._cli.project.CommitOps"
    ) as mock_commit_ops:
        mock_project.return_value = SimpleNamespace(
            name="demo",
            owner="alice",
            branch="main",
            remote_uri="s3://bucket/prefix",
        )
        mock_commit_ops.return_value.resolve_revision.return_value = Mock(
            commit="commit:1", kind="branch", branch="main"
        )
        result = ProjectAliasHandlers.checkout(ops, Namespace(revision="main"))

    assert result["mode"] == "attached"
    assert result["head"] == "head:main"


def test_checkout_alias_reports_detached_mode_for_tag(tmp_path):
    ops = Mock(path=str(tmp_path))
    with patch("daggerml._cli.project._load_project_config") as mock_project, patch(
        "daggerml._cli.project.CommitOps"
    ) as mock_commit_ops:
        mock_project.return_value = SimpleNamespace(
            name="demo",
            owner="alice",
            branch="main",
            remote_uri="s3://bucket/prefix",
        )
        mock_commit_ops.return_value.resolve_revision.return_value = Mock(
            commit="commit:2",
            kind="tag",
            branch=None,
        )
        result = ProjectAliasHandlers.checkout(ops, Namespace(revision="v1.0"))

    assert result["mode"] == "detached"
    assert result["head"] is None


def test_clone_rejects_direct_commit_target():
    args = Namespace(uri=f"dml://alice/demo@{'a' * 64}", bucket="bucket", prefix="prefix", branch=None, no_hooks=True)
    with pytest.raises(DmlRepoError, match="direct-commit"):
        execute_clone(args)


@patch("daggerml._cli.project.run_project_hooks")
@patch("daggerml._cli.project._checkout_resolved_target")
@patch("daggerml._cli.project._ops_remote")
@patch("daggerml._cli.project.create_s3_client")
@patch("daggerml._cli.project.require_boto3")
@patch("daggerml._cli.project.DmlOps.create")
@patch("daggerml._cli.project.init_project_layout")
@patch("daggerml._cli.project.RemoteOps.parse_dml_uri")
@patch("daggerml._cli.project.DmlConfig.resolve")
def test_clone_branch_uses_fetch_then_checkout(
    mock_resolve,
    mock_parse_uri,
    mock_init_layout,
    mock_create,
    mock_require_boto3,
    mock_create_s3_client,
    mock_ops_remote,
    mock_checkout,
    mock_hooks,
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)
    mock_resolve.return_value = SimpleNamespace(
        default_branch="main",
        hooks=SimpleNamespace(post_clone=()),
        config_home="/cfg",
    )
    mock_parse_uri.return_value = SimpleNamespace(owner="alice", project="demo", branch="main", tag=None)
    mock_require_boto3.return_value = object()
    mock_create_s3_client.return_value = object()
    ops = Mock()
    ops._db = object()
    mock_create.return_value.__enter__.return_value = ops
    mock_create.return_value.__exit__.return_value = None
    remote_ops = mock_ops_remote.return_value
    mock_checkout.return_value = {
        "head": "head:main",
        "mode": "attached",
        "commit": "commit:1",
        "message": "Checked out branch 'main' (attached)",
    }

    result = execute_clone(
        Namespace(uri="dml://alice/demo#main", bucket="bucket", prefix="prefix", branch=None, no_hooks=False)
    )

    mock_init_layout.assert_called_once()
    remote_ops.fetch_uri.assert_called_once_with("dml://alice/demo#main")
    mock_checkout.assert_called_once_with(ops, revision="main")
    mock_hooks.assert_called_once()
    assert mock_hooks.call_args[0][0] == "post-clone"
    assert result["mode"] == "attached"
    assert result["head"] == "head:main"


@patch("daggerml._cli.project.run_project_hooks")
@patch("daggerml._cli.project._checkout_resolved_target")
@patch("daggerml._cli.project._ops_remote")
@patch("daggerml._cli.project.create_s3_client")
@patch("daggerml._cli.project.require_boto3")
@patch("daggerml._cli.project.DmlOps.create")
@patch("daggerml._cli.project.init_project_layout")
@patch("daggerml._cli.project.RemoteOps.parse_dml_uri")
@patch("daggerml._cli.project.DmlConfig.resolve")
def test_clone_tag_uses_fetch_then_detached_checkout(
    mock_resolve,
    mock_parse_uri,
    mock_init_layout,
    mock_create,
    mock_require_boto3,
    mock_create_s3_client,
    mock_ops_remote,
    mock_checkout,
    mock_hooks,
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)
    mock_resolve.return_value = SimpleNamespace(
        default_branch="main",
        hooks=SimpleNamespace(post_clone=()),
        config_home="/cfg",
    )
    mock_parse_uri.return_value = SimpleNamespace(owner="alice", project="demo", branch=None, tag="v1.0")
    mock_require_boto3.return_value = object()
    mock_create_s3_client.return_value = object()
    ops = Mock()
    ops._db = object()
    mock_create.return_value.__enter__.return_value = ops
    mock_create.return_value.__exit__.return_value = None
    remote_ops = mock_ops_remote.return_value
    mock_checkout.return_value = {
        "head": None,
        "mode": "detached",
        "commit": "commit:2",
        "message": "Checked out 'v1.0' in detached scratch mode",
    }

    result = execute_clone(
        Namespace(uri="dml://alice/demo@v1.0", bucket="bucket", prefix="prefix", branch=None, no_hooks=True)
    )

    mock_init_layout.assert_called_once()
    remote_ops.fetch_uri.assert_called_once_with("dml://alice/demo@v1.0")
    mock_checkout.assert_called_once_with(ops, revision="v1.0")
    assert result["mode"] == "detached"
    assert result["head"] is None
