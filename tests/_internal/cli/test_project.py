from argparse import Namespace
from unittest.mock import Mock, patch

import pytest

from daggerml._cli.project import ProjectAliasHandlers, execute_clone
from daggerml._internal._db import Ref
from daggerml._internal.types import DmlRepoError


@patch("daggerml._cli.project.create_s3_client")
@patch("daggerml._cli.project.require_boto3")
def test_fetch_alias_delegates_to_dmlops(mock_require_boto3, mock_create_s3_client):
    ops = Mock()
    args = Namespace(remote_or_uri="origin", branch=None)
    client = object()
    mock_require_boto3.return_value = object()
    mock_create_s3_client.return_value = client
    ops.fetch_project.return_value = "commit:1"

    result = ProjectAliasHandlers.fetch(ops, args)

    ops.fetch_project.assert_called_once_with("origin", None, s3_client=client)
    assert result == "commit:1"


@patch("daggerml._cli.project.create_s3_client")
@patch("daggerml._cli.project.require_boto3")
def test_pull_alias_delegates_to_dmlops(mock_require_boto3, mock_create_s3_client):
    ops = Mock()
    args = Namespace(remote_or_uri="origin", branch=None, head="head:main", user="alice")
    client = object()
    mock_require_boto3.return_value = object()
    mock_create_s3_client.return_value = client
    ops.pull_project.return_value = "commit:1"

    result = ProjectAliasHandlers.pull(ops, args)

    ops.pull_project.assert_called_once_with(
        "origin",
        None,
        head=Ref("head:main"),
        user="alice",
        s3_client=client,
    )
    assert result == "commit:1"


@patch("daggerml._cli.project.create_s3_client")
@patch("daggerml._cli.project.require_boto3")
def test_push_alias_delegates_to_dmlops(mock_require_boto3, mock_create_s3_client):
    ops = Mock()
    args = Namespace(tag=None, head="head:main", create=False, force=False)
    client = object()
    mock_require_boto3.return_value = object()
    mock_create_s3_client.return_value = client
    ops.push_project.return_value = "commit:1"

    result = ProjectAliasHandlers.push(ops, args)

    ops.push_project.assert_called_once_with(
        None,
        head=Ref("head:main"),
        create=False,
        force=False,
        s3_client=client,
    )
    assert result == "commit:1"


@patch("daggerml._cli.project.create_s3_client")
@patch("daggerml._cli.project.require_boto3")
def test_push_alias_with_tag_delegates_to_dmlops(mock_require_boto3, mock_create_s3_client):
    ops = Mock()
    args = Namespace(tag="v1.0", head="head:main", create=False, force=False)
    client = object()
    mock_require_boto3.return_value = object()
    mock_create_s3_client.return_value = client
    ops.push_project.return_value = "projects/alice/demo/tags/v1.0.json"

    result = ProjectAliasHandlers.push(ops, args)

    ops.push_project.assert_called_once_with(
        "v1.0",
        head=Ref("head:main"),
        create=False,
        force=False,
        s3_client=client,
    )
    assert result == "projects/alice/demo/tags/v1.0.json"


def test_checkout_alias_delegates_to_dmlops():
    ops = Mock()
    args = Namespace(revision="main")
    ops.checkout_project.return_value = {"mode": "attached", "head": "head:main"}

    result = ProjectAliasHandlers.checkout(ops, args)

    ops.checkout_project.assert_called_once_with("main")
    assert result["mode"] == "attached"


def test_merge_alias_delegates_to_dmlops():
    ops = Mock()
    args = Namespace(revision="origin/main", head="head:main", user="alice")
    ops.merge_project.return_value = "commit:2"

    result = ProjectAliasHandlers.merge(ops, args)

    ops.merge_project.assert_called_once_with("origin/main", Ref("head:main"), "alice")
    assert result == "commit:2"


def test_revert_alias_delegates_to_dmlops():
    ops = Mock()
    args = Namespace(revision="origin/main", head="head:main", user="alice")
    ops.revert_project.return_value = "commit:2"

    result = ProjectAliasHandlers.revert(ops, args)

    ops.revert_project.assert_called_once_with("origin/main", Ref("head:main"), "alice")
    assert result == "commit:2"


def test_clone_rejects_direct_commit_target():
    args = Namespace(uri=f"dml://alice/demo@{'a' * 64}", bucket="bucket", prefix="prefix", branch=None, no_hooks=True)
    with pytest.raises(DmlRepoError, match="direct-commit"):
        execute_clone(args)


@patch("daggerml._cli.project.create_s3_client")
@patch("daggerml._cli.project.require_boto3")
@patch("daggerml._cli.project.DmlOps.create")
@patch("daggerml._cli.project.DmlConfig.resolve")
@patch("daggerml._cli.project.RemoteOps.parse_dml_uri")
def test_clone_branch_delegates_to_dmlops(
    mock_parse_uri,
    mock_cfg_resolve,
    mock_create,
    mock_require_boto3,
    mock_create_s3_client,
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    mock_parse_uri.return_value = Mock(tag=None, branch="main", project="demo", owner="alice")
    mock_cfg_resolve.return_value = Mock(default_branch="main", hooks=Mock(post_clone=()), config_home="/cfg")
    client = object()
    mock_require_boto3.return_value = object()
    mock_create_s3_client.return_value = client
    clone_context = Mock()
    clone_ops = Mock()
    clone_context.__enter__ = Mock(return_value=clone_ops)
    clone_context.__exit__ = Mock(return_value=None)
    clone_ops.checkout_project.return_value = {
        "head": "head:main",
        "mode": "attached",
        "commit": "commit:1",
        "message": "Checked out branch 'main' (attached)",
    }
    mock_create.return_value = clone_context

    result = execute_clone(
        Namespace(uri="dml://alice/demo#main", bucket="bucket", prefix="prefix", branch=None, no_hooks=False)
    )

    mock_create.assert_called_once_with("demo", remote_root="s3://bucket/prefix", branch="main")
    clone_ops.fetch_project.assert_called_once_with("dml://alice/demo#main", None, s3_client=client)
    clone_ops.checkout_project.assert_called_once_with("main")
    assert result["mode"] == "attached"
    assert result["head"] == "head:main"


@patch("daggerml._cli.project.create_s3_client")
@patch("daggerml._cli.project.require_boto3")
@patch("daggerml._cli.project.DmlOps.create")
@patch("daggerml._cli.project.DmlConfig.resolve")
@patch("daggerml._cli.project.RemoteOps.parse_dml_uri")
def test_clone_tag_delegates_to_dmlops(
    mock_parse_uri,
    mock_cfg_resolve,
    mock_create,
    mock_require_boto3,
    mock_create_s3_client,
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    mock_parse_uri.return_value = Mock(tag="v1.0", branch=None, project="demo", owner="alice")
    mock_cfg_resolve.return_value = Mock(default_branch="main", hooks=Mock(post_clone=()), config_home="/cfg")
    client = object()
    mock_require_boto3.return_value = object()
    mock_create_s3_client.return_value = client
    clone_context = Mock()
    clone_ops = Mock()
    clone_context.__enter__ = Mock(return_value=clone_ops)
    clone_context.__exit__ = Mock(return_value=None)
    clone_ops.checkout_project.return_value = {
        "head": None,
        "mode": "detached",
        "commit": "commit:2",
        "message": "Checked out 'v1.0' in detached scratch mode",
    }
    mock_create.return_value = clone_context

    result = execute_clone(
        Namespace(uri="dml://alice/demo@v1.0", bucket="bucket", prefix="prefix", branch=None, no_hooks=True)
    )

    mock_create.assert_called_once_with("demo", remote_root="s3://bucket/prefix", branch="main")
    clone_ops.fetch_project.assert_called_once_with("dml://alice/demo@v1.0", None, s3_client=client)
    clone_ops.checkout_project.assert_called_once_with("v1.0")
    assert result["mode"] == "detached"
    assert result["head"] is None
