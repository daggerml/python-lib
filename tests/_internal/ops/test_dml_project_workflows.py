from types import SimpleNamespace
from unittest.mock import ANY, Mock, patch

import pytest

from daggerml._internal._db import Ref
from daggerml._internal.ops import DmlOps
from daggerml._internal.types import DmlRepoError


def test_fetch_pull_push_workflows_delegate_to_remote_ops():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())
    remote_ops = Mock()
    with patch.object(
        ops,
        "_load_project_config",
        return_value=SimpleNamespace(branch="main", uri="dml://alice/demo"),
    ), patch.object(ops, "remote", return_value=remote_ops):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        remote_ops.pull_uri_into_head.return_value = Ref("commit:2")
        remote_ops.push_project_branch.return_value = "projects/alice/demo/heads/main.json"

        fetched = ops.fetch_project("origin", None, s3_client=object())
        pulled = ops.pull_project("origin", None, head=Ref("head:main"), user="alice", s3_client=object())
        pushed = ops.push_project(
            "origin", None, head=Ref("head:main"), create=False, force=False, s3_client=object()
        )

    remote_ops.fetch_uri.assert_called_once_with("dml://alice/demo#main")
    remote_ops.pull_uri_into_head.assert_called_once_with("dml://alice/demo#main", Ref("head:main"), user="alice")
    remote_ops.push_project_branch.assert_called_once_with(
        "dml://alice/demo#main", Ref("head:main"), create=False, force=False
    )
    assert fetched == Ref("commit:1")
    assert pulled == Ref("commit:2")
    assert pushed == "projects/alice/demo/heads/main.json"


def test_checkout_merge_revert_workflows_delegate_to_commit_ops():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())
    project = SimpleNamespace(name="demo", owner="alice", branch="main", remote_uri="s3://bucket/prefix")
    commit_ops = Mock()
    commit_ops.resolve_revision.return_value = SimpleNamespace(commit=Ref("commit:1"), kind="branch", branch="feature")
    commit_ops.resolve_revision_ref.return_value = Ref("commit:2")
    commit_ops.merge_into_head.return_value = Ref("commit:3")
    commit_ops.revert.return_value = Ref("commit:4")

    with patch.object(ops, "_load_project_config", return_value=project), patch.object(
        ops, "commit", return_value=commit_ops
    ), patch("daggerml._internal.ops.DmlProjectConfig.save") as mock_save:
        checkout = ops.checkout_project("feature")
        merged = ops.merge_project("origin/main", Ref("head:main"), "alice")
        reverted = ops.revert_project("origin/main", Ref("head:main"), "alice")

    commit_ops.resolve_revision.assert_called_once_with("feature", current_branch="main", project_dir="/repo")
    commit_ops.resolve_revision_ref.assert_any_call("origin/main", project_dir="/repo")
    commit_ops.merge_into_head.assert_called_once_with(Ref("head:main"), Ref("commit:2"), "alice")
    commit_ops.revert.assert_called_once_with(Ref("head:main"), Ref("commit:2"), "alice")
    mock_save.assert_called_once_with("/repo")
    assert checkout["mode"] == "attached"
    assert merged == Ref("commit:3")
    assert reverted == Ref("commit:4")


def test_clone_project_composes_fetch_checkout_and_hooks(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    clone_context = Mock()
    clone_ops = Mock()
    clone_context.__enter__ = Mock(return_value=clone_ops)
    clone_context.__exit__ = Mock(return_value=None)
    clone_ops.fetch_project.return_value = Ref("commit:1")
    clone_ops.checkout_project.return_value = {
        "head": "head:main",
        "mode": "attached",
        "commit": Ref("commit:1"),
        "message": "Checked out branch 'main' (attached)",
    }

    with patch("daggerml._internal.ops.DmlConfig.resolve") as mock_cfg, patch(
        "daggerml._internal.ops.DmlOps.create", return_value=clone_context
    ) as mock_create, patch("daggerml._internal.ops.run_project_hooks") as mock_hooks:
        mock_cfg.return_value = SimpleNamespace(
            default_branch="main",
            hooks=SimpleNamespace(post_clone=()),
            config_home="/cfg",
        )
        result = DmlOps.clone_project(
            uri="dml://alice/demo#main",
            bucket="bucket",
            prefix="prefix",
            branch=None,
            no_hooks=False,
            s3_client=object(),
        )

    mock_create.assert_called_once()
    clone_ops.fetch_project.assert_called_once_with("dml://alice/demo#main", None, s3_client=ANY)
    clone_ops.checkout_project.assert_called_once_with("main")
    mock_hooks.assert_called_once()
    assert result["head"] == "head:main"
    assert result["mode"] == "attached"


def test_clone_project_rejects_direct_commit_target():
    with pytest.raises(DmlRepoError, match="direct-commit"):
        DmlOps.clone_project(
            uri=f"dml://alice/demo@{'a' * 64}",
            bucket="bucket",
            prefix="prefix",
            branch=None,
            no_hooks=True,
            s3_client=object(),
        )
