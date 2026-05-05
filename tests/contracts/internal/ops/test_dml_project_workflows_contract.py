from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from daggerml._internal._db import Ref
from daggerml._internal.config import DmlProjectConfig, init_project_layout
from daggerml._internal.ops import DmlOps
from daggerml._internal.types import DmlRepoError


def test_fetch_pull_push_workflows_delegate_to_remote_ops():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())
    remote_ops = Mock()
    head_ops = Mock()
    head_ops.get_attached_head_branch.return_value = "main"
    head_ops.require_attached_head_branch.return_value = "main"
    with (
        patch.object(
            ops,
            "_load_project_config",
            return_value=SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo"),
        ),
        patch.object(ops, "remote", return_value=remote_ops),
        patch.object(ops, "head", return_value=head_ops),
    ):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        remote_ops.pull_uri_into_branch.return_value = Ref("commit:2")
        remote_ops.push_project_branch.return_value = "projects/alice/demo/heads/main.json"
        remote_ops.push_project_tag.return_value = "projects/alice/demo/tags/v1.0.json"

        fetched = ops.fetch_project("origin", None, s3_client=object())
        pulled = ops.pull_project("origin", None, branch=None, user="alice", s3_client=object())
        pushed = ops.push_project(None, branch=None, create=False, force=False, s3_client=object())
        pushed_tag = ops.push_project("v1.0", branch=None, create=False, force=False, s3_client=object())

    remote_ops.fetch_uri.assert_called_once_with("dml://alice/demo#main")
    remote_ops.pull_uri_into_branch.assert_called_once_with("dml://alice/demo#main", "main", user="alice")
    remote_ops.push_project_branch.assert_called_once_with(
        "dml://alice/demo#main", "main", create=False, force=False
    )
    remote_ops.push_project_tag.assert_called_once_with("dml://alice/demo@v1.0", "main")
    assert fetched == Ref("commit:1")
    assert pulled == Ref("commit:2")
    assert pushed == "projects/alice/demo/heads/main.json"
    assert pushed_tag == "projects/alice/demo/tags/v1.0.json"


def test_project_workflows_create_s3_client_when_not_explicitly_supplied():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())
    remote_ops = Mock()
    s3_client = object()
    head_ops = Mock()
    head_ops.get_attached_head_branch.return_value = "main"
    head_ops.require_attached_head_branch.return_value = "main"
    with (
        patch.object(
            ops,
            "_load_project_config",
            return_value=SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo"),
        ),
        patch.object(ops, "remote", return_value=remote_ops),
        patch.object(ops, "head", return_value=head_ops),
        patch.object(ops, "_create_s3_client", return_value=s3_client) as mock_create_s3,
    ):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        remote_ops.pull_uri_into_branch.return_value = Ref("commit:2")
        remote_ops.push_project_branch.return_value = "projects/alice/demo/heads/main.json"

        fetched = ops.fetch_project("origin", None)
        pulled = ops.pull_project("origin", None, branch=None, user="alice")
        pushed = ops.push_project(None, branch=None, create=False, force=False)

    assert mock_create_s3.call_count == 3
    remote_ops.fetch_uri.assert_called_once_with("dml://alice/demo#main")
    remote_ops.pull_uri_into_branch.assert_called_once_with("dml://alice/demo#main", "main", user="alice")
    remote_ops.push_project_branch.assert_called_once_with(
        "dml://alice/demo#main", "main", create=False, force=False
    )
    assert fetched == Ref("commit:1")
    assert pulled == Ref("commit:2")
    assert pushed == "projects/alice/demo/heads/main.json"


def test_fetch_project_origin_falls_back_to_default_branch_without_attached_head():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())
    remote_ops = Mock()
    with (
        patch.object(
            ops,
            "_load_project_config",
            return_value=SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo"),
        ),
        patch.object(ops, "remote", return_value=remote_ops),
        patch.object(ops, "head", return_value=Mock(get_attached_head_branch=Mock(return_value=None))),
        patch.object(ops, "_default_branch", return_value="main"),
    ):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        fetched = ops.fetch_project("origin", None, s3_client=object())

    remote_ops.fetch_uri.assert_called_once_with("dml://alice/demo#main")
    assert fetched == Ref("commit:1")


def test_push_project_requires_attached_head_or_explicit_branch():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())
    detached_error = DmlRepoError("Current checkout is detached; attach HEAD or pass an explicit branch")
    with (
        patch.object(
            ops,
            "_load_project_config",
            return_value=SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo"),
        ),
        patch.object(
            ops,
            "head",
            return_value=Mock(require_attached_head_branch=Mock(side_effect=detached_error)),
        ),
    ):
        with pytest.raises(DmlRepoError, match="Current checkout is detached"):
            ops.push_project(None, branch=None, create=False, force=False, s3_client=object())


def test_checkout_merge_revert_workflows_delegate_to_commit_ops():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())
    commit_ops = Mock()
    commit_ops.resolve_revision.return_value = SimpleNamespace(commit=Ref("commit:1"), kind="branch", branch="feature")
    commit_ops.resolve_revision_ref.return_value = Ref("commit:2")
    commit_ops.merge_into_head.return_value = Ref("commit:3")
    commit_ops.revert.return_value = Ref("commit:4")
    head_ops = Mock()
    head_ops.require_attached_head_branch.return_value = "main"

    with (
        patch.object(ops, "commit", return_value=commit_ops),
        patch.object(ops, "head", return_value=head_ops),
    ):
        checkout = ops.checkout_project("feature")
        merged = ops.merge_project("origin/main", None, "alice")
        reverted = ops.revert_project("origin/main", None, "alice")

    commit_ops.resolve_revision.assert_called_once_with("feature", project_dir="/repo")
    commit_ops.resolve_revision_ref.assert_any_call("origin/main", project_dir="/repo")
    commit_ops.merge_into_head.assert_called_once_with("main", Ref("commit:2"), "alice")
    commit_ops.revert.assert_called_once_with("main", Ref("commit:2"), "alice")
    head_ops.write_attached_head.assert_called_once_with("feature")
    assert checkout["mode"] == "attached"
    assert merged == Ref("commit:3")
    assert reverted == Ref("commit:4")


def test_checkout_dag_from_revision_delegates_to_commit_ops_with_resolved_defaults():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())
    commit_ops = Mock()
    commit_ops.resolve_revision_ref.return_value = Ref("commit:2")
    commit_ops.checkout_dag.return_value = Ref("commit:3")
    head_ops = Mock()
    head_ops.require_attached_head_branch.return_value = "main"

    with (
        patch.object(ops, "commit", return_value=commit_ops),
        patch.object(ops, "head", return_value=head_ops),
        patch("daggerml._internal.ops.DmlConfig.resolve", return_value=SimpleNamespace(user="alice")),
    ):
        result = ops.checkout_dag_from_revision("origin/main", "train")

    commit_ops.resolve_revision_ref.assert_called_once_with(
        "origin/main",
        project_dir="/repo",
    )
    commit_ops.checkout_dag.assert_called_once_with(
        "main",
        Ref("commit:2"),
        "train",
        target_name=None,
        replace=False,
        user="alice",
    )
    assert result == Ref("commit:3")


def test_checkout_dag_from_revision_requires_user_if_not_resolved():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())

    with (
        patch("daggerml._internal.ops.DmlConfig.resolve", return_value=SimpleNamespace(user=None)),
    ):
        with pytest.raises(DmlRepoError, match="user is required for dag checkout"):
            ops.checkout_dag_from_revision("origin/main", "train")


def test_dmlops_init_recovers_when_config_exists_and_db_missing(tmp_path):
    repo_dir = tmp_path / "repo"
    dml_dir = repo_dir / ".dml"
    dml_dir.mkdir(parents=True)
    (dml_dir / "config.toml").write_text('[project]\nuri = "dml://alice/demo"\n[remote]\nuri = "s3://bucket/prefix"\n')

    open_context = Mock()
    open_ops = Mock()
    open_context.__enter__ = Mock(return_value=open_ops)
    open_context.__exit__ = Mock(return_value=None)
    open_ops.pull_project.return_value = Ref("commit:9")

    with (
        patch("daggerml._internal.ops.DmlOps.create") as mock_create,
        patch("daggerml._internal.ops.DmlOps.open", return_value=open_context) as mock_open,
        patch("daggerml._internal.ops.DmlOps._create_s3_client", return_value=object()),
    ):
        result = DmlOps.init(str(repo_dir), remote_uri="s3://bucket/prefix")

    mock_create.assert_called_once()
    mock_open.assert_called_once_with(str(repo_dir), remote_root="s3://bucket/prefix")
    open_ops.pull_project.assert_called_once()
    assert result["branch"] == "main"


def test_dmlops_init_uses_init_project_layout_for_bootstrap(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    create_context = Mock()
    create_context.__enter__ = Mock(return_value=None)
    create_context.__exit__ = Mock(return_value=None)

    with (
        patch("daggerml._internal.ops.DmlOps.create", return_value=create_context) as mock_create,
        patch("daggerml._internal.ops.init_project_layout", wraps=init_project_layout) as mock_init_layout,
    ):
        result = DmlOps.init(
            str(repo_dir),
            name="demo",
            owner="ignored-owner",
            branch="main",
            remote_uri="s3://bucket/prefix",
            user="alice@example-host",
            no_hooks=True,
        )

    mock_init_layout.assert_called_once()
    init_root, init_cfg = mock_init_layout.call_args.args
    assert init_root == repo_dir
    assert isinstance(init_cfg, DmlProjectConfig)
    assert init_cfg.name == "demo"
    assert init_cfg.owner == "alice"
    assert init_cfg.project_uri == "dml://alice/demo"
    assert init_cfg.remote_uri == "s3://bucket/prefix"
    mock_create.assert_called_once()
    assert result["branch"] == "main"


def test_dmlops_init_rejects_name_and_project_uri_together(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with pytest.raises(
        ValueError,
        match=(
            "NAME and --project-uri are mutually exclusive; provide NAME to derive project URI "
            "or use --project-uri for an explicit URI"
        ),
    ):
        DmlOps.init(
            str(repo_dir),
            name="demo",
            project_uri="dml://alice/demo",
            remote_uri="s3://bucket/prefix",
        )


def test_dmlops_init_name_mode_requires_resolved_user(tmp_path, monkeypatch):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    monkeypatch.setenv("USER", "")
    monkeypatch.setattr("daggerml._internal.config.getuser", lambda: (_ for _ in ()).throw(RuntimeError()))
    monkeypatch.setattr("daggerml._internal.config.gethostname", lambda: (_ for _ in ()).throw(RuntimeError()))

    with pytest.raises(DmlRepoError, match="user is required to derive project URI from NAME"):
        DmlOps.init(
            str(repo_dir),
            name="demo",
            remote_uri="s3://bucket/prefix",
        )


def test_dmlops_init_requires_remote_uri_for_recovery_pull(tmp_path):
    repo_dir = tmp_path / "repo"
    dml_dir = repo_dir / ".dml"
    dml_dir.mkdir(parents=True)
    (dml_dir / "config.toml").write_text('[project]\nuri = "dml://alice/demo"\n')

    with pytest.raises(DmlRepoError, match="remote.uri is required"):
        DmlOps.init(str(repo_dir), remote_uri="")


def test_dmlops_init_requires_existing_project_directory(tmp_path):
    missing = tmp_path / "missing"
    with pytest.raises(FileNotFoundError, match="does not exist"):
        DmlOps.init(str(missing), project_uri="dml://alice/demo", remote_uri="s3://bucket/prefix")
