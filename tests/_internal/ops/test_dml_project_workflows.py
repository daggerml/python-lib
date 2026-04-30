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
    with (
        patch.object(
            ops,
            "_load_project_config",
            return_value=SimpleNamespace(branch="main", uri="dml://alice/demo"),
        ),
        patch.object(ops, "remote", return_value=remote_ops),
    ):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        remote_ops.pull_uri_into_head.return_value = Ref("commit:2")
        remote_ops.push_project_branch.return_value = "projects/alice/demo/heads/main.json"
        remote_ops.push_project_tag.return_value = "projects/alice/demo/tags/v1.0.json"

        fetched = ops.fetch_project("origin", None, s3_client=object())
        pulled = ops.pull_project("origin", None, head=Ref("head:main"), user="alice", s3_client=object())
        pushed = ops.push_project(None, head=Ref("head:main"), create=False, force=False, s3_client=object())
        pushed_tag = ops.push_project("v1.0", head=Ref("head:main"), create=False, force=False, s3_client=object())

    remote_ops.fetch_uri.assert_called_once_with("dml://alice/demo#main")
    remote_ops.pull_uri_into_head.assert_called_once_with("dml://alice/demo#main", Ref("head:main"), user="alice")
    remote_ops.push_project_branch.assert_called_once_with(
        "dml://alice/demo#main", Ref("head:main"), create=False, force=False
    )
    remote_ops.push_project_tag.assert_called_once_with("dml://alice/demo@v1.0", Ref("head:main"))
    assert fetched == Ref("commit:1")
    assert pulled == Ref("commit:2")
    assert pushed == "projects/alice/demo/heads/main.json"
    assert pushed_tag == "projects/alice/demo/tags/v1.0.json"


def test_project_workflows_create_s3_client_when_not_explicitly_supplied():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())
    remote_ops = Mock()
    s3_client = object()
    with (
        patch.object(
            ops,
            "_load_project_config",
            return_value=SimpleNamespace(branch="main", uri="dml://alice/demo"),
        ),
        patch.object(ops, "remote", return_value=remote_ops),
        patch.object(ops, "_create_s3_client", return_value=s3_client) as mock_create_s3,
    ):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        remote_ops.pull_uri_into_head.return_value = Ref("commit:2")
        remote_ops.push_project_branch.return_value = "projects/alice/demo/heads/main.json"

        fetched = ops.fetch_project("origin", None)
        pulled = ops.pull_project("origin", None, head=Ref("head:main"), user="alice")
        pushed = ops.push_project(None, head=Ref("head:main"), create=False, force=False)

    assert mock_create_s3.call_count == 3
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

    with (
        patch.object(ops, "_load_project_config", return_value=project),
        patch.object(ops, "commit", return_value=commit_ops),
        patch("daggerml._internal.ops.DmlProjectConfig.save") as mock_save,
    ):
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


def test_checkout_dag_from_revision_delegates_to_commit_ops_with_resolved_defaults():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())
    project = SimpleNamespace(name="demo", owner="alice", branch="main", remote_uri="s3://bucket/prefix")
    commit_ops = Mock()
    commit_ops.resolve_revision_ref.return_value = Ref("commit:2")
    commit_ops.checkout_dag.return_value = Ref("commit:3")

    with (
        patch.object(ops, "_load_project_config", return_value=project),
        patch.object(ops, "commit", return_value=commit_ops),
        patch("daggerml._internal.ops.DmlConfig.resolve", return_value=SimpleNamespace(user="alice")),
    ):
        result = ops.checkout_dag_from_revision("origin/main", "train")

    commit_ops.resolve_revision_ref.assert_called_once_with(
        "origin/main",
        current_branch="main",
        project_dir="/repo",
    )
    commit_ops.checkout_dag.assert_called_once_with(
        Ref("head:main"),
        Ref("commit:2"),
        "train",
        target_name=None,
        replace=False,
        user="alice",
    )
    assert result == Ref("commit:3")


def test_checkout_dag_from_revision_requires_user_if_not_resolved():
    ops = DmlOps(path="/repo", remote_root="s3://bucket/prefix", _db=Mock())
    project = SimpleNamespace(name="demo", owner="alice", branch="main", remote_uri="s3://bucket/prefix")

    with (
        patch.object(ops, "_load_project_config", return_value=project),
        patch("daggerml._internal.ops.DmlConfig.resolve", return_value=SimpleNamespace(user=None)),
    ):
        with pytest.raises(DmlRepoError, match="user is required for dag checkout"):
            ops.checkout_dag_from_revision("origin/main", "train")


def test_push_lifecycle_uses_configured_uri_and_optional_tag(tmp_path, aws_server):
    import boto3

    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    init_project_layout(
        repo_dir,
        DmlProjectConfig(name="demo", owner="alice", branch="main", remote_uri="s3://test-bucket/test-prefix"),
    )

    s3 = boto3.client("s3", endpoint_url=aws_server["endpoint"])
    try:
        s3.create_bucket(Bucket="test-bucket")
    except Exception:
        pass

    with DmlOps.create(str(repo_dir), remote_root="s3://test-bucket/test-prefix", branch="main"):
        pass

    with DmlOps.open(str(repo_dir), remote_root="s3://test-bucket/test-prefix") as ops:
        branch_ref_path = ops.push_project(None, head=Ref("head:main"), create=True, force=False, s3_client=s3)
        tag_ref_path = ops.push_project("v1.0", head=Ref("head:main"), create=False, force=False, s3_client=s3)

    assert branch_ref_path == "projects/alice/demo/heads/main.json"
    assert tag_ref_path == "projects/alice/demo/tags/v1.0.json"

    objects = s3.list_objects_v2(Bucket="test-bucket", Prefix="test-prefix/dml/refs/projects/alice/demo/")
    keys = {obj["Key"] for obj in objects.get("Contents", [])}
    assert "test-prefix/dml/refs/projects/alice/demo/heads/main.json" in keys
    assert "test-prefix/dml/refs/projects/alice/demo/tags/v1.0.json" in keys


def test_dmlops_init_recovers_when_config_exists_and_db_missing(tmp_path):
    repo_dir = tmp_path / "repo"
    dml_dir = repo_dir / ".dml"
    dml_dir.mkdir(parents=True)
    (dml_dir / "config.toml").write_text('[project]\nuri = "dml://alice/demo#main"\n[remote]\nuri = "s3://bucket/prefix"\n')

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
    assert result["head"] == "head:main"


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
    assert init_cfg.branch == "main"
    assert init_cfg.remote_uri == "s3://bucket/prefix"
    mock_create.assert_called_once()
    assert result["head"] == "head:main"


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
            project_uri="dml://alice/demo#main",
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
    (dml_dir / "config.toml").write_text('[project]\nuri = "dml://alice/demo#main"\n')

    with pytest.raises(DmlRepoError, match="remote.uri is required"):
        DmlOps.init(str(repo_dir), remote_uri="")


def test_dmlops_init_requires_existing_project_directory(tmp_path):
    missing = tmp_path / "missing"
    with pytest.raises(FileNotFoundError, match="does not exist"):
        DmlOps.init(str(missing), project_uri="dml://alice/demo#main", remote_uri="s3://bucket/prefix")
