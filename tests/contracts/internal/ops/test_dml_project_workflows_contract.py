from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock, PropertyMock, patch

import pytest

import daggerml._internal.dml as dml_module
from daggerml._internal._db import Ref
from daggerml._internal.config import DmlProjectConfig, init_project_layout
from daggerml._internal.dml import Dml
from daggerml._internal.types import DmlRepoError


@contextmanager
def _opened_ops(remote_ops):
    runtime_ops = Mock()
    runtime_ops.remote.return_value = remote_ops
    yield runtime_ops


def test_fetch_pull_push_workflows_delegate_to_remote_ops():
    ops = Dml(project_home="/repo", remote_uri="s3://bucket/prefix")
    remote_ops = Mock()
    head_ops = Mock()
    head_ops.get_attached_head_branch.return_value = "main"
    head_ops.require_attached_head_branch.return_value = "main"
    project_cfg = SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo")
    with (
        patch("daggerml._internal.dml_context.DmlProjectConfig.load", return_value=project_cfg),
        patch.object(dml_module, "head_ops", return_value=head_ops),
        patch.object(dml_module, "with_ops", side_effect=lambda _dml: _opened_ops(remote_ops)),
    ):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        remote_ops.pull_uri_into_branch.return_value = Ref("commit:2")
        remote_ops.push_project_branch.return_value = "projects/alice/demo/heads/main.json"
        remote_ops.push_project_tag.return_value = "projects/alice/demo/tags/v1.0.json"

        fetched = ops.fetch("origin", None, s3_client=object())
        pulled = ops.pull("origin", None, branch=None, user="alice", s3_client=object())
        pushed = ops.push(None, branch=None, create=False, force=False, s3_client=object())
        pushed_tag = ops.push("v1.0", branch=None, create=False, force=False, s3_client=object())

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
    ops = Dml(project_home="/repo", remote_uri="s3://bucket/prefix")
    remote_ops = Mock()
    s3_client = object()
    head_ops = Mock()
    head_ops.get_attached_head_branch.return_value = "main"
    head_ops.require_attached_head_branch.return_value = "main"
    project_cfg = SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo")
    with (
        patch("daggerml._internal.dml_context.DmlProjectConfig.load", return_value=project_cfg),
        patch.object(dml_module, "head_ops", return_value=head_ops),
        patch.object(dml_module, "create_s3_client", return_value=s3_client) as mock_create_s3,
        patch.object(dml_module, "with_ops", side_effect=lambda _dml: _opened_ops(remote_ops)),
    ):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        remote_ops.pull_uri_into_branch.return_value = Ref("commit:2")
        remote_ops.push_project_branch.return_value = "projects/alice/demo/heads/main.json"

        fetched = ops.fetch("origin", None)
        pulled = ops.pull("origin", None, branch=None, user="alice")
        pushed = ops.push(None, branch=None, create=False, force=False)

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
    ops = Dml(project_home="/repo", remote_uri="s3://bucket/prefix")
    remote_ops = Mock()
    project_cfg = SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo")
    detached_head_ops = Mock(get_attached_head_branch=Mock(return_value=None))
    with (
        patch("daggerml._internal.dml_context.DmlProjectConfig.load", return_value=project_cfg),
        patch.object(dml_module, "head_ops", return_value=detached_head_ops),
        patch.object(dml_module, "with_ops", side_effect=lambda _dml: _opened_ops(remote_ops)),
    ):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        fetched = ops.fetch("origin", None, s3_client=object())

    remote_ops.fetch_uri.assert_called_once_with("dml://alice/demo#main")
    assert fetched == Ref("commit:1")


def test_push_project_requires_attached_head_or_explicit_branch():
    ops = Dml(project_home="/repo", remote_uri="s3://bucket/prefix")
    detached_error = DmlRepoError("Current checkout is detached; attach HEAD or pass an explicit branch")
    project_cfg = SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo")
    detached_head_ops = Mock(require_attached_head_branch=Mock(side_effect=detached_error))
    with (
        patch("daggerml._internal.dml.load_project_config", return_value=project_cfg),
        patch.object(dml_module, "head_ops", return_value=detached_head_ops),
    ):
        with pytest.raises(DmlRepoError, match="Current checkout is detached"):
            ops.push(None, branch=None, create=False, force=False, s3_client=object())


def test_checkout_merge_revert_workflows_delegate_to_commit_ops():
    ops = Dml(project_home="/repo", remote_uri="s3://bucket/prefix")
    commit_ops = Mock()
    commit_ops.merge_into_head.return_value = Ref("commit:3")
    commit_ops.revert.return_value = Ref("commit:4")
    head_ops = Mock()
    head_ops.require_attached_head_branch.return_value = "main"

    with (
        patch.object(dml_module, "commit_ops", return_value=commit_ops),
        patch.object(dml_module, "head_ops", return_value=head_ops),
        patch.object(
            dml_module,
            "resolve_dml_revision",
            return_value=SimpleNamespace(commit=Ref("commit:1"), kind="branch", branch="feature"),
        ),
        patch.object(dml_module, "resolve_dml_revision_ref", return_value=Ref("commit:2")),
    ):
        checkout = ops.checkout("feature")
        merged = ops.merge("origin/main", None, "alice")
        reverted = ops.revert("origin/main", None, "alice")

    commit_ops.merge_into_head.assert_called_once_with("main", Ref("commit:2"), "alice")
    commit_ops.revert.assert_called_once_with("main", Ref("commit:2"), "alice")
    head_ops.write_attached_head.assert_called_once_with("feature")
    assert checkout["mode"] == "attached"
    assert merged == Ref("commit:3")
    assert reverted == Ref("commit:4")


def test_dag_checkout_delegates_to_commit_ops_with_resolved_defaults():
    ops = Dml(project_home="/repo", remote_uri="s3://bucket/prefix", user="alice")
    commit_ops = Mock()
    commit_ops.checkout_dag.return_value = Ref("commit:3")
    head_ops = Mock()
    head_ops.require_attached_head_branch.return_value = "main"

    with (
        patch.object(dml_module, "commit_ops", return_value=commit_ops),
        patch.object(dml_module, "head_ops", return_value=head_ops),
        patch.object(dml_module, "resolve_dml_revision_ref", return_value=Ref("commit:2")),
    ):
        result = ops.dag.checkout("origin/main", "train")

    commit_ops.checkout_dag.assert_called_once_with(
        "main",
        Ref("commit:2"),
        "train",
        target_name=None,
        replace=False,
        user="alice",
    )
    assert result == Ref("commit:3")


def test_dag_checkout_requires_user_if_not_resolved():
    ops = Dml(project_home="/repo", remote_uri="s3://bucket/prefix", user=None)
    with (
        patch.object(type(ops._context), "user", new_callable=PropertyMock, return_value=None),
        patch.object(
            dml_module, "head_ops", return_value=Mock(require_attached_head_branch=Mock(return_value="main"))
        ),
        patch.object(dml_module, "resolve_dml_revision_ref", return_value=Ref("commit:2")),
    ):
        with pytest.raises(DmlRepoError, match="user is required for dag checkout"):
            ops.dag.checkout("origin/main", "train")


def test_dag_describe_node_resolves_named_node_with_revision_context():
    ops = Dml(project_home="/repo", remote_uri="s3://bucket/prefix")
    revision = SimpleNamespace(commit=Ref("commit:2"), kind="branch", branch="main", tag=None)
    node_ops = Mock(describe=Mock(return_value={"id": "4", "ref": Ref("node:4"), "type": "LiteralNode"}))

    with (
        patch(
            "daggerml._internal.dml.resolve_node_ref",
            return_value=SimpleNamespace(ref=Ref("node:4"), dag_selector="train", revision=revision),
        ),
        patch.object(dml_module, "node_ops", return_value=node_ops),
    ):
        result = ops.dag.describe_node("result", dag_selector="train", revision="HEAD")

    node_ops.describe.assert_called_once_with(Ref("node:4"))
    assert result == {
        "selector": "result",
        "dag_selector": "train",
        "revision": {"input": "HEAD", "kind": "branch", "commit": Ref("commit:2"), "branch": "main", "tag": None},
        "node": {"id": "4", "ref": Ref("node:4"), "type": "LiteralNode"},
    }


def test_dag_get_node_resolves_named_node_with_explicit_dag_ref():
    ops = Dml(project_home="/repo", remote_uri="s3://bucket/prefix")
    node_ops = Mock(get=Mock(return_value={"answer": Ref("datum:5")}))

    with (
        patch(
            "daggerml._internal.dml.resolve_node_ref",
            return_value=SimpleNamespace(ref=Ref("node:4"), dag_selector="dag:3", revision=None),
        ),
        patch.object(dml_module, "node_ops", return_value=node_ops),
    ):
        result = ops.dag.get_node("result", dag_selector="dag:3")

    node_ops.get.assert_called_once_with(Ref("node:4"))
    assert result == {
        "selector": "result",
        "dag_selector": "dag:3",
        "node": {"answer": Ref("datum:5")},
    }


def test_dag_describe_node_accepts_explicit_node_ref_without_dag_context():
    ops = Dml(project_home="/repo", remote_uri="s3://bucket/prefix")
    node_ref = Ref("node-literal:4")
    node_ops = Mock(describe=Mock(return_value={"id": "4", "ref": node_ref, "type": "LiteralNode"}))

    with patch.object(dml_module, "node_ops", return_value=node_ops):
        result = ops.dag.describe_node(node_ref)

    node_ops.describe.assert_called_once_with(node_ref)
    assert result == {
        "selector": "node-literal:4",
        "dag_selector": None,
        "node": {"id": "4", "ref": node_ref, "type": "LiteralNode"},
    }


def test_dag_get_node_accepts_explicit_node_ref_without_dag_context():
    ops = Dml(project_home="/repo", remote_uri="s3://bucket/prefix")
    node_ref = Ref("node-fn:4")
    node_ops = Mock(get=Mock(return_value={"answer": Ref("datum:5")}))

    with patch.object(dml_module, "node_ops", return_value=node_ops):
        result = ops.dag.get_node(node_ref.to, dag_selector="train", revision="HEAD")

    node_ops.get.assert_called_once_with(node_ref)
    assert result == {
        "selector": "node-fn:4",
        "dag_selector": None,
        "node": {"answer": Ref("datum:5")},
    }


def test_dml_init_recovers_when_config_exists_and_db_missing(tmp_path):
    repo_dir = tmp_path / "repo"
    dml_dir = repo_dir / ".dml"
    dml_dir.mkdir(parents=True)
    (dml_dir / "config.toml").write_text('[project]\nuri = "dml://alice/demo"\n[remote]\nuri = "s3://bucket/prefix"\n')

    with (
        patch("daggerml._internal.dml.DmlOps.create") as mock_create,
        patch("daggerml._internal.dml.Dml.pull", return_value=Ref("commit:9")) as mock_pull,
        patch("daggerml._internal.dml.create_s3_client", return_value=object()),
    ):
        result = Dml.init(str(repo_dir), remote_uri="s3://bucket/prefix")

    mock_create.assert_called_once()
    mock_pull.assert_called_once()
    assert result["branch"] == "main"


def test_dml_boundary_keeps_only_allowed_private_helpers():
    dml = Dml(project_home="/repo")

    assert hasattr(dml, "_context")
    assert hasattr(dml, "_tempdirs")
    assert not hasattr(dml, "_with_ops")
    assert not hasattr(dml, "_head_ops")
    assert not hasattr(dml, "_commit_ops")
    assert not hasattr(dml, "_resolve_revision")

    for namespace in (dml.config, dml.runtime, dml.dag, dml.admin):
        assert hasattr(namespace, "_dml")
        assert not hasattr(namespace, "_stringify_node_selector")


def test_dml_init_uses_init_project_layout_for_bootstrap(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    create_context = Mock()
    create_context.__enter__ = Mock(return_value=None)
    create_context.__exit__ = Mock(return_value=None)

    with (
        patch("daggerml._internal.dml.DmlOps.create", return_value=create_context) as mock_create,
        patch("daggerml._internal.dml.init_project_layout", wraps=init_project_layout) as mock_init_layout,
    ):
        result = Dml.init(
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


def test_dml_init_rejects_name_and_project_uri_together(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with pytest.raises(
        ValueError,
        match=(
            "NAME and --project-uri are mutually exclusive; provide NAME to derive project URI "
            "or use --project-uri for an explicit URI"
        ),
    ):
        Dml.init(
            str(repo_dir),
            name="demo",
            project_uri="dml://alice/demo",
            remote_uri="s3://bucket/prefix",
        )


def test_dml_init_name_mode_requires_resolved_user(tmp_path, monkeypatch):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    monkeypatch.setenv("USER", "")
    monkeypatch.setattr("daggerml._internal.config.getuser", lambda: (_ for _ in ()).throw(RuntimeError()))
    monkeypatch.setattr("daggerml._internal.config.gethostname", lambda: (_ for _ in ()).throw(RuntimeError()))

    with pytest.raises(DmlRepoError, match="user is required to derive project URI from NAME"):
        Dml.init(
            str(repo_dir),
            name="demo",
            remote_uri="s3://bucket/prefix",
        )


def test_dml_init_requires_remote_uri_for_recovery_pull(tmp_path):
    repo_dir = tmp_path / "repo"
    dml_dir = repo_dir / ".dml"
    dml_dir.mkdir(parents=True)
    (dml_dir / "config.toml").write_text('[project]\nuri = "dml://alice/demo"\n')

    with pytest.raises(DmlRepoError, match="remote.uri is required"):
        Dml.init(str(repo_dir), remote_uri="")


def test_dml_init_requires_existing_project_directory(tmp_path):
    missing = tmp_path / "missing"
    with pytest.raises(FileNotFoundError, match="does not exist"):
        Dml.init(str(missing), project_uri="dml://alice/demo", remote_uri="s3://bucket/prefix")
