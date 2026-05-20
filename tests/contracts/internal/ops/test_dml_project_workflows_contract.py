import logging
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, PropertyMock, patch

import pytest

import daggerml._internal.dml as dml_module
from daggerml._internal._db import Ref
from daggerml._internal.config import DmlProjectConfig
from daggerml._internal.dml import Dml
from daggerml._internal.types import DmlRepoError


@contextmanager
def _opened_db(db=None):
    yield db if db is not None else Mock()


def test_fetch_pull_push_workflows_delegate_to_remote_ops():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix")
    remote_ops = Mock()
    head_ops = Mock()
    head_ops.get_attached_head_branch.return_value = "main"
    head_ops.require_attached_head_branch.return_value = "main"
    project_cfg = SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo", remote_project="dml://alice/demo")
    with (
        patch("daggerml._internal.dml_context.DmlProjectConfig.load", return_value=project_cfg),
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_head_ops", return_value=head_ops),
        patch.object(dml_module, "make_remote_ops", return_value=remote_ops),
    ):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        remote_ops.pull_uri_into_branch.return_value = Ref("commit:2")
        remote_ops.push_project_branch.return_value = "projects/alice/demo/heads/main.json"
        remote_ops.push_project_tag.return_value = "projects/alice/demo/tags/v1.0.json"

        fetched = ops.fetch("origin", None)
        pulled = ops.pull("origin", None, branch=None, user="alice")
        pushed = ops.push(None, branch=None, create=False, force=False)
        pushed_tag = ops.push("v1.0", branch=None, create=False, force=False)

    remote_ops.fetch_uri.assert_called_once_with("dml://alice/demo#main")
    remote_ops.pull_uri_into_branch.assert_called_once_with("dml://alice/demo#main", "main", user="alice")
    remote_ops.push_project_branch.assert_called_once_with("dml://alice/demo#main", "main", create=False, force=False)
    remote_ops.push_project_tag.assert_called_once_with("dml://alice/demo@v1.0", "main")
    assert fetched == Ref("commit:1")
    assert pulled == Ref("commit:2")
    assert pushed == "projects/alice/demo/heads/main.json"
    assert pushed_tag == "projects/alice/demo/tags/v1.0.json"


def test_project_workflows_use_dml_owned_s3_client():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix")
    remote_ops = Mock()
    head_ops = Mock()
    head_ops.get_attached_head_branch.return_value = "main"
    head_ops.require_attached_head_branch.return_value = "main"
    project_cfg = SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo", remote_project="dml://alice/demo")
    with (
        patch("daggerml._internal.dml_context.DmlProjectConfig.load", return_value=project_cfg),
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_head_ops", return_value=head_ops),
        patch.object(dml_module, "make_remote_ops", return_value=remote_ops),
    ):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        remote_ops.pull_uri_into_branch.return_value = Ref("commit:2")
        remote_ops.push_project_branch.return_value = "projects/alice/demo/heads/main.json"

        fetched = ops.fetch("origin", None)
        pulled = ops.pull("origin", None, branch=None, user="alice")
        pushed = ops.push(None, branch=None, create=False, force=False)

    remote_ops.fetch_uri.assert_called_once_with("dml://alice/demo#main")
    remote_ops.pull_uri_into_branch.assert_called_once_with("dml://alice/demo#main", "main", user="alice")
    remote_ops.push_project_branch.assert_called_once_with("dml://alice/demo#main", "main", create=False, force=False)
    assert fetched == Ref("commit:1")
    assert pulled == Ref("commit:2")
    assert pushed == "projects/alice/demo/heads/main.json"
    assert ops._s3_client is not None


def test_fetch_project_origin_falls_back_to_default_branch_without_attached_head():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix")
    remote_ops = Mock()
    project_cfg = SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo", remote_project="dml://alice/demo")
    detached_head_ops = Mock(get_attached_head_branch=Mock(return_value=None))
    with (
        patch("daggerml._internal.dml_context.DmlProjectConfig.load", return_value=project_cfg),
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_head_ops", return_value=detached_head_ops),
        patch.object(dml_module, "make_remote_ops", return_value=remote_ops),
    ):
        remote_ops.fetch_uri.return_value = Ref("commit:1")
        fetched = ops.fetch("origin", None)

    remote_ops.fetch_uri.assert_called_once_with("dml://alice/demo#main")
    assert fetched == Ref("commit:1")


def test_push_project_requires_attached_head_or_explicit_branch():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix")
    detached_error = DmlRepoError("Current checkout is detached; attach HEAD or pass an explicit branch")
    project_cfg = SimpleNamespace(owner="alice", name="demo", uri="dml://alice/demo")
    detached_head_ops = Mock(require_attached_head_branch=Mock(side_effect=detached_error))
    with (
        patch("daggerml._internal.dml.load_project_config", return_value=project_cfg),
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_head_ops", return_value=detached_head_ops),
    ):
        with pytest.raises(DmlRepoError, match="Current checkout is detached"):
            ops.push(None, branch=None, create=False, force=False)


def test_checkout_merge_revert_workflows_delegate_to_commit_ops():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix")
    commit_ops = Mock()
    commit_ops.merge_into_head.return_value = Ref("commit:3")
    commit_ops.revert.return_value = Ref("commit:4")
    head_ops = Mock()
    head_ops.require_attached_head_branch.return_value = "main"

    with (
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_commit_ops", return_value=commit_ops),
        patch.object(dml_module, "make_head_ops", return_value=head_ops),
        patch.object(
            dml_module,
            "resolve_dml_revision",
            return_value=SimpleNamespace(commit=Ref("commit:1"), kind="branch", branch="feature"),
        ),
        patch.object(dml_module, "resolve_dml_revision_ref", return_value=Ref("commit:2")),
    ):
        checkout = ops.checkout("feature")
        merged = ops.merge("origin/main", branch=None, user="alice")
        reverted = ops.revert("origin/main", branch=None, user="alice")

    commit_ops.merge_into_head.assert_called_once_with("main", Ref("commit:2"), "alice")
    commit_ops.revert.assert_called_once_with("main", Ref("commit:2"), "alice")
    head_ops.write_attached_head.assert_called_once_with("feature")
    assert checkout["mode"] == "attached"
    assert merged == Ref("commit:3")
    assert reverted == Ref("commit:4")


def test_dag_checkout_delegates_to_commit_ops_with_resolved_defaults():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix", user="alice")
    commit_ops = Mock()
    commit_ops.checkout_dag.return_value = Ref("commit:3")
    head_ops = Mock()
    head_ops.require_attached_head_branch.return_value = "main"

    with (
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_commit_ops", return_value=commit_ops),
        patch.object(dml_module, "make_head_ops", return_value=head_ops),
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
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix", user=None)
    with (
        patch.object(type(ops._context), "user", new_callable=PropertyMock, return_value=None),
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(
            dml_module, "make_head_ops", return_value=Mock(require_attached_head_branch=Mock(return_value="main"))
        ),
        patch.object(dml_module, "resolve_dml_revision_ref", return_value=Ref("commit:2")),
    ):
        with pytest.raises(DmlRepoError, match="user is required for dag checkout"):
            ops.dag.checkout("origin/main", "train")


def test_runtime_cancel_runs_retry_loop_and_returns_stats(caplog):
    caplog.set_level(logging.INFO)
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix", user="alice")
    index = Mock()
    index.cancel.return_value = {
        "index_id": "idx-1",
        "requested_by": "alice",
        "cancelled_path": Path("/tmp/cancelled/idx-1"),
        "graph": {("idx-1", "exec-1"), ("idx-1", "exec-2")},
        "candidate_set": {"exec-1", "exec-2"},
        "own_executions": {"exec-1", "exec-2"},
    }
    outcomes = {
        (2, "exec-1"): {"execution_id": "exec-1", "outcome": None, "lock_retry": True, "cancel_requested": False},
        (2, "exec-2"): {"execution_id": "exec-2", "outcome": -1, "lock_retry": False, "cancel_requested": False},
        (1, "exec-1"): {"execution_id": "exec-1", "outcome": 1, "lock_retry": False, "cancel_requested": True},
    }

    def _cancel_candidate(execution_id, *, requested_by, own_executions):
        assert requested_by == "alice"
        return outcomes[(len(own_executions), execution_id)]

    index._cancel_execution_candidate.side_effect = _cancel_candidate
    sleeps = []
    with (
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_index_ops", return_value=index),
        patch.object(dml_module.time, "sleep", side_effect=lambda delay: sleeps.append(delay)),
    ):
        result = ops.runtime.cancel("idx-1")

    index.cancel.assert_called_once_with("idx-1", requested_by="alice")
    index._complete_index_cancellation.assert_called_once_with(
        "idx-1",
        cancelled_path=Path("/tmp/cancelled/idx-1"),
        own_executions={"exec-1"},
    )
    assert result == {
        "index_id": "idx-1",
        "iterations": 2,
        "graph_edges": 2,
        "candidate_count": 2,
        "own_execution_count": 1,
        "cancelled_count": 1,
        "dropped_count": 1,
        "lock_retry_count": 1,
    }
    assert sleeps == [0.05]
    assert "runtime.cancel iteration=1 index_id=idx-1 candidates=2 owned=2" in caplog.text


def test_runtime_cancel_retries_candidate_errors_with_backoff():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix", user="alice")
    index = Mock()
    index.cancel.return_value = {
        "index_id": "idx-1",
        "requested_by": "alice",
        "cancelled_path": Path("/tmp/cancelled/idx-1"),
        "graph": {("idx-1", "exec-1")},
        "candidate_set": {"exec-1"},
        "own_executions": {"exec-1"},
    }
    attempts = {"count": 0}

    def _cancel_candidate(execution_id, *, requested_by, own_executions):
        assert execution_id == "exec-1"
        assert requested_by == "alice"
        assert own_executions == {"exec-1"}
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise DmlRepoError("boom")
        return {"execution_id": "exec-1", "outcome": 1, "lock_retry": False, "cancel_requested": True}

    index._cancel_execution_candidate.side_effect = _cancel_candidate
    sleeps = []
    with (
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_index_ops", return_value=index),
        patch.object(dml_module.time, "sleep", side_effect=lambda delay: sleeps.append(delay)),
    ):
        result = ops.runtime.cancel("idx-1")

    assert attempts["count"] == 3
    assert sleeps == [0.05, 0.1]
    assert result["iterations"] == 3
    assert result["cancelled_count"] == 1


def test_dag_describe_node_resolves_named_node_with_revision_context():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix")
    revision = SimpleNamespace(commit=Ref("commit:2"), kind="branch", branch="main", tag=None)
    node_ops = Mock(describe=Mock(return_value={"id": "4", "ref": Ref("node:4"), "type": "LiteralNode"}))

    with (
        patch(
            "daggerml._internal.dml.resolve_node_ref",
            return_value=SimpleNamespace(ref=Ref("node:4"), dag="train", revision=revision),
        ),
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_node_ops", return_value=node_ops),
    ):
        result = ops.dag.describe_node("result", dag="train", revision="HEAD")

    node_ops.describe.assert_called_once_with(Ref("node:4"))
    assert result == {
        "revision": {"input": "HEAD", "kind": "branch", "commit": Ref("commit:2"), "branch": "main", "tag": None},
        "node": {"ref": Ref("node:4"), "type": "LiteralNode"},
    }


def test_dag_get_node_resolves_named_node_with_explicit_dag_ref():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix")
    node_ops = Mock(get=Mock(return_value={"answer": Ref("datum:5")}))

    with (
        patch(
            "daggerml._internal.dml.resolve_node_ref",
            return_value=SimpleNamespace(ref=Ref("node:4"), dag="train", revision=None),
        ),
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_node_ops", return_value=node_ops),
    ):
        result = ops.dag.get_node("result", dag="train")

    node_ops.get.assert_called_once_with(Ref("node:4"))
    assert result == {"node": {"answer": Ref("datum:5")}}


def test_dag_describe_node_accepts_explicit_node_ref_without_dag_context():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix")
    node_ref = Ref("node-literal:4")
    node_ops = Mock(describe=Mock(return_value={"id": "4", "ref": node_ref, "type": "LiteralNode"}))

    with (
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_node_ops", return_value=node_ops),
    ):
        result = ops.dag.describe_node(node_ref)

    node_ops.describe.assert_called_once_with(node_ref)
    assert result == {"node": {"ref": node_ref, "type": "LiteralNode"}}


def test_dag_get_node_accepts_explicit_node_ref_without_dag_context():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix")
    node_ref = Ref("node-fn:4")
    node_ops = Mock(get=Mock(return_value={"answer": Ref("datum:5")}))

    with (
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_node_ops", return_value=node_ops),
    ):
        result = ops.dag.get_node(node_ref)

    node_ops.get.assert_called_once_with(node_ref)
    assert result == {"node": {"answer": Ref("datum:5")}}


def test_dag_get_node_rejects_ref_like_node_string_with_dag_context():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix")

    with pytest.raises(DmlRepoError, match="Expected node Ref"):
        with patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()):
            ops.dag.get_node("node-fn:4", dag="train", revision="HEAD")


def test_dag_describe_node_uses_explicit_dag_ref_context_for_named_lookup():
    ops = Dml(project_home="/repo", remote_root="s3://bucket/prefix")
    dag_ref = Ref("dag:3")
    node_ops = Mock(describe=Mock(return_value={"id": "4", "ref": Ref("node:4"), "type": "LiteralNode"}))
    dag_ops = Mock(get_node=Mock(return_value=Ref("node:4")))

    with (
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_node_ops", return_value=node_ops),
        patch.object(dml_module, "make_dag_ops", return_value=dag_ops),
    ):
        result = ops.dag.describe_node("result", dag=dag_ref)

    dag_ops.get_node.assert_called_once_with(dag_ref, "result")
    assert result == {"node": {"ref": Ref("node:4"), "type": "LiteralNode"}}


def test_dml_init_recovers_when_config_exists_and_db_missing(tmp_path):
    repo_dir = tmp_path / "repo"
    dml_dir = repo_dir / ".dml"
    dml_dir.mkdir(parents=True)
    (dml_dir / "config.toml").write_text('[remote]\nproject = "dml://alice/demo"\nroot = "s3://bucket/prefix"\n')

    with (
        patch("daggerml._internal.dml.Dml.fetch", return_value=Ref("commit:9")) as mock_fetch,
    ):
        result = Dml.init(str(repo_dir), remote_root="s3://bucket/prefix")

    mock_fetch.assert_called_once_with("origin", None)
    assert (dml_dir / "db").exists()
    assert result["created"] == {"db": True, "config": False}


def test_dml_boundary_keeps_only_allowed_private_helpers():
    dml = Dml(project_home="/repo")

    assert hasattr(dml, "_context")
    assert hasattr(dml, "_s3_client")
    assert not hasattr(dml, "_with_ops")
    assert not hasattr(dml, "_head_ops")
    assert not hasattr(dml, "_commit_ops")
    assert not hasattr(dml, "_resolve_revision")

    for namespace in (dml.config, dml.runtime, dml.dag, dml.admin):
        assert hasattr(namespace, "_dml")
        assert not hasattr(namespace, "_selector_payload")


def test_dml_init_uses_init_project_layout_for_bootstrap(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with patch("daggerml._internal.dml.Dml.fetch", return_value=Ref("commit:9")) as mock_fetch:
        result = Dml.init(
            str(repo_dir),
            remote_project="dml://alice/demo",
            remote_root="s3://bucket/prefix",
            user="alice@example-host",
        )

    init_cfg = DmlProjectConfig.load(repo_dir)
    assert init_cfg.name == "demo"
    assert init_cfg.owner == "alice"
    assert init_cfg.remote_project == "dml://alice/demo"
    assert init_cfg.remote_root == "s3://bucket/prefix"
    assert (repo_dir / ".dml").is_dir()
    assert (repo_dir / ".dml" / "config.toml").exists()
    assert (repo_dir / ".dml" / "db").exists()
    mock_fetch.assert_called_once_with("origin", None)
    assert result["project_home"] == str(repo_dir.resolve())
    assert result["remote_root"] == "s3://bucket/prefix"
    assert result["user"] == "alice@example-host"
    assert result["created"] == {"db": True, "config": True}


def test_dml_init_allows_local_only_bootstrap(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with (
        patch.dict("os.environ", {}, clear=True),
        patch("daggerml._internal.dml.Dml.fetch") as mock_fetch,
    ):
        result = Dml.init(str(repo_dir))

    project_cfg = DmlProjectConfig.load(repo_dir)
    assert project_cfg.remote_project is None
    assert project_cfg.remote_root == ""
    mock_fetch.assert_not_called()
    assert result["created"] == {"db": True, "config": True}


def test_dml_init_allows_remote_root_without_remote_project(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with patch("daggerml._internal.dml.Dml.fetch") as mock_fetch:
        result = Dml.init(str(repo_dir), remote_root="s3://bucket/prefix")

    project_cfg = DmlProjectConfig.load(repo_dir)
    assert project_cfg.remote_project is None
    assert project_cfg.remote_root == "s3://bucket/prefix"
    mock_fetch.assert_not_called()
    assert result["remote_root"] == "s3://bucket/prefix"


def test_dml_init_rejects_remote_project_without_remote_root(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(DmlRepoError, match="remote.root is required"):
            Dml.init(str(repo_dir), remote_project="dml://alice/demo")


def test_dml_init_resolves_remote_root_from_env_for_remote_project(tmp_path, monkeypatch):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    monkeypatch.setenv("DML_REMOTE_ROOT", "s3://bucket/prefix")

    with patch("daggerml._internal.dml.Dml.fetch", return_value=Ref("commit:9")) as mock_fetch:
        result = Dml.init(str(repo_dir), remote_project="dml://alice/demo")

    project_cfg = DmlProjectConfig.load(repo_dir)
    assert project_cfg.remote_project == "dml://alice/demo"
    assert project_cfg.remote_root == "s3://bucket/prefix"
    assert result["remote_root"] == "s3://bucket/prefix"
    mock_fetch.assert_called_once_with("origin", None)


def test_dml_init_requires_remote_root_for_recovery_pull(tmp_path):
    repo_dir = tmp_path / "repo"
    dml_dir = repo_dir / ".dml"
    dml_dir.mkdir(parents=True)
    (dml_dir / "config.toml").write_text('[remote]\nproject = "dml://alice/demo"\n')

    with pytest.raises(DmlRepoError, match="remote.root is required"):
        Dml.init(str(repo_dir), remote_root="")


def test_dml_init_requires_existing_project_directory(tmp_path):
    missing = tmp_path / "missing"
    with pytest.raises(FileNotFoundError, match="does not exist"):
        Dml.init(str(missing), remote_project="dml://alice/demo", remote_root="s3://bucket/prefix")


def test_project_sync_requires_remote_project(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    Dml.init(str(repo_dir), remote_root="s3://bucket/prefix")
    dml = Dml(project_home=str(repo_dir), remote_root="s3://bucket/prefix")

    with pytest.raises(DmlRepoError, match="remote.project is required for project sync"):
        dml.fetch("origin", None)

    with pytest.raises(DmlRepoError, match="remote.project is required for project sync"):
        dml.pull("origin", None, branch="main", user="alice")

    with pytest.raises(DmlRepoError, match="remote.project is required for project sync"):
        dml.push(None, branch="main", create=False, force=False)


def test_remote_root_only_repo_can_create_indexes(tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    Dml.init(str(repo_dir), remote_root="s3://bucket/prefix")
    dml = Dml(project_home=str(repo_dir), remote_root="s3://bucket/prefix")

    with patch("daggerml._internal.exec_state.ExecutionState.update_execution_record", return_value={}):
        index_id = dml.runtime.create()
    node = dml.runtime.put_literal(index_id, 42, name="answer")

    assert index_id
    assert node.ns() == "node-literal"
