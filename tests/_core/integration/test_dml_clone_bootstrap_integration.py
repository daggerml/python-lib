from __future__ import annotations

import pytest

from daggerml._core import Dml
from daggerml._core.head import Head
from tests._core.helpers import commit_literal_dag, make_local_dml

pytestmark = pytest.mark.slow


def _seed_remote_project(tmp_path, monkeypatch):
    remote_root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    remote_project = "dml://acme/demo"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=remote_root, remote_project=remote_project)
    main_commit = commit_literal_dag(source, "train", 1)
    source.push()
    source.branch.create("feature")
    source.checkout("feature")
    feature_commit = commit_literal_dag(source, "eval", 2)
    source.push()
    source.tag.create("v1")
    source.push("@v1")
    return remote_root, remote_project, main_commit, feature_commit


def test_clone_bare_project_uses_default_branch_name(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    remote_root, remote_project, _, feature_commit = _seed_remote_project(tmp_path, monkeypatch)

    target_home = tmp_path / "target-bare"
    cloned = Dml.clone(
        remote_project,
        str(target_home),
        remote_root=remote_root,
        default_branch_name="feature",
        user="reviewer",
    )

    target = Dml(str(target_home))
    head = Head(str(target_home))
    status = cloned.status()
    assert status["mode"] == "attached"
    assert status["branch"] == "feature"
    assert status["commit"] == feature_commit
    assert target.config.get("remote.project") == remote_project
    assert head.get_remote_ref("acme", "demo", "feature") == feature_commit
    assert head.get_local_ref("feature") == feature_commit


def test_clone_branch_uri_attaches_head_to_the_cloned_branch(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    remote_root, remote_project, main_commit, _ = _seed_remote_project(tmp_path, monkeypatch)

    target_home = tmp_path / "target-branch"
    cloned = Dml.clone(f"{remote_project}#main", str(target_home), remote_root=remote_root, user="reviewer")

    target = Dml(str(target_home))
    head = Head(str(target_home))
    status = cloned.status()
    assert status["mode"] == "attached"
    assert status["branch"] == "main"
    assert status["commit"] == main_commit
    assert target.config.get("remote.project") == remote_project
    assert head.get_remote_ref("acme", "demo", "main") == main_commit
    assert head.get_local_ref("main") == main_commit


def test_clone_tag_uri_detaches_head_at_the_fetched_commit(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    remote_root, remote_project, _, feature_commit = _seed_remote_project(tmp_path, monkeypatch)

    target_home = tmp_path / "target-tag"
    cloned = Dml.clone(f"{remote_project}@v1", str(target_home), remote_root=remote_root, user="reviewer")

    target = Dml(str(target_home))
    head = Head(str(target_home))
    status = cloned.status()
    assert status["mode"] == "detached"
    assert status["branch"] is None
    assert status["commit"] == feature_commit
    assert target.config.get("remote.project") == remote_project
    assert head.get_remote_ref("acme", "demo", "v1", kind="tag") == feature_commit
    assert head.list_local_refs() == []
