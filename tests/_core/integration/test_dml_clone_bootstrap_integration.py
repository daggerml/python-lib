from __future__ import annotations

import pytest

from daggerml._core import Dml
from daggerml._core.head import Head
from daggerml._core.types import Commit, Tree
from tests._core.helpers import commit_literal_dag, make_local_dml

pytestmark = pytest.mark.slow


def test_clone_branch_attaches_and_persists_remote_root(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    commit = commit_literal_dag(source, "train", 1)
    source.push()

    clone = Dml.clone("main", project_home=str(tmp_path / "clone"), remote_root=root, user="reviewer")
    assert clone.config.get("remote.root") == root
    assert clone.status()["commit"] == commit
    assert clone.status()["branch"] == "main"
    assert Head(str(tmp_path / "clone")).get_remote_tracking_ref("main") == commit


def test_clone_tag_detaches_head(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    commit = commit_literal_dag(source, "train", 1)
    source.tag.create("v1")
    source.push()
    # Tags are published through the direct remote transport API in this focused integration.
    from daggerml._core.dml import _remote_ops

    _remote_ops(source).put_ref(commit, "tag", "v1", source._db)

    clone = Dml.clone("@v1", project_home=str(tmp_path / "clone"), remote_root=root, user="reviewer")
    assert clone.status()["mode"] == "detached"
    assert clone.status()["commit"] == commit


def test_clone_depth_one_materializes_complete_tip_snapshot(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    parent = commit_literal_dag(source, "train", 1)
    tip = commit_literal_dag(source, "eval", 2)
    source.push()

    clone = Dml.clone("main", project_home=str(tmp_path / "clone"), remote_root=root, depth=1)
    shallow = Head(str(tmp_path / "clone")).get_shallow_commits()

    with clone._db.tx(readonly=True) as txn:
        commit = txn.get(tip)
        tree = txn.get(commit.tree)
        assert isinstance(commit, Commit)
        assert isinstance(tree, Tree)
        assert all(txn.exists(dag) for dag in tree.dags.values())
        assert not txn.exists(parent)
    assert shallow == {parent}


def test_clone_exact_commit_with_depth_detaches_head(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    parent = commit_literal_dag(source, "train", 1)
    tip = commit_literal_dag(source, "eval", 2)
    source.push()

    clone = Dml.clone(tip, project_home=str(tmp_path / "clone"), remote_root=root, depth=1)

    assert clone.status()["mode"] == "detached"
    assert clone.status()["commit"] == tip
    assert Head(str(tmp_path / "clone")).get_shallow_commits() == {parent}
