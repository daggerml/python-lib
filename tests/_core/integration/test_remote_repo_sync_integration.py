from __future__ import annotations

import pytest

from daggerml._core import Dml
from daggerml._core.head import Head
from daggerml._core.types import DmlRepoError
from tests._core.helpers import commit_literal_dag, make_local_dml

pytestmark = pytest.mark.slow


def test_push_fetch_and_pull_use_remote_root_branch_tracking(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    first = commit_literal_dag(source, "train", 1)
    source.push()

    target = make_local_dml(tmp_path / "target", monkeypatch, remote_root=root)
    target.fetch()
    head = Head(str(tmp_path / "target"))
    assert head.get_remote_tracking_ref("main") == first
    head.update_local_ref("main", first)
    head.set_upstream("main", "main")

    second = commit_literal_dag(source, "eval", 2)
    source.push()
    assert target.pull()["commit"] == second


def test_fetch_failure_preserves_existing_tracking_ref(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    commit = commit_literal_dag(source, "train", 1)
    source.push()
    target = make_local_dml(tmp_path / "target", monkeypatch, remote_root=root)
    target.fetch()

    with pytest.raises(DmlRepoError, match="not found"):
        target.fetch("missing")
    assert Head(str(tmp_path / "target")).get_remote_tracking_ref("main") == commit


def test_fetch_depth_can_deepen_then_unshallow_history(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    first = commit_literal_dag(source, "one", 1)
    second = commit_literal_dag(source, "two", 2)
    third = commit_literal_dag(source, "three", 3)
    source.push()
    target = make_local_dml(tmp_path / "target", monkeypatch, remote_root=root)
    head = Head(str(tmp_path / "target"))

    target.fetch(depth=1)
    with target._db.tx(readonly=True) as txn:
        assert txn.exists(third)
        assert not txn.exists(second)
    assert head.get_shallow_commits() == {second}

    target.fetch(depth=2)
    with target._db.tx(readonly=True) as txn:
        assert txn.exists(second)
        assert not txn.exists(first)
    assert head.get_shallow_commits() == {first}

    target.fetch(unshallow=True)
    with target._db.tx(readonly=True) as txn:
        assert txn.exists(first)
    assert head.get_shallow_commits() == set()


def test_fetch_rejects_conflicting_or_invalid_depth_before_remote_access(
    tmp_path, monkeypatch, remote_env, s3_bucket
) -> None:
    del remote_env, s3_bucket
    target = make_local_dml(tmp_path / "target", monkeypatch)

    with pytest.raises(DmlRepoError, match="positive integer"):
        target.fetch(depth=0)
    with pytest.raises(DmlRepoError, match="cannot be selected together"):
        target.fetch(depth=1, unshallow=True)


def test_pull_connects_new_history_and_preserves_older_shallow_boundary(
    tmp_path, monkeypatch, remote_env, s3_bucket
) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    first = commit_literal_dag(source, "one", 1)
    second = commit_literal_dag(source, "two", 2)
    source.push()
    target = Dml.clone("main", project_home=str(tmp_path / "target"), remote_root=root, depth=1)

    third = commit_literal_dag(source, "three", 3)
    source.push()
    result = target.pull()

    assert result["commit"] == third
    with target._db.tx(readonly=True) as txn:
        assert txn.exists(second)
        assert not txn.exists(first)
    assert Head(str(tmp_path / "target")).get_shallow_commits() == {first}


def test_non_forced_push_can_advance_remote_tip_from_shallow_history(
    tmp_path, monkeypatch, remote_env, s3_bucket
) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    first = commit_literal_dag(source, "one", 1)
    second = commit_literal_dag(source, "two", 2)
    source.push()
    target = Dml.clone("main", project_home=str(tmp_path / "target"), remote_root=root, depth=1)

    third = commit_literal_dag(target, "three", 3)
    target.push()

    source.fetch()
    assert Head(str(tmp_path / "source")).get_remote_tracking_ref("main") == third
    with target._db.tx(readonly=True) as txn:
        assert not txn.exists(first)
    assert Head(str(tmp_path / "target")).get_shallow_commits() == {first}
    assert second != third


def test_force_or_new_branch_push_rejects_shallow_history(
    tmp_path, monkeypatch, remote_env, s3_bucket
) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    commit_literal_dag(source, "one", 1)
    commit_literal_dag(source, "two", 2)
    source.push()
    target = Dml.clone("main", project_home=str(tmp_path / "target"), remote_root=root, depth=1)

    with pytest.raises(DmlRepoError, match="Cannot publish shallow history"):
        target.push(force=True)

    target.branch.set_upstream("new-branch")
    with pytest.raises(DmlRepoError, match="Cannot publish shallow history"):
        target.push()
    assert all(item["name"] != "new-branch" for item in target.branch.list(remote=True))


def test_shallow_metadata_failure_preserves_tracking_ref(
    tmp_path, monkeypatch, remote_env, s3_bucket
) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    first = commit_literal_dag(source, "one", 1)
    source.push()
    target = make_local_dml(tmp_path / "target", monkeypatch, remote_root=root)
    target.fetch()
    commit_literal_dag(source, "two", 2)
    source.push()

    def fail_shallow_write(self, commits):
        del self, commits
        raise OSError("shallow write failed")

    monkeypatch.setattr(Head, "write_shallow_commits", fail_shallow_write)
    with pytest.raises(OSError, match="shallow write failed"):
        target.fetch(depth=1)

    assert Head(str(tmp_path / "target")).get_remote_tracking_ref("main") == first


def test_pull_depth_refuses_when_it_cannot_connect_to_local_history(
    tmp_path, monkeypatch, remote_env, s3_bucket
) -> None:
    del remote_env, s3_bucket
    root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=root)
    first = commit_literal_dag(source, "one", 1)
    source.push()
    target = Dml.clone("main", project_home=str(tmp_path / "target"), remote_root=root, depth=1)
    commit_literal_dag(source, "two", 2)
    third = commit_literal_dag(source, "three", 3)
    source.push()

    with pytest.raises(DmlRepoError, match="fetch with greater depth or --unshallow"):
        target.pull(depth=1)

    assert target.status()["commit"] == first
    assert Head(str(tmp_path / "target")).get_remote_tracking_ref("main") == third
