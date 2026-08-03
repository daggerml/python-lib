from __future__ import annotations

import pytest

from daggerml._core.head import Head
from daggerml._core.types import DmlRepoError
from tests._core.helpers import commit_literal_dag, make_local_dml

pytestmark = pytest.mark.slow


def test_push_uses_attached_branch_upstream_and_fetches_named_tracking_refs(
    tmp_path,
    monkeypatch,
    remote_env,
    s3_bucket,
) -> None:
    del remote_env, s3_bucket
    remote_root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    remote_project = "dml://acme/demo"

    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=remote_root, remote_project=remote_project)
    commit = commit_literal_dag(source, "train", 1)

    source.push()

    target = make_local_dml(
        tmp_path / "target",
        monkeypatch,
        user="reviewer",
        remote_root=remote_root,
        remote_project=remote_project,
    )
    target.fetch()
    assert Head(str(tmp_path / "target")).get_remote_tracking_ref("origin", "main") == commit
    assert source.status()["upstream"] == "origin/main"


def test_fetch_updates_local_remote_tracking_refs(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    remote_root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    remote_project = "dml://acme/demo"

    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=remote_root, remote_project=remote_project)
    first = commit_literal_dag(source, "train", 1)
    source.push()

    target_home = tmp_path / "target"
    target = make_local_dml(
        target_home,
        monkeypatch,
        user="reviewer",
        remote_root=remote_root,
        remote_project=remote_project,
    )
    target.fetch()
    target_head = Head(str(target_home))
    assert target_head.get_remote_tracking_ref("origin", "main") == first

    second = commit_literal_dag(source, "eval", 2)
    source.push()

    target.fetch()
    assert target_head.get_remote_tracking_ref("origin", "main") == second


def test_fetch_explicit_uri_updates_uri_tracking_ref(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    remote_root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    remote_project = "dml://acme/demo"
    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=remote_root, remote_project=remote_project)
    commit = commit_literal_dag(source, "train", 1)
    source.push()

    target = make_local_dml(tmp_path / "target", monkeypatch, remote_root=remote_root)
    target.fetch(f"{remote_project}#main")

    assert Head(str(tmp_path / "target")).get_remote_ref("acme", "demo", "main") == commit


def test_pull_fast_forwards_attached_branch_and_rejects_detached_head(
    tmp_path,
    monkeypatch,
    remote_env,
    s3_bucket,
) -> None:
    del remote_env, s3_bucket
    remote_root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    remote_project = "dml://acme/demo"

    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=remote_root, remote_project=remote_project)
    base = commit_literal_dag(source, "train", 1)
    source.push()

    target_home = tmp_path / "target"
    target = make_local_dml(
        target_home,
        monkeypatch,
        user="reviewer",
        remote_root=remote_root,
        remote_project=remote_project,
    )
    target.fetch()
    Head(str(target_home)).update_local_ref("main", base)
    Head(str(target_home)).set_upstream("main", "origin", "main")

    commit = commit_literal_dag(source, "eval", 2)
    source.push()

    status = target.pull()
    assert status["mode"] == "attached"
    assert status["branch"] == "main"
    assert status["commit"] == commit
    assert set(target.show("HEAD")["dags"]) == {"train", "eval"}

    target.checkout("HEAD")
    with pytest.raises(DmlRepoError, match="Cannot pull when HEAD is detached"):
        target.pull()


def test_push_rejects_divergence_without_moving_local_refs_and_force_overwrites(
    tmp_path, monkeypatch, remote_env, s3_bucket
) -> None:
    del remote_env, s3_bucket
    remote_root = f"s3://test-bucket/test-prefix/{tmp_path.name}"
    remote_project = "dml://acme/demo"

    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=remote_root, remote_project=remote_project)
    base = commit_literal_dag(source, "base", 1)
    source.push()

    target_home = tmp_path / "target"
    target = make_local_dml(target_home, monkeypatch, remote_root=remote_root, remote_project=remote_project)
    target.fetch()
    target_head = Head(str(target_home))
    target_head.update_local_ref("main", base)

    remote_tip = commit_literal_dag(source, "source", 2)
    source.push()
    local_tip = commit_literal_dag(target, "target", 3)

    with pytest.raises(DmlRepoError, match="non-fast-forward"):
        target.push()

    assert target.status()["commit"] == local_tip
    assert target_head.get_remote_tracking_ref("origin", "main") == base

    source.fetch()
    assert Head(str(tmp_path / "source")).get_remote_tracking_ref("origin", "main") == remote_tip

    target.push(force=True)
    source.fetch()
    assert Head(str(tmp_path / "source")).get_remote_tracking_ref("origin", "main") == local_tip
