from __future__ import annotations

import pytest

from daggerml._core.head import Head
from daggerml._core.types import DmlRepoError
from tests._core.helpers import commit_literal_dag, make_local_dml

pytestmark = pytest.mark.slow


def test_push_uses_attached_branch_by_default_and_rejects_unsupported_revision(
    tmp_path,
    monkeypatch,
    remote_env,
    s3_bucket,
) -> None:
    del remote_env, s3_bucket
    remote_root = "s3://test-bucket/test-prefix"
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
    target.fetch(remote_project)
    assert Head(str(tmp_path / "target")).get_remote_ref("acme", "demo", "main") == commit

    with pytest.raises(DmlRepoError, match="Unsupported revision for push: HEAD~1"):
        source.push("HEAD~1")


def test_fetch_updates_local_remote_tracking_refs(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    remote_root = "s3://test-bucket/test-prefix"
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
    target.fetch(remote_project)
    target_head = Head(str(target_home))
    assert target_head.get_remote_ref("acme", "demo", "main") == first

    second = commit_literal_dag(source, "eval", 2)
    source.push()

    target.fetch(remote_project)
    assert target_head.get_remote_ref("acme", "demo", "main") == second


def test_push_delete_removes_remote_branch_and_tag_refs(tmp_path, monkeypatch, remote_env, s3_bucket) -> None:
    del remote_env, s3_bucket
    remote_root = "s3://test-bucket/test-prefix"
    remote_project = "dml://acme/demo"

    source = make_local_dml(tmp_path / "source", monkeypatch, remote_root=remote_root, remote_project=remote_project)
    commit_literal_dag(source, "train", 1)
    source.branch.create("feature")
    source.tag.create("v1")

    source.push("feature")
    source.push("@v1")

    target = make_local_dml(
        tmp_path / "target",
        monkeypatch,
        user="reviewer",
        remote_root=remote_root,
        remote_project=remote_project,
    )
    target.fetch("#feature")
    target.fetch("@v1")
    target_head = Head(str(tmp_path / "target"))
    assert target_head.list_remote_refs("acme", "demo") == ["feature"]
    assert target_head.list_remote_refs("acme", "demo", kind="tag") == ["v1"]

    source.push("#feature", delete=True)
    source.push("dml://acme/demo@v1", delete=True)

    with pytest.raises(DmlRepoError, match="Remote branch ref not found"):
        target.fetch("#feature")
    with pytest.raises(DmlRepoError, match="Remote tag ref not found"):
        target.fetch("@v1")


def test_pull_fast_forwards_attached_branch_and_rejects_detached_head(
    tmp_path,
    monkeypatch,
    remote_env,
    s3_bucket,
) -> None:
    del remote_env, s3_bucket
    remote_root = "s3://test-bucket/test-prefix"
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
    target.fetch(remote_project)
    Head(str(target_home)).update_local_ref("main", base)

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
