from __future__ import annotations

import pytest

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
