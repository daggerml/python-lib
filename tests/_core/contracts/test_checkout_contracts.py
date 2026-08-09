from __future__ import annotations

from daggerml._core.head import Head
from tests._core.helpers import commit_literal_dag, make_local_dml


def test_checkout_local_branch_keeps_head_attached(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "train", 1)
    Head(str(tmp_path)).create_local_ref("feature", base)

    status = dml.checkout("feature")

    assert status == {
        "mode": "attached",
        "branch": "feature",
        "commit": base,
        "branches": ["feature", "main"],
        "upstream": None,
        "num_indexes": 0,
        "ahead": None,
        "behind": None,
    }


def test_checkout_commit_tag_and_remote_tracking_detach_head(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    first = commit_literal_dag(dml, "train", 1)
    second = commit_literal_dag(dml, "eval", 2)
    head = Head(str(tmp_path))
    head.create_local_ref("release", first, kind="tag")
    head.create_remote_tracking_ref("main", first)

    assert dml.checkout("HEAD~1")["commit"] == first
    assert dml.status()["mode"] == "detached"

    assert dml.checkout("@release")["commit"] == first
    assert dml.status()["mode"] == "detached"

    status = dml.checkout("main", remote=True)

    assert status == {
        "mode": "detached",
        "branch": None,
        "commit": first,
        "branches": ["main"],
        "upstream": None,
        "num_indexes": 0,
        "ahead": None,
        "behind": None,
    }
    assert second == Head(str(tmp_path)).get_local_ref("main")
