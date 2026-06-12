from __future__ import annotations

import pytest

from daggerml._core.head import Head
from daggerml._core.types import DmlRepoError
from tests._core.helpers import commit_literal_dag, make_local_dml


def test_branch_namespace_supports_create_move_rename_and_delete(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "train", 1, message="base")
    remote_tip = commit_literal_dag(dml, "remote", 2, message="remote")
    head = Head(str(tmp_path))
    head.create_remote_ref("acme", "demo", "feature", remote_tip)

    assert dml.branch.list() == ["main"]
    assert dml.branch.create("feature", "dml://acme/demo#feature") == "feature"
    assert head.get_local_ref("feature") == remote_tip

    assert dml.branch.move("feature", "HEAD~1") == "feature"
    assert head.get_local_ref("feature") == base

    dml.checkout("feature")
    assert dml.branch.rename("feature", "trunk") == "trunk"
    assert dml.status()["branch"] == "trunk"
    assert head.get_local_ref("trunk") == base

    with pytest.raises(DmlRepoError, match="Cannot delete current branch"):
        dml.branch.delete("trunk")

    dml.checkout("main")
    assert dml.branch.list() == ["main", "trunk"]
    assert dml.branch.delete("trunk") is None
    assert dml.branch.list() == ["main"]


def test_branch_create_on_unborn_head_repoints_head_without_materializing_ref(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    head = Head(str(tmp_path))

    assert dml.branch.create("feature") == "feature"
    assert dml.status()["branch"] == "feature"
    assert dml.status()["commit"] is None
    assert dml.branch.list() == []
    assert not head.local_ref_path("feature", kind="branch").exists()


def test_tag_namespace_supports_create_list_and_delete(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit = commit_literal_dag(dml, "train", 1)
    head = Head(str(tmp_path))

    assert dml.tag.create("v1") == "v1"
    assert dml.tag.list() == ["v1"]
    assert head.get_local_ref("v1", kind="tag") == commit
    assert dml.rev_parse("@v1")["commit"] == commit

    assert dml.tag.delete("v1") is None
    assert dml.tag.list() == []


def test_rev_parse_rejects_named_remote_shorthand(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)

    with pytest.raises(DmlRepoError, match="Unsupported named-remote selector: origin/main"):
        dml.rev_parse("origin/main")
