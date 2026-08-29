from __future__ import annotations

from dataclasses import replace

import pytest

import daggerml._core.dml as dml_mod
from daggerml._core.db import Ref
from daggerml._core.head import Head
from daggerml._core.types import DmlRepoError
from tests._core.helpers import commit_literal_dag, make_local_dml


def test_branch_namespace_supports_create_move_rename_and_delete(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    base = commit_literal_dag(dml, "train", 1, message="base")
    remote_tip = commit_literal_dag(dml, "remote", 2, message="remote")
    head = Head(str(tmp_path))
    head.create_remote_tracking_ref("feature", remote_tip)

    assert dml.branch.list() == [{"name": "main", "commit": remote_tip}]
    assert dml.branch.create("feature", revision="feature", remote=True) == "feature"
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
    assert dml.branch.list() == [
        {"name": "main", "commit": remote_tip},
        {"name": "trunk", "commit": base},
    ]
    assert dml.branch.delete("trunk") is None
    assert dml.branch.list() == [{"name": "main", "commit": remote_tip}]


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
    assert dml.tag.list() == [{"name": "v1", "commit": commit}]
    assert head.get_local_ref("v1", kind="tag") == commit
    assert dml.rev_parse("@v1")["commit"] == commit

    assert dml.tag.delete("v1") is None
    assert dml.tag.list() == []


def test_rev_parse_resolves_remote_tracking_source(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit = commit_literal_dag(dml, "train", 1)
    Head(str(tmp_path)).create_remote_tracking_ref("main", commit)

    assert dml.rev_parse("main", remote=True)["commit"] == commit


def test_revision_sources_remain_mutually_exclusive(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)

    with pytest.raises(DmlRepoError, match="remote and dep cannot be selected together"):
        dml.rev_parse("main", remote=True, dep="models")


@pytest.mark.parametrize("kind", ["branch", "tag"])
def test_ref_list_source_matrix_returns_exact_ordered_items(tmp_path, monkeypatch, kind) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    head = Head(str(tmp_path))
    local_a = commit_literal_dag(dml, "local-a", 1)
    local_z = commit_literal_dag(dml, "local-z", 2)
    if kind == "branch":
        head.create_local_ref("zeta", local_z, kind=kind)
        local = [{"name": "main", "commit": local_z}, {"name": "zeta", "commit": local_z}]
    else:
        head.create_local_ref("zeta", local_z, kind=kind)
        local = [{"name": "zeta", "commit": local_z}]
    head.create_local_ref("alpha", local_a, kind=kind)
    local.insert(0, {"name": "alpha", "commit": local_a})
    head.add_dependency("models", "s3://bucket/models")
    fetched_a = Ref("commit:" + "a" * 64)
    fetched_z = Ref("commit:" + "b" * 64)
    head.update_dependency_ref("models", "zeta", fetched_z, kind=kind)
    head.update_dependency_ref("models", "alpha", fetched_a, kind=kind)
    endpoint_a = Ref("commit:" + "c" * 64)
    endpoint_z = Ref("commit:" + "d" * 64)
    calls = []

    class FakeRemote:
        def __init__(self, root):
            self.root = root

        def list_ref_tips(self, requested_kind):
            calls.append((self.root, requested_kind))
            return [("zeta", endpoint_z), ("alpha", endpoint_a)]

    def remote_ops_for_root(_dml, root, *, initialize=True):
        assert initialize is False
        return FakeRemote(root)

    monkeypatch.setattr(dml_mod, "_remote_ops_for_root", remote_ops_for_root)
    namespace = getattr(dml, kind)

    assert namespace.list() == local
    assert namespace.list(dep="models") == [
        {"name": "alpha", "commit": fetched_a},
        {"name": "zeta", "commit": fetched_z},
    ]
    assert namespace.list(remote=True) == [
        {"name": "alpha", "commit": endpoint_a},
        {"name": "zeta", "commit": endpoint_z},
    ]
    assert namespace.list(remote=True, dep="models") == [
        {"name": "alpha", "commit": endpoint_a},
        {"name": "zeta", "commit": endpoint_z},
    ]
    assert calls == [("s3://bucket/root", kind), ("s3://bucket/models", kind)]
    assert all(set(item) == {"name", "commit"} for item in namespace.list())


def test_ref_list_rejects_unknown_dependency_and_missing_remote_root(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)

    with pytest.raises(DmlRepoError, match="Dependency does not exist"):
        dml.branch.list(dep="missing")

    dml._config = replace(dml._config, remote=replace(dml._config.remote, root=None))
    with pytest.raises(DmlRepoError, match="remote.root is required"):
        dml.tag.list(remote=True)


def test_branch_get_upstream_inspects_arbitrary_branch_and_tag_has_no_counterpart(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    commit_literal_dag(dml, "train", 1)
    dml.branch.create("feature")
    head = Head(str(tmp_path))
    head.set_upstream("feature", "main")

    assert dml.status()["branch"] == "main"
    assert dml.branch.get_upstream("feature") == {"branch": "main"}
    assert dml.branch.get_upstream("main") is None
    assert dml.branch.get_upstream("missing") is None
    assert not hasattr(dml.tag, "get_upstream")
    with pytest.raises(ValueError, match="Invalid branch"):
        dml.branch.get_upstream("bad branch")


@pytest.mark.parametrize(
    "payload",
    ["not-json", "[]", '{"wrong":"main"}', '{"branch":1}', '{"branch":"Bad"}'],
)
def test_branch_get_upstream_rejects_malformed_metadata(tmp_path, monkeypatch, payload) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    path = Head(str(tmp_path)).upstream_path("feature")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")

    with pytest.raises(DmlRepoError, match="Invalid upstream config"):
        dml.branch.get_upstream("feature")
