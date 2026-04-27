from pathlib import Path

import pytest

from daggerml._internal.config import DmlProjectConfig, init_project_layout
from daggerml._internal.ops.commit import CommitOps
from daggerml._internal.ops.head import HeadOps
from daggerml._internal.ops.remote import RemoteOps
from daggerml._internal.types import DmlRepoError


def test_project_ref_paths_and_dml_uri_validation(remote_ops):
    assert remote_ops._project_branch_ref_path("alice", "demo", "feature/x") == (
        "projects/alice/demo/heads/feature/x.json"
    )
    assert remote_ops._project_tag_ref_path("alice", "demo", "v1.0") == "projects/alice/demo/tags/v1.0.json"
    assert RemoteOps.canonical_dml_uri("dml://alice/demo#main", require_identifier=True) == "dml://alice/demo#main"
    assert remote_ops._dml_uri_ref_path("dml://alice/demo@v1.0") == "projects/alice/demo/tags/v1.0.json"
    with pytest.raises(ValueError, match="64-byte"):
        RemoteOps.canonical_dml_uri("dml://alice/" + "x" * 80 + "#main", require_identifier=True)


def test_project_config_layout_roundtrip(tmp_path: Path):
    cfg = DmlProjectConfig(
        name="demo",
        owner="alice",
        branch="main",
        remote_uri="s3://bucket/team/dml",
    )
    db_path = init_project_layout(tmp_path, cfg)

    assert db_path == tmp_path / ".dml" / "db"
    assert (tmp_path / ".dml" / ".gitignore").read_text() == "*\n"
    loaded = DmlProjectConfig.load(tmp_path)
    assert loaded.name == "demo"
    assert loaded.owner == "alice"
    assert loaded.remote_uri == "s3://bucket/team/dml"


def test_head_advance_and_commitish_resolution(temp_bo_fn):
    head_ops = HeadOps(_db=temp_bo_fn._db)
    commit_ops = CommitOps(_db=temp_bo_fn._db)
    head = head_ops.create("feature")
    commit = head_ops.describe(head)["commit"]
    new_head = head_ops.create("copy", from_head=commit)

    head_ops.advance(new_head, commit)
    assert commit_ops.resolve_commitish("copy") == commit
    with pytest.raises(DmlRepoError, match="walks past root"):
        commit_ops.resolve_commitish("copy~1")


def test_checkout_absent_dag_does_not_advance_head(temp_bo_fn):
    head_ops = HeadOps(_db=temp_bo_fn._db)
    commit_ops = CommitOps(_db=temp_bo_fn._db)
    head = head_ops.create("checkout")
    commit = head_ops.describe(head)["commit"]
    with pytest.raises(DmlRepoError, match="not found"):
        commit_ops.checkout_dag(head, commit, "missing", user="alice")
    assert head_ops.describe(head)["commit"] == commit
