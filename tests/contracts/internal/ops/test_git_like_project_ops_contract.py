from pathlib import Path

import pytest

from daggerml._internal.config import DmlProjectConfig, init_project_layout, normalize_project_uri
from daggerml._internal.dml_resolution import resolve_revision_ref
from daggerml._internal.ops.commit import CommitOps
from daggerml._internal.ops.head import HeadOps
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.ops.remote import RemoteOps
from daggerml._internal.revision_uri import canonicalize_revision_uri, parse_revision_uri, stringify_revision_uri
from daggerml._internal.types import DmlPointerConflictError, DmlRepoError


def test_project_ref_paths_and_dml_uri_validation(remote_ops):
    assert remote_ops._project_branch_ref_path("alice", "demo", "feature/x") == (
        "projects/alice/demo/heads/feature/x.json"
    )
    assert remote_ops._project_tag_ref_path("alice", "demo", "v1.0") == "projects/alice/demo/tags/v1.0.json"


def test_shared_revision_uri_helpers_and_wrappers_are_compatible():
    parsed = parse_revision_uri("dml://alice/demo", default_branch="main")
    assert stringify_revision_uri(parsed) == "dml://alice/demo#main"
    assert canonicalize_revision_uri("dml://alice/demo", default_branch="main") == "dml://alice/demo#main"
    assert normalize_project_uri("dml://alice/demo", default_branch="main", require_branch=True) == "dml://alice/demo#main"
    assert normalize_project_uri("dml://alice/demo@v1", require_branch=False) == "dml://alice/demo@v1"
    assert RemoteOps.canonical_dml_uri("dml://alice/demo@v1", require_identifier=True) == "dml://alice/demo@v1"


def test_project_config_layout_roundtrip(tmp_path: Path):
    cfg = DmlProjectConfig(
        name="demo",
        owner="alice",
        remote_uri="s3://bucket/team/dml",
    )
    db_path = init_project_layout(tmp_path, cfg)

    assert db_path == tmp_path / ".dml" / "db"
    assert (tmp_path / ".dml" / ".gitignore").read_text() == "db\nHEAD\nrefs\n"
    loaded = DmlProjectConfig.load(tmp_path)
    assert loaded.name == "demo"
    assert loaded.owner == "alice"
    assert loaded.remote_uri == "s3://bucket/team/dml"
    assert loaded.remote_project == "dml://alice/demo"


def test_project_config_layout_allows_missing_remote_project(tmp_path: Path):
    cfg = DmlProjectConfig(remote_uri="s3://bucket/team/dml")
    init_project_layout(tmp_path, cfg)

    loaded = DmlProjectConfig.load(tmp_path)
    assert loaded.name is None
    assert loaded.owner is None
    assert loaded.remote_uri == "s3://bucket/team/dml"
    assert loaded.remote_project is None


def test_head_advance_and_revision_resolution(temp_bo_fn):
    head_ops = HeadOps(_db=temp_bo_fn._db)
    commit_ops = CommitOps(_db=temp_bo_fn._db)
    head = head_ops.create_branch("feature")
    commit = head_ops.get_branch_commit(head)
    new_head = head_ops.create_branch("copy", commit)

    head_ops.update_branch_commit(new_head, head_ops.get_branch_commit(new_head), commit)
    assert resolve_revision_ref(value="copy", commit_ops=commit_ops, head_ops=head_ops, project_dir=".") == commit
    head_ops.write_attached_head("copy")
    with pytest.raises(DmlRepoError, match="walks past the root commit"):
        resolve_revision_ref(value="HEAD~1", commit_ops=commit_ops, head_ops=head_ops, project_dir=".")


def test_checkout_absent_dag_does_not_advance_head(temp_bo_fn):
    head_ops = HeadOps(_db=temp_bo_fn._db)
    commit_ops = CommitOps(_db=temp_bo_fn._db)
    head = head_ops.create_branch("checkout")
    commit = head_ops.get_branch_commit(head)
    with pytest.raises(DmlRepoError, match="not found"):
        commit_ops.checkout_dag(head, commit, "missing", user="alice")
    assert head_ops.get_branch_commit(head) == commit


def test_detached_commit_does_not_advance_branch_head_and_reattach_resumes(temp_bo_fn):
    head_ops = HeadOps(_db=temp_bo_fn._db)
    index_ops = IndexOps(_db=temp_bo_fn._db, remote_root="")
    main_head = head_ops.create_branch("main")
    start = head_ops.get_branch_commit(main_head)

    detached_index = index_ops.create(head=main_head)
    node = index_ops.put_literal(detached_index, 42)
    detached_commit = index_ops.commit(detached_index, node, head=None, message="detached")
    assert head_ops.get_branch_commit(main_head) == start

    attached_index = index_ops.create(head=main_head)
    node2 = index_ops.put_literal(attached_index, 84)
    attached_commit = index_ops.commit(attached_index, node2, head=main_head, message="attached")
    assert head_ops.get_branch_commit(main_head) == attached_commit
    assert attached_commit != detached_commit


def test_commit_lifecycle_stages_attached_detached_detached_reattach(temp_bo_fn):
    head_ops = HeadOps(_db=temp_bo_fn._db)
    index_ops = IndexOps(_db=temp_bo_fn._db, remote_root="")
    main_head = head_ops.create_branch("main")

    # Stage 1: attached commit advances branch head.
    start = head_ops.get_branch_commit(main_head)
    idx1 = index_ops.create(head=main_head)
    n1 = index_ops.put_literal(idx1, "s1")
    c1 = index_ops.commit(idx1, n1, head=main_head, message="stage-1-attached")
    assert head_ops.get_branch_commit(main_head) == c1

    # Stage 2: detached commit from branch snapshot does not advance head.
    idx2 = index_ops.create(head=main_head)
    n2 = index_ops.put_literal(idx2, "s2")
    c2 = index_ops.commit(idx2, n2, head=None, message="stage-2-detached")
    assert head_ops.get_branch_commit(main_head) == c1

    # Stage 3: detached commit from detached commit also does not advance any head.
    detached_head = head_ops.create_branch("scratch", c2)
    idx3 = index_ops.create(head=detached_head)
    n3 = index_ops.put_literal(idx3, "s3")
    _c3 = index_ops.commit(idx3, n3, head=None, message="stage-3-detached")
    assert head_ops.get_branch_commit(main_head) == c1
    assert head_ops.get_branch_commit(detached_head) == c2

    # Stage 4: re-attach and commit resumes branch progression.
    idx4 = index_ops.create(head=main_head)
    n4 = index_ops.put_literal(idx4, "s4")
    c4 = index_ops.commit(idx4, n4, head=main_head, message="stage-4-reattach")
    assert head_ops.get_branch_commit(main_head) == c4
    assert c4 != start


def test_headops_stale_pointer_updates_report_current_commit(temp_bo_fn):
    head_ops = HeadOps(_db=temp_bo_fn._db)
    index_ops = IndexOps(_db=temp_bo_fn._db, remote_root="")

    branch = head_ops.create_branch("main")
    start = head_ops.get_branch_commit(branch)

    idx1 = index_ops.create(head=branch)
    node1 = index_ops.put_literal(idx1, "first")
    commit1 = index_ops.commit(idx1, node1, head=branch, message="first")

    with pytest.raises(DmlPointerConflictError) as branch_conflict:
        head_ops.update_branch_commit(branch, start, commit1)
    assert branch_conflict.value.current_commit == commit1

    idx2 = index_ops.create(head=branch)
    stale = head_ops.get_index_commit(idx2)
    # write a second node (we don't need the returned ref here)
    _ = index_ops.put_literal(idx2, "second")
    latest = head_ops.get_index_commit(idx2)

    with pytest.raises(DmlPointerConflictError) as index_conflict:
        head_ops.update_index_commit(idx2, stale, latest)
    assert index_conflict.value.current_commit == latest
