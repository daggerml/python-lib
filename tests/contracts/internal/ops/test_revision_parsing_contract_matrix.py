from pathlib import Path

import pytest

from daggerml._cli.base import parse_ref
from daggerml._internal._db import Ref
from daggerml._internal.config import DmlProjectConfig, init_project_layout
from daggerml._internal.ops.commit import CommitOps
from daggerml._internal.ops.head import HeadOps
from daggerml._internal.ops.remote import RemoteOps
from daggerml._internal.types import Commit, DmlRepoError, Tree


@pytest.mark.parametrize(
    "contract_id,label,ref_string",
    [
        ("revision-parse-ref-roundtrip", "index-ref", "index:default"),
    ],
    ids=[
        "revision-parse-ref-roundtrip:index-ref",
    ],
)
def test_ref_parse_matrix(contract_id, label, ref_string):
    del contract_id, label
    assert parse_ref(ref_string) == Ref(ref_string)


@pytest.mark.parametrize(
    "contract_id,label,uri,expected_path",
    [
        (
            "revision-uri-canonicalization",
            "branch-uri",
            "dml://alice/demo#main",
            "projects/alice/demo/heads/main.json",
        ),
        (
            "revision-uri-canonicalization",
            "tag-uri",
            "dml://alice/demo@v1.0",
            "projects/alice/demo/tags/v1.0.json",
        ),
    ],
    ids=[
        "revision-uri-canonicalization:branch-uri",
        "revision-uri-canonicalization:tag-uri",
    ],
)
def test_uri_canonicalization_matrix(remote_ops, contract_id, label, uri, expected_path):
    del contract_id, label
    assert RemoteOps.canonical_dml_uri(uri, require_identifier=True) == uri
    assert remote_ops._dml_uri_ref_path(uri) == expected_path


def test_uri_canonicalization_rejects_oversized_identifier(remote_ops):
    del remote_ops
    with pytest.raises(ValueError, match="64-byte"):
        RemoteOps.canonical_dml_uri("dml://alice/" + "x" * 80 + "#main", require_identifier=True)


def _seed_project_commit_history(temp_bo_fn, tmp_path: Path) -> tuple[CommitOps, Ref, Ref]:
    project = DmlProjectConfig(name="demo", owner="alice", branch="main", remote_uri="s3://bucket/prefix")
    init_project_layout(tmp_path, project)
    head_ops = HeadOps(_db=temp_bo_fn._db)
    commit_ops = CommitOps(_db=temp_bo_fn._db)

    main_head = head_ops.create_branch("main")
    initial = head_ops.get_branch_commit(main_head)
    with commit_ops._tx(readonly=False) as txn:
        tree = txn.get(txn.get(initial).tree)
        next_tree = txn.put(Tree(dags=dict(tree.dags)))
        next_commit = txn.put(Commit(parents=[initial], tree=next_tree, author="alice", message="next"))
    head_ops.update_branch_commit(main_head, initial, next_commit)
    head_ops.create_branch("dml://alice/demo@v1_0", initial)

    return commit_ops, initial, next_commit


@pytest.mark.parametrize(
    "contract_id,label,revision_builder,expected_kind,expected_commit",
    [
        (
            "revision-form-classification",
            "branch",
            lambda initial, next_commit: "main",
            "branch",
            lambda initial, next_commit: next_commit,
        ),
        (
            "revision-form-classification",
            "tag",
            lambda initial, next_commit: "v1_0",
            "tag",
            lambda initial, next_commit: initial,
        ),
        (
            "revision-form-classification",
            "ancestry-expression",
            lambda initial, next_commit: "HEAD~1",
            "commit",
            lambda initial, next_commit: initial,
        ),
        (
            "revision-form-classification",
            "direct-commit-id",
            lambda initial, next_commit: initial.id(),
            "commit",
            lambda initial, next_commit: initial,
        ),
        (
            "revision-form-classification",
            "explicit-commit-ref",
            lambda initial, next_commit: f"commit:{initial.id()}",
            "commit",
            lambda initial, next_commit: initial,
        ),
    ],
    ids=[
        "revision-form-classification:branch",
        "revision-form-classification:tag",
        "revision-form-classification:ancestry-expression",
        "revision-form-classification:direct-commit-id",
        "revision-form-classification:explicit-commit-ref",
    ],
)
def test_revision_form_classification_matrix(
    temp_bo_fn,
    tmp_path: Path,
    contract_id,
    label,
    revision_builder,
    expected_kind,
    expected_commit,
):
    del contract_id, label
    commit_ops, initial, next_commit = _seed_project_commit_history(temp_bo_fn, tmp_path)
    revision = revision_builder(initial, next_commit)
    resolved = commit_ops.resolve_revision(revision, current_branch="main", project_dir=str(tmp_path))
    assert resolved.kind == expected_kind
    assert resolved.commit == expected_commit(initial, next_commit)


def test_revision_rejects_unfetched_remote_uri_boundary(temp_bo_fn, tmp_path: Path):
    commit_ops, _initial, _next_commit = _seed_project_commit_history(temp_bo_fn, tmp_path)
    with pytest.raises(DmlRepoError, match="cannot be resolved locally"):
        commit_ops.resolve_revision("dml://alice/demo#main", project_dir=str(tmp_path))
