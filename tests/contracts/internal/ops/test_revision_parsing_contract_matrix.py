from pathlib import Path

import pytest

from daggerml._internal._db import Ref
from daggerml._internal.config import DmlProjectConfig, init_project_layout
from daggerml._internal.dml_resolution import resolve_dag_ref, resolve_node_ref, resolve_revision
from daggerml._internal.ops.commit import CommitOps
from daggerml._internal.ops.dag import DagOps
from daggerml._internal.ops.head import HeadOps
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.ops.remote import RemoteOps
from daggerml._internal.types import Commit, DmlRepoError, Tree


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
    project = DmlProjectConfig(name="demo", owner="alice", remote_root="s3://bucket/prefix")
    init_project_layout(tmp_path, project)
    head_ops = HeadOps(_db=temp_bo_fn._db)
    commit_ops = CommitOps(_db=temp_bo_fn._db)

    main_head = head_ops.create_branch("main")
    head_ops.write_attached_head("main")
    initial = head_ops.get_branch_commit(main_head)
    with commit_ops._tx(readonly=False) as txn:
        tree = txn.get(txn.get(initial).tree)
        next_tree = txn.put(Tree(dags=dict(tree.dags)))
        next_commit = txn.put(Commit(parents=[initial], tree=next_tree, author="alice", message="next"))
    head_ops.update_branch_commit(main_head, initial, next_commit)
    head_ops.create_branch("dml://alice/demo@v1_0", initial)

    return commit_ops, initial, next_commit


def _seed_named_dags(temp_bo_fn, tmp_path: Path, dag_nodes: dict[str, str]):
    project = DmlProjectConfig(name="demo", owner="alice", remote_root="s3://bucket/prefix")
    init_project_layout(tmp_path, project)
    head_ops = HeadOps(_db=temp_bo_fn._db)
    commit_ops = CommitOps(_db=temp_bo_fn._db)
    dag_ops = DagOps(_db=temp_bo_fn._db)
    index_ops = IndexOps(_db=temp_bo_fn._db, remote_root="")

    main_head = head_ops.create_branch("main")
    head_ops.write_attached_head("main")

    node_refs: dict[str, Ref] = {}
    for dag_name, node_name in dag_nodes.items():
        index_id = index_ops.create(head=main_head)
        node_ref = index_ops.put_literal(index_id, dag_name, name=node_name)
        index_ops.commit(index_id, node_ref, head=main_head, message=f"add {dag_name}", dag_name=dag_name)
        node_refs[dag_name] = node_ref

    latest_commit = head_ops.get_branch_commit(main_head)
    dag_refs = {dag_name: commit_ops.get_dag(latest_commit, dag_name) for dag_name in dag_nodes}
    return commit_ops, head_ops, dag_ops, latest_commit, dag_refs, node_refs


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
    resolved = resolve_revision(
        value=revision,
        commit_ops=commit_ops,
        head_ops=HeadOps(_db=temp_bo_fn._db),
        project_dir=str(tmp_path),
    )
    assert resolved.kind == expected_kind
    assert resolved.commit == expected_commit(initial, next_commit)


def test_revision_rejects_unfetched_remote_root_boundary(temp_bo_fn, tmp_path: Path):
    commit_ops, _initial, _next_commit = _seed_project_commit_history(temp_bo_fn, tmp_path)
    with pytest.raises(DmlRepoError, match="cannot be resolved locally"):
        resolve_revision(
            value="dml://alice/demo#main",
            commit_ops=commit_ops,
            head_ops=HeadOps(_db=temp_bo_fn._db),
            project_dir=str(tmp_path),
        )


def test_detached_head_ancestry_resolves_from_head_file(temp_bo_fn, tmp_path: Path):
    commit_ops, initial, next_commit = _seed_project_commit_history(temp_bo_fn, tmp_path)
    HeadOps(_db=temp_bo_fn._db).write_detached_head(next_commit)

    resolved = resolve_revision(
        value="HEAD~1",
        commit_ops=commit_ops,
        head_ops=HeadOps(_db=temp_bo_fn._db),
        project_dir=str(tmp_path),
    )

    assert resolved.kind == "commit"
    assert resolved.commit == initial


def test_dag_resolution_returns_named_dag_ref(temp_bo_fn, tmp_path: Path):
    commit_ops, head_ops, _dag_ops, latest_commit, dag_refs, _node_refs = _seed_named_dags(
        temp_bo_fn, tmp_path, {"train": "result"}
    )

    resolved = resolve_dag_ref(
        value="train",
        revision="HEAD",
        commit_ops=commit_ops,
        head_ops=head_ops,
        project_dir=str(tmp_path),
        operation="get",
    )

    assert resolved.ref == dag_refs["train"]
    assert resolved.selector == "train"
    assert resolved.revision is not None
    assert resolved.revision.commit == latest_commit


def test_dag_resolution_rejects_revision_with_explicit_dag_ref(temp_bo_fn, tmp_path: Path):
    commit_ops, head_ops, _dag_ops, _latest_commit, dag_refs, _node_refs = _seed_named_dags(
        temp_bo_fn, tmp_path, {"train": "result"}
    )

    with pytest.raises(DmlRepoError, match="rejects --revision with explicit dag refs"):
        resolve_dag_ref(
            value=dag_refs["train"],
            revision="HEAD",
            commit_ops=commit_ops,
            head_ops=head_ops,
            project_dir=str(tmp_path),
            operation="get",
        )


def test_node_resolution_resolves_named_node_with_explicit_dag_selector(temp_bo_fn, tmp_path: Path):
    commit_ops, head_ops, dag_ops, _latest_commit, _dag_refs, node_refs = _seed_named_dags(
        temp_bo_fn, tmp_path, {"train": "result"}
    )

    resolved = resolve_node_ref(
        value="result",
        dag_selector="train",
        revision="HEAD",
        commit_ops=commit_ops,
        dag_ops=dag_ops,
        head_ops=head_ops,
        project_dir=str(tmp_path),
        operation="describe-node",
    )

    assert resolved.ref == node_refs["train"]
    assert resolved.dag_selector == "train"
    assert resolved.revision is not None


def test_node_resolution_resolves_named_node_without_dag_selector_when_unique(temp_bo_fn, tmp_path: Path):
    commit_ops, head_ops, dag_ops, _latest_commit, _dag_refs, node_refs = _seed_named_dags(
        temp_bo_fn, tmp_path, {"train": "result", "score": "score_result"}
    )

    resolved = resolve_node_ref(
        value="score_result",
        commit_ops=commit_ops,
        dag_ops=dag_ops,
        head_ops=head_ops,
        project_dir=str(tmp_path),
        operation="get-node",
    )

    assert resolved.ref == node_refs["score"]
    assert resolved.dag_selector == "score"
    assert resolved.revision is not None


def test_node_resolution_rejects_ambiguous_named_lookup_without_dag_selector(temp_bo_fn, tmp_path: Path):
    commit_ops, head_ops, dag_ops, _latest_commit, _dag_refs, _node_refs = _seed_named_dags(
        temp_bo_fn, tmp_path, {"train": "result", "score": "result"}
    )

    with pytest.raises(DmlRepoError, match="requires dag_selector for ambiguous node lookup"):
        resolve_node_ref(
            value="result",
            commit_ops=commit_ops,
            dag_ops=dag_ops,
            head_ops=head_ops,
            project_dir=str(tmp_path),
            operation="describe-node",
        )


def test_node_resolution_accepts_explicit_node_ref(temp_bo_fn, tmp_path: Path):
    commit_ops, head_ops, dag_ops, _latest_commit, _dag_refs, _node_refs = _seed_named_dags(
        temp_bo_fn, tmp_path, {"train": "result"}
    )

    resolved = resolve_node_ref(
        value="node-literal:abc123",
        dag_selector="train",
        revision="HEAD",
        commit_ops=commit_ops,
        dag_ops=dag_ops,
        head_ops=head_ops,
        project_dir=str(tmp_path),
        operation="get-node",
    )

    assert resolved.ref == Ref("node-literal:abc123")
    assert resolved.dag_selector is None
    assert resolved.revision is None
