from __future__ import annotations

from dataclasses import dataclass

from daggerml._internal._db import Ref
from daggerml._internal.dml_context import load_project_config
from daggerml._internal.revision_uri import parse_revision_uri
from daggerml._internal.types import Commit, DmlRepoError, Tree


@dataclass(frozen=True)
class ResolvedRevision:
    kind: str
    commit: Ref
    branch: str | None = None
    tag: str | None = None


@dataclass(frozen=True)
class ResolvedDag:
    ref: Ref
    selector: str
    revision: ResolvedRevision | None = None


@dataclass(frozen=True)
class ResolvedNode:
    ref: Ref
    selector: str
    dag_selector: str | None = None
    revision: ResolvedRevision | None = None


def _walk_first_parent(commit_ops, commit: Ref, steps: int) -> Ref:
    current = commit
    for _ in range(steps):
        info = commit_ops.describe(current)
        parents = info["parents"]
        if not parents:
            raise DmlRepoError(f"Revision ancestry walks past the root commit: {current}")
        current = parents[0]
    return current


def _resolve_head(head_ops) -> Ref:
    return head_ops.resolve_head_commit()


def _resolve_local_tag_ref(value: str, head_ops):
    try:
        return head_ops.get_branch_commit(value)
    except DmlRepoError:
        return None


def _resolve_project_tag_ref(value: str, *, head_ops, project_dir: str):
    try:
        project = load_project_config(project_dir)
    except Exception:
        return None
    return _resolve_local_tag_ref(f"{project.remote_project}@{value}", head_ops)


def _coerce_ref(value: str | Ref, expected_root_ns: str) -> Ref | None:
    candidate = value if isinstance(value, Ref) else None
    if candidate is None and isinstance(value, str) and ":" in value:
        try:
            candidate = Ref(value)
        except Exception:
            candidate = None
    if candidate is None:
        return None
    if candidate.nss()[0] != expected_root_ns:
        raise DmlRepoError(f"Expected {expected_root_ns} ref, got: {candidate}")
    return candidate


def _list_commit_dags(*, commit: Ref, commit_ops) -> dict[str, Ref]:
    with commit_ops._tx(readonly=True) as txn:
        commit_obj = txn.get(commit)
        if not isinstance(commit_obj, Commit):
            raise DmlRepoError(f"Expected Commit at {commit}, got {type(commit_obj)}")
        tree = txn.get(commit_obj.tree)
        if not isinstance(tree, Tree):
            raise DmlRepoError(f"Expected Tree at {commit_obj.tree}, got {type(tree)}")
        return dict(tree.dags)


def resolve_revision(*, value: str, commit_ops, head_ops, project_dir: str) -> ResolvedRevision:
    if isinstance(value, Ref):
        if value.ns() != "commit":
            raise DmlRepoError(f"Expected commit ref, got: {value}")
        return ResolvedRevision(kind="commit", commit=value)

    if not isinstance(value, str) or not value:
        raise DmlRepoError("Revision is required")

    if value.startswith("commit:"):
        commit = Ref(value)
        if commit.ns() != "commit":
            raise DmlRepoError(f"Expected commit ref, got: {commit}")
        commit_ops.describe(commit)
        return ResolvedRevision(kind="commit", commit=commit)

    if value.startswith("dml://"):
        try:
            parsed = parse_revision_uri(value, require_identifier=True)
        except ValueError as exc:
            raise DmlRepoError(str(exc)) from exc
        local_name = value
        commit = _resolve_local_tag_ref(local_name, head_ops)
        if commit is None and parsed.branch is not None:
            commit = _resolve_local_tag_ref(local_name, head_ops)
        if commit is None:
            raise DmlRepoError(f"Revision {value!r} cannot be resolved locally")
        kind = "branch" if parsed.branch is not None else "tag"
        return ResolvedRevision(kind=kind, commit=commit, branch=parsed.branch, tag=parsed.tag)

    if value.startswith("HEAD"):
        base = _resolve_head(head_ops)
        if value == "HEAD":
            state = head_ops.get_head_state()
            return ResolvedRevision(kind="branch" if state.branch else "commit", commit=base, branch=state.branch)
        if not value.startswith("HEAD~"):
            raise DmlRepoError(f"Unsupported revision: {value}")
        try:
            steps = int(value[5:], 10)
        except ValueError as exc:
            raise DmlRepoError(f"Unsupported revision: {value}") from exc
        return ResolvedRevision(kind="commit", commit=_walk_first_parent(commit_ops, base, steps))

    if len(value) == 64 and all(ch in "0123456789abcdef" for ch in value):
        commit = Ref(f"commit:{value}")
        commit_ops.describe(commit)
        return ResolvedRevision(kind="commit", commit=commit)

    try:
        commit = head_ops.get_branch_commit(value)
    except DmlRepoError:
        commit = _resolve_project_tag_ref(value, head_ops=head_ops, project_dir=project_dir)
        if commit is not None:
            return ResolvedRevision(kind="tag", commit=commit, tag=value)
        raise
    return ResolvedRevision(kind="branch", commit=commit, branch=value)


def resolve_revision_ref(*, value: str, commit_ops, head_ops, project_dir: str) -> Ref:
    return resolve_revision(value=value, commit_ops=commit_ops, head_ops=head_ops, project_dir=project_dir).commit


def resolve_dag_ref(
    *,
    value: str | Ref,
    revision: str | None = None,
    commit_ops,
    head_ops,
    project_dir: str,
    operation: str,
) -> ResolvedDag:
    dag_ref = _coerce_ref(value, "dag")
    if dag_ref is not None:
        if revision is not None:
            raise DmlRepoError(f"dml dag {operation} rejects --revision with explicit dag refs")
        return ResolvedDag(ref=dag_ref, selector=dag_ref.to)

    if not isinstance(value, str) or not value:
        raise DmlRepoError("DAG selector is required")

    resolved = resolve_revision(
        value=revision or "HEAD",
        commit_ops=commit_ops,
        head_ops=head_ops,
        project_dir=project_dir,
    )
    resolved_dag_ref = commit_ops.get_dag(resolved.commit, value)
    if resolved_dag_ref is None:
        raise DmlRepoError(f"DAG '{value}' not found")
    return ResolvedDag(ref=resolved_dag_ref, selector=value, revision=resolved)


def resolve_node_ref(
    *,
    value: str | Ref,
    dag_selector: str | Ref | None = None,
    revision: str | None = None,
    commit_ops,
    dag_ops,
    head_ops,
    project_dir: str,
    operation: str,
) -> ResolvedNode:
    node_ref = _coerce_ref(value, "node")
    if node_ref is not None:
        return ResolvedNode(ref=node_ref, selector=node_ref.to)

    if not isinstance(value, str) or not value:
        raise DmlRepoError("Node selector is required")

    if dag_selector is not None:
        resolved_dag = resolve_dag_ref(
            value=dag_selector,
            revision=revision,
            commit_ops=commit_ops,
            head_ops=head_ops,
            project_dir=project_dir,
            operation=operation,
        )
        return ResolvedNode(
            ref=dag_ops.get_node(resolved_dag.ref, value),
            selector=value,
            dag_selector=resolved_dag.selector,
            revision=resolved_dag.revision,
        )

    resolved_revision = resolve_revision(
        value=revision or "HEAD",
        commit_ops=commit_ops,
        head_ops=head_ops,
        project_dir=project_dir,
    )
    matches: list[tuple[str, Ref]] = []
    for dag_name, dag_ref in _list_commit_dags(commit=resolved_revision.commit, commit_ops=commit_ops).items():
        try:
            dag_ops.get_node(dag_ref, value)
        except DmlRepoError:
            continue
        matches.append((dag_name, dag_ref))

    if not matches:
        raise DmlRepoError(f"Node '{value}' not found at revision {revision or 'HEAD'}")
    if len(matches) > 1:
        dag_names = ", ".join(name for name, _dag_ref in matches)
        raise DmlRepoError(
            f"dml dag {operation} requires dag_selector for ambiguous node lookup '{value}' (matches: {dag_names})"
        )

    matched_name, matched_dag_ref = matches[0]
    return ResolvedNode(
        ref=dag_ops.get_node(matched_dag_ref, value),
        selector=value,
        dag_selector=matched_name,
        revision=resolved_revision,
    )
