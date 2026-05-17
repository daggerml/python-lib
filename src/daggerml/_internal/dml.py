from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypedDict, cast, overload

from daggerml._internal._db import DmlDbEnv, Ref
from daggerml._internal.config import DmlProjectConfig, parse_dml_project_uri, run_project_hooks
from daggerml._internal.dml_context import (
    config_dict,
    current_head_branch,
    current_head_state,
    db_path_for_project,
    gitignore_exists,
    load_project_config,
    mutable_branch,
    project_config_exists,
    project_remote_uri,
    require_project_home,
    require_user,
    resolve_runtime_context,
)
from daggerml._internal.dml_resolution import (
    resolve_dag_ref,
    resolve_node_ref,
)
from daggerml._internal.dml_resolution import (
    resolve_revision as resolve_revision_value,
)
from daggerml._internal.dml_resolution import (
    resolve_revision_ref as resolve_revision_ref_value,
)
from daggerml._internal.ops.cache import CacheOps
from daggerml._internal.ops.commit import CommitOps
from daggerml._internal.ops.config import ConfigOps
from daggerml._internal.ops.dag import DagOps
from daggerml._internal.ops.gc import GcOps
from daggerml._internal.ops.head import HeadOps
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.ops.node import NodeOps
from daggerml._internal.ops.remote import RemoteOps
from daggerml._internal.types import DEFAULT_HEAD, NAMESPACES, DmlRepoError

# FIXME: remove the `id` and `ref` duplication and just return with something descriptive like `dag: Ref` (if its a dag)
_RUNTIME_CANCEL_MAX_ATTEMPTS = 3
_RUNTIME_CANCEL_BACKOFF_SECONDS = 0.05

logger = logging.getLogger(__name__)

_DB_MAP_SIZE = 1024**3


class ProjectConfigPayload(TypedDict):
    home: str | None
    uri: str | None


class DbConfigPayload(TypedDict):
    path: str | None


class RemoteConfigPayload(TypedDict):
    root: str
    fetch_workers: int


HooksConfigPayload = TypedDict("HooksConfigPayload", {"post-init": list[str]})


class ConfigShowPayload(TypedDict):
    project: ProjectConfigPayload
    db: DbConfigPayload
    remote: RemoteConfigPayload
    user: str | None
    default_branch: str
    hooks: HooksConfigPayload
    config_home: str


class ConfigShowContribPayload(ConfigShowPayload):
    contrib: dict[str, Any]


class CommitPayload(TypedDict):
    id: str
    parents: list[Ref]
    tree: Ref
    author: str | None
    message: str | None
    dag: Ref | None
    created: int
    modified: int


class RevisionPayload(TypedDict):
    input: str
    kind: str
    commit: Ref
    branch: str | None
    tag: str | None


class DagSummaryPayload(TypedDict):
    id: str
    nodes: list[Ref]
    names: dict[str, Ref]
    result: Ref | None
    argv: Ref | None
    kwargv: Ref | None
    ref: Ref


class NodeDescriptionPayload(TypedDict, total=False):
    id: str
    ref: Ref
    type: str
    value_ref: Ref
    dag: Ref
    argv: list[Ref]
    node: Ref


class DagPayload(TypedDict):
    id: str
    nodes: list[NodeDescriptionPayload]
    names: dict[str, Ref]
    result: Ref | None
    argv: Ref | None
    kwargv: Ref | None
    ref: Ref


class NodeSelectorPayload(TypedDict):
    selector: str
    dag_selector: str | None
    node: Any


class NodeSelectorWithRevisionPayload(NodeSelectorPayload):
    revision: RevisionPayload


class DagListPayload(TypedDict):
    revision: RevisionPayload
    dags: dict[str, Ref]


class DagDescribePayload(TypedDict):
    selector: str
    dag: DagSummaryPayload


class DagDescribeWithRevisionPayload(DagDescribePayload):
    revision: RevisionPayload


class DagGetPayload(TypedDict):
    selector: str
    dag: DagPayload


class DagGetWithRevisionPayload(DagGetPayload):
    revision: RevisionPayload


class DagMapDiffPayload(TypedDict):
    added: dict[str, Ref]
    removed: dict[str, Ref]
    updated: dict[str, dict[str, Ref]]


class ShowChangePayload(DagMapDiffPayload):
    base: Ref | None


class LogPayload(TypedDict):
    revision: RevisionPayload
    commits: list[CommitPayload]


class ShowPayload(TypedDict):
    revision: RevisionPayload
    commit: CommitPayload
    dags: dict[str, Ref]
    change: ShowChangePayload


class DiffPayload(DagMapDiffPayload):
    left: RevisionPayload
    right: RevisionPayload


class HeadStatePayload(TypedDict):
    mode: str
    branch: str | None
    commit: Ref


class StatusPayload(TypedDict):
    head: HeadStatePayload | None
    branches: list[str]
    dags: dict[str, Ref]
    indexes: list[str]


class BranchLocalPayload(TypedDict):
    branches: list[str]
    head: str | None
    remote: Literal[False]


class BranchRemotePayload(TypedDict):
    branches: list[str]
    remote: Literal[True]


class CheckoutAttachedPayload(TypedDict):
    mode: Literal["attached"]
    branch: str


class CheckoutDetachedPayload(TypedDict):
    mode: Literal["detached"]
    branch: None


class IndexDescribePayload(TypedDict):
    id: str
    commit: Ref
    dag: Ref | None
    nodes: list[Ref]
    names: dict[str, Ref]
    result: Ref | None
    argv: Ref | None
    kwargv: Ref | None


class RuntimeCancelPayload(TypedDict):
    index_id: str
    iterations: int
    graph_edges: int
    candidate_count: int
    own_execution_count: int
    cancelled_count: int
    dropped_count: int
    lock_retry_count: int


class IndexCommitPayload(TypedDict):
    ref: Ref
    summary: CommitPayload


class AdminIndexItemPayload(TypedDict):
    id: str
    commit: IndexCommitPayload
    dag: Ref | None
    nodes: list[Ref]
    names: dict[str, Ref]
    result: Ref | None
    argv: Ref | None
    kwargv: Ref | None


class AdminIndexListPayload(TypedDict):
    indexes: list[AdminIndexItemPayload]


class AdminIndexGetPayload(TypedDict):
    index: AdminIndexItemPayload


class AdminIndexDeletePayload(TypedDict):
    index: str
    deleted: Literal[True]


class AdminCacheInvalidatePayload(TypedDict):
    cache_keys: list[str]
    invalidated: dict[str, Any]


class AdminRemoteProjectsPayload(TypedDict):
    projects: list[str]


class AdminRemoteProjectRefsPayload(TypedDict):
    project: str
    branches: list[str]
    tags: list[str]


class AdminRemoteGcPayload(TypedDict):
    deleted: int
    kept_live: int
    kept_young: int


MalformedPolicy = Literal["raise", "warn", "ignore"]


class AdminGcDryRunPayload(TypedDict):
    dry_run: Literal[True]
    would_delete: int
    orphans: list[Ref]


class AdminGcRunPayload(TypedDict):
    dry_run: Literal[False]
    deleted: int


class InitCreatedStatePayload(TypedDict):
    db: bool
    config: bool


class InitPayload(TypedDict):
    project_home: str
    remote_uri: str | None
    user: str | None
    config_home: str | None
    created: InitCreatedStatePayload


def stringify_node_selector(node_selector: str | Ref) -> str:
    return node_selector.to if isinstance(node_selector, Ref) else node_selector


@contextmanager
def with_db(dml: "Dml", map_size: int = _DB_MAP_SIZE):
    project_home = require_project_home(dml._context.project_home)
    db = DmlDbEnv.open(str(db_path_for_project(project_home)), namespaces=sorted(NAMESPACES), map_size=map_size)
    try:
        yield db
    finally:
        db.close()


def create_db(project_home: str, *, branch: str | None = None) -> None:
    branch = branch or DEFAULT_HEAD
    db_path = db_path_for_project(project_home)
    db_path.mkdir(parents=True, exist_ok=True)
    db = DmlDbEnv.create(str(db_path), namespaces=sorted(NAMESPACES), map_size=_DB_MAP_SIZE)
    try:
        head_ops = HeadOps(_db=db)
        head_ops.create_branch(branch)
        head_ops.write_attached_head(branch)
    finally:
        db.close()


def make_commit_ops(db: DmlDbEnv) -> CommitOps:
    return CommitOps(_db=db)


def make_head_ops(db: DmlDbEnv) -> HeadOps:
    return HeadOps(_db=db)


def make_index_ops(db: DmlDbEnv, dml: "Dml") -> IndexOps:
    return IndexOps(_db=db, remote_root=dml._context.remote_uri)


def make_dag_ops(db: DmlDbEnv) -> DagOps:
    return DagOps(_db=db)


def make_node_ops(db: DmlDbEnv) -> NodeOps:
    return NodeOps(_db=db)


def make_cache_ops(db: DmlDbEnv, dml: "Dml") -> CacheOps:
    return CacheOps(_db=db, remote_root=dml._context.remote_uri)


def make_gc_ops(db: DmlDbEnv) -> GcOps:
    return GcOps(_db=db)


def split_remote_root(remote_root: str) -> tuple[str, str]:
    if not remote_root.startswith("s3://"):
        raise ValueError(f"Invalid remote root URI: {remote_root!r}")
    rest = remote_root[5:]
    if not rest:
        raise ValueError(f"Invalid remote root URI: {remote_root!r}")
    if "/" not in rest:
        return rest, "dml"
    bucket, prefix = rest.split("/", 1)
    prefix = prefix.strip("/")
    return bucket, f"{prefix}/dml" if prefix else "dml"


def make_remote_ops(db: DmlDbEnv, dml: "Dml", *, client=None) -> RemoteOps:
    bucket, prefix = split_remote_root(dml._context.remote_uri)
    remote_kwargs: dict[str, Any] = {
        "bucket": bucket,
        "prefix": prefix,
        "fetch_workers": dml._context.config.remote.fetch_workers,
    }
    if client is not None:
        remote_kwargs["client"] = client
    return RemoteOps(_db=db, **remote_kwargs)


def config_ops(dml: "Dml"):
    return ConfigOps(project_home=dml._context.project_home, config_home=dml._context.config.config_home)


def tree_dags(dml: "Dml", tree_ref: Ref) -> dict[str, Ref]:
    with with_db(dml) as db:
        with make_commit_ops(db)._tx(readonly=True) as txn:
            tree = txn.get(tree_ref)
            return dict(tree.dags)


def dag_map_for_commit(dml: "Dml", commit_ref: Ref) -> dict[str, Ref]:
    with with_db(dml) as db:
        tree_ref = make_commit_ops(db).describe(commit_ref)["tree"]
    return tree_dags(dml, tree_ref)


def dag_summary_payload(dml: "Dml", dag_ref: Ref) -> DagSummaryPayload:
    with with_db(dml) as db:
        dag = cast(DagSummaryPayload, dict(make_dag_ops(db).describe(dag_ref)))
    dag["ref"] = dag_ref
    return dag


def dag_map_diff(left: dict[str, Ref], right: dict[str, Ref]) -> DagMapDiffPayload:
    added: dict[str, Ref] = {}
    removed: dict[str, Ref] = {}
    updated: dict[str, dict[str, Ref]] = {}
    for name in sorted(set(left) | set(right)):
        before = left.get(name)
        after = right.get(name)
        if before is None and after is not None:
            added[name] = after
        elif before is not None and after is None:
            removed[name] = before
        elif before is not None and after is not None and before != after:
            updated[name] = {"before": before, "after": after}
    return {"added": added, "removed": removed, "updated": updated}


def revision_payload(value: str, resolved) -> RevisionPayload:
    return {
        "input": value,
        "kind": resolved.kind,
        "commit": resolved.commit,
        "branch": resolved.branch,
        "tag": resolved.tag,
    }


def remote_tracking_branches(dml: "Dml") -> list[str]:
    project_home = require_project_home(dml._context.project_home)
    remote_root = Path(project_home) / ".dml" / "refs" / "remote"
    if not remote_root.exists():
        return []
    branches: list[str] = []
    for ref_path in sorted(remote_root.glob("*/*/heads/**/*")):
        if not ref_path.is_file() or ref_path.name.endswith(".lock"):
            continue
        relative = ref_path.relative_to(remote_root)
        parts = relative.parts
        if len(parts) < 4:
            continue
        owner = parts[0]
        project = parts[1]
        branch_name = "/".join(parts[3:])
        branches.append(f"dml://{owner}/{project}#{branch_name}")
    return branches


def dag_payload(dml: "Dml", dag_ref: Ref) -> DagPayload:
    summary = dag_summary_payload(dml, dag_ref)
    node_refs = list(summary["nodes"])
    with with_db(dml) as db:
        node_ops = make_node_ops(db)
        nodes = [cast(NodeDescriptionPayload, node_ops.describe(node_ref)) for node_ref in node_refs]
    return {
        "id": summary["id"],
        "nodes": nodes,
        "names": summary["names"],
        "result": summary["result"],
        "argv": summary["argv"],
        "kwargv": summary["kwargv"],
        "ref": summary["ref"],
    }


def resolve_dml_revision(dml: "Dml", value: str):
    with with_db(dml) as db:
        return resolve_revision_value(
            value=value,
            commit_ops=make_commit_ops(db),
            head_ops=make_head_ops(db),
            project_dir=require_project_home(dml._context.project_home),
        )


def resolve_dml_revision_ref(dml: "Dml", value: str) -> Ref:
    with with_db(dml) as db:
        return resolve_revision_ref_value(
            value=value,
            commit_ops=make_commit_ops(db),
            head_ops=make_head_ops(db),
            project_dir=require_project_home(dml._context.project_home),
        )


def create_s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client("s3", config=Config(max_pool_connections=20))


@dataclass(frozen=True)
class _ConfigNamespace:
    _dml: "Dml"

    def get(self, key: str, *, scope: Literal["global", "local"] = "local"):
        return config_ops(self._dml).get(key, scope=scope)

    def set(self, key: str, values: list[str], *, scope: Literal["global", "local"] = "local"):
        return config_ops(self._dml).set(key, values, scope=scope)

    @overload
    def show(self, *, contrib: Literal[False] = False) -> ConfigShowPayload: ...

    @overload
    def show(self, *, contrib: Literal[True]) -> ConfigShowContribPayload: ...

    def show(self, *, contrib: bool = False) -> ConfigShowPayload:
        payload = cast(ConfigShowPayload, config_dict(self._dml._context.config))
        if contrib:
            from daggerml.contrib import status as contrib_status

            return cast(ConfigShowContribPayload, {**payload, "contrib": contrib_status.status()})
        return payload


@dataclass(frozen=True)
class _RuntimeNamespace:
    _dml: "Dml"

    def create(
        self,
        *,
        head: str | None = None,
        commit: Ref | None = None,
        argv_ptr: str | None = None,
        index_id: str | None = None,
    ) -> str:
        with with_db(self._dml) as db:
            if head is None and commit is None and argv_ptr is None:
                head_state = make_head_ops(db).get_head_state()
                head = head_state.branch
                commit = head_state.commit if head is None else None
            return make_index_ops(db, self._dml).create(head=head, commit=commit, argv_ptr=argv_ptr, index_id=index_id)

    def describe(self, index_id: str) -> IndexDescribePayload:
        with with_db(self._dml) as db:
            return cast(IndexDescribePayload, make_index_ops(db, self._dml).describe(index_id))

    def get_node(self, index_id: str, name: str) -> Ref:
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).get_node(index_id, name)

    def get_argv(self, index_id: str) -> Ref:
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).get_argv(index_id)

    def put_literal(self, index_id: str, value: Any, *, name: str | None = None) -> Ref:
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).put_literal(index_id, value, name=name)

    def put_import(self, index_id: str, dag: Ref, *, node: Ref | None = None, name: str | None = None) -> Ref:
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).put_import(index_id, dag, node=node, name=name)

    def set_node_name(self, index_id: str, name: str, node: Ref) -> Ref:
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).set_node_name(index_id, name, node)

    def start_fn(
        self,
        index_id: str,
        argv: list[Ref],
        *,
        kwargv: dict[str, Ref] | None = None,
        name: str | None = None,
    ) -> Ref | None:
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).start_fn(index_id, argv, kwargv=kwargv, name=name)

    def cancel(self, index_id: str) -> RuntimeCancelPayload:
        requested_by = require_user(self._dml._context.user, message="user is required for runtime cancel")
        with with_db(self._dml) as db:
            index = make_index_ops(db, self._dml)
            plan = index.cancel(index_id, requested_by=requested_by)
            candidate_set = set(cast(set[str], plan["candidate_set"]))
            own_executions = set(cast(set[str], plan["own_executions"]))
            retry_counts = {candidate_id: 0 for candidate_id in candidate_set}
            adapter_retry_candidates: set[str] = set()
            stats: RuntimeCancelPayload = {
                "index_id": index_id,
                "iterations": 0,
                "graph_edges": len(cast(set[tuple[str, str]], plan["graph"])),
                "candidate_count": len(candidate_set),
                "own_execution_count": 0,
                "cancelled_count": 0,
                "dropped_count": 0,
                "lock_retry_count": 0,
            }
            while candidate_set:
                stats["iterations"] += 1
                batch = sorted(candidate_set)
                normal_retry_pending = False
                logger.info(
                    "runtime.cancel iteration=%s index_id=%s candidates=%s owned=%s",
                    stats["iterations"],
                    index_id,
                    len(batch),
                    len(own_executions),
                )
                with ThreadPoolExecutor() as executor:
                    futures = {
                        executor.submit(
                            index._cancel_execution_candidate,
                            candidate_id,
                            requested_by=requested_by,
                            own_executions=set(own_executions),
                        ): candidate_id
                        for candidate_id in batch
                    }
                    for future in as_completed(futures):
                        candidate_id = futures[future]
                        try:
                            result = future.result()
                        except Exception as exc:
                            retry_counts[candidate_id] += 1
                            if retry_counts[candidate_id] >= _RUNTIME_CANCEL_MAX_ATTEMPTS:
                                raise DmlRepoError(
                                    f"runtime.cancel exceeded retry limit for execution {candidate_id}: {exc}"
                                ) from exc
                            adapter_retry_candidates.add(candidate_id)
                            continue
                        if cast(bool, result["lock_retry"]):
                            stats["lock_retry_count"] += 1
                            normal_retry_pending = True
                        outcome = cast(int | None, result["outcome"])
                        if outcome == 1:
                            candidate_set.discard(candidate_id)
                            stats["cancelled_count"] += 1
                        elif outcome == -1:
                            candidate_set.discard(candidate_id)
                            if candidate_id in own_executions:
                                own_executions.discard(candidate_id)
                                stats["dropped_count"] += 1
                        elif outcome is None:
                            normal_retry_pending = True
                if adapter_retry_candidates or normal_retry_pending:
                    delay = _RUNTIME_CANCEL_BACKOFF_SECONDS
                    if adapter_retry_candidates:
                        delay *= 2 ** (max(retry_counts[candidate_id] for candidate_id in adapter_retry_candidates) - 1)
                    time.sleep(delay)
                    adapter_retry_candidates.clear()
            index._complete_index_cancellation(
                index_id,
                cancelled_path=cast(Path, plan["cancelled_path"]),
                own_executions=own_executions,
            )
            stats["own_execution_count"] = len(own_executions)
            logger.info(
                "runtime.cancel complete index_id=%s iterations=%s cancelled=%s dropped=%s lock_retries=%s",
                index_id,
                stats["iterations"],
                stats["cancelled_count"],
                stats["dropped_count"],
                stats["lock_retry_count"],
            )
            return stats

    def commit(
        self,
        index_id: str,
        value: Ref | Any,
        *,
        head: str | None = None,
        message: str | None = None,
        dag_name: str | None = None,
    ) -> Ref:
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).commit(index_id, value, head=head, message=message, dag_name=dag_name)


@dataclass(frozen=True)
class _DagNamespace:
    _dml: "Dml"

    def list(self, revision: str = "HEAD") -> DagListPayload:
        resolved = resolve_dml_revision(self._dml, revision)
        return {
            "revision": revision_payload(revision, resolved),
            "dags": dag_map_for_commit(self._dml, resolved.commit),
        }

    @overload
    def describe(self, value: str | Ref, *, revision: None = None) -> DagDescribePayload: ...

    @overload
    def describe(self, value: str | Ref, *, revision: str) -> DagDescribeWithRevisionPayload: ...

    def describe(self, value: str | Ref, *, revision: str | None = None) -> DagDescribePayload:
        with with_db(self._dml) as db:
            resolved = resolve_dag_ref(
                value=value,
                revision=revision,
                commit_ops=make_commit_ops(db),
                head_ops=make_head_ops(db),
                project_dir=require_project_home(self._dml._context.project_home),
                operation="describe",
            )
        payload: DagDescribePayload = {
            "selector": resolved.selector,
            "dag": dag_summary_payload(self._dml, resolved.ref),
        }
        if resolved.revision is not None:
            return cast(
                DagDescribeWithRevisionPayload,
                {**payload, "revision": revision_payload(revision or "HEAD", resolved.revision)},
            )
        return payload

    @overload
    def get(self, value: str | Ref, *, revision: None = None) -> DagGetPayload: ...

    @overload
    def get(self, value: str | Ref, *, revision: str) -> DagGetWithRevisionPayload: ...

    def get(self, value: str | Ref, *, revision: str | None = None) -> DagGetPayload:
        with with_db(self._dml) as db:
            resolved = resolve_dag_ref(
                value=value,
                revision=revision,
                commit_ops=make_commit_ops(db),
                head_ops=make_head_ops(db),
                project_dir=require_project_home(self._dml._context.project_home),
                operation="get",
            )
        payload: DagGetPayload = {"selector": resolved.selector, "dag": dag_payload(self._dml, resolved.ref)}
        if resolved.revision is not None:
            return cast(
                DagGetWithRevisionPayload,
                {**payload, "revision": revision_payload(revision or "HEAD", resolved.revision)},
            )
        return payload

    @overload
    def describe_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: None = None,
    ) -> NodeSelectorPayload: ...

    @overload
    def describe_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: str,
    ) -> NodeSelectorWithRevisionPayload: ...

    def describe_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: str | None = None,
    ) -> NodeSelectorPayload:
        with with_db(self._dml) as db:
            resolved = resolve_node_ref(
                value=node_selector,
                dag_selector=dag_selector,
                revision=revision,
                commit_ops=make_commit_ops(db),
                dag_ops=make_dag_ops(db),
                head_ops=make_head_ops(db),
                project_dir=require_project_home(self._dml._context.project_home),
                operation="describe-node",
            )
            described_node = make_node_ops(db).describe(resolved.ref)
        payload: NodeSelectorPayload = {
            "selector": stringify_node_selector(node_selector),
            "dag_selector": resolved.dag_selector,
            "node": described_node,
        }
        if resolved.revision is not None:
            return cast(
                NodeSelectorWithRevisionPayload,
                {**payload, "revision": revision_payload(revision or "HEAD", resolved.revision)},
            )
        return payload

    @overload
    def get_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: None = None,
    ) -> NodeSelectorPayload: ...

    @overload
    def get_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: str,
    ) -> NodeSelectorWithRevisionPayload: ...

    def get_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: str | None = None,
    ) -> NodeSelectorPayload:
        with with_db(self._dml) as db:
            resolved = resolve_node_ref(
                value=node_selector,
                dag_selector=dag_selector,
                revision=revision,
                commit_ops=make_commit_ops(db),
                dag_ops=make_dag_ops(db),
                head_ops=make_head_ops(db),
                project_dir=require_project_home(self._dml._context.project_home),
                operation="get-node",
            )
            node_value = make_node_ops(db).get(resolved.ref)
        payload: NodeSelectorPayload = {
            "selector": stringify_node_selector(node_selector),
            "dag_selector": resolved.dag_selector,
            "node": node_value,
        }
        if resolved.revision is not None:
            return cast(
                NodeSelectorWithRevisionPayload,
                {**payload, "revision": revision_payload(revision or "HEAD", resolved.revision)},
            )
        return payload

    @overload
    def unroll_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: None = None,
    ) -> NodeSelectorPayload: ...

    @overload
    def unroll_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: str,
    ) -> NodeSelectorWithRevisionPayload: ...

    def unroll_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: str | None = None,
    ) -> NodeSelectorPayload:
        with with_db(self._dml) as db:
            resolved = resolve_node_ref(
                value=node_selector,
                dag_selector=dag_selector,
                revision=revision,
                commit_ops=make_commit_ops(db),
                dag_ops=make_dag_ops(db),
                head_ops=make_head_ops(db),
                project_dir=require_project_home(self._dml._context.project_home),
                operation="unroll-node",
            )
            unrolled_node = make_node_ops(db).unroll(resolved.ref)
        payload: NodeSelectorPayload = {
            "selector": stringify_node_selector(node_selector),
            "dag_selector": resolved.dag_selector,
            "node": unrolled_node,
        }
        if resolved.revision is not None:
            return cast(
                NodeSelectorWithRevisionPayload,
                {**payload, "revision": revision_payload(revision or "HEAD", resolved.revision)},
            )
        return payload

    def checkout(
        self,
        revision: str,
        dag_name: str,
        *,
        branch: str | None = None,
        target_name: str | None = None,
        replace: bool = False,
        user: str | None = None,
    ) -> Ref:
        author = require_user(user or self._dml._context.user, message="user is required for dag checkout")
        resolved_revision = resolve_dml_revision_ref(self._dml, revision)
        with with_db(self._dml) as db:
            target_branch = mutable_branch(branch=branch, head_ops=make_head_ops(db))
            return make_commit_ops(db).checkout_dag(
                target_branch,
                resolved_revision,
                dag_name,
                target_name=target_name,
                replace=replace,
                user=author,
            )

    def delete(self, name: str, *, branch: str | None = None, user: str | None = None):
        author = require_user(user or self._dml._context.user, message="user is required for dag delete")
        with with_db(self._dml) as db:
            return make_commit_ops(db).delete_dag(name, branch, author)


@dataclass(frozen=True)
class _AdminIndexNamespace:
    _dml: "Dml"

    def list(self) -> AdminIndexListPayload:
        with with_db(self._dml) as db:
            indexes = [self.get(index_id)["index"] for index_id in make_head_ops(db).list_indexes()]
        return {"indexes": indexes}

    def get(self, index_id: str) -> AdminIndexGetPayload:
        with with_db(self._dml) as db:
            index = dict(make_index_ops(db, self._dml).describe(index_id))
            commit_ref = index["commit"]
            index["commit"] = {
                "ref": commit_ref,
                "summary": make_commit_ops(db).describe(commit_ref),
            }
        return {"index": cast(AdminIndexItemPayload, index)}

    def delete(self, index_id: str) -> AdminIndexDeletePayload:
        with with_db(self._dml) as db:
            make_index_ops(db, self._dml).delete(index_id)
        return {"index": index_id, "deleted": True}


@dataclass(frozen=True)
class _AdminCacheNamespace:
    _dml: "Dml"

    def invalidate(self, cache_keys: list[str]) -> AdminCacheInvalidatePayload:
        if not cache_keys:
            raise DmlRepoError("At least one cache key is required")
        for cache_key in cache_keys:
            if ":" in cache_key:
                raise DmlRepoError("Admin cache invalidation accepts exact cache keys only")
        requested_by = self._dml._context.user or "cli"
        with with_db(self._dml) as db:
            invalidated = make_remote_ops(db, self._dml).invalidate_cache(cache_keys, requested_by=requested_by)
        return {"cache_keys": cache_keys, "invalidated": invalidated}


@dataclass(frozen=True)
class _AdminRemoteNamespace:
    _dml: "Dml"

    @overload
    def list(self, project: None = None, *, owner: str | None = None) -> AdminRemoteProjectsPayload: ...

    @overload
    def list(self, project: str, *, owner: None = None) -> AdminRemoteProjectRefsPayload: ...

    def list(
        self, project: str | None = None, *, owner: str | None = None
    ) -> AdminRemoteProjectsPayload | AdminRemoteProjectRefsPayload:
        with with_db(self._dml) as db:
            remote = make_remote_ops(db, self._dml)
            refs = remote.list("projects")
        if project is None:
            projects: set[str] = set()
            for ref in refs:
                ref_path = ref.get("ref_path")
                if not isinstance(ref_path, str):
                    continue
                parts = ref_path.split("/")
                if len(parts) < 5 or parts[0] != "projects":
                    continue
                candidate_owner = parts[1]
                candidate_project = parts[2]
                if owner is not None and candidate_owner != owner:
                    continue
                projects.add(f"dml://{candidate_owner}/{candidate_project}")
            return {"projects": sorted(projects)}

        if not project.startswith("dml://") or "#" in project or "@" in project:
            raise DmlRepoError("Admin remote list expects a bare dml://<owner>/<project> project URI")
        with with_db(self._dml) as db:
            parsed = make_remote_ops(db, self._dml).parse_dml_uri(project, require_identifier=False)
        branches: list[str] = []
        tags: list[str] = []
        for ref in refs:
            ref_path = ref.get("ref_path")
            if not isinstance(ref_path, str):
                continue
            parts = ref_path.split("/")
            if len(parts) < 5 or parts[0] != "projects" or parts[1] != parsed.owner or parts[2] != parsed.project:
                continue
            name = "/".join(parts[4:])
            if not name.endswith(".json"):
                continue
            name = name[:-5]
            if parts[3] == "heads":
                branches.append(f"dml://{parsed.owner}/{parsed.project}#{name}")
            elif parts[3] == "tags":
                tags.append(f"dml://{parsed.owner}/{parsed.project}@{name}")
        return {"project": project, "branches": sorted(branches), "tags": sorted(tags)}

    def gc(self, *, min_age_seconds: int = 24 * 3600, malformed: MalformedPolicy = "warn") -> AdminRemoteGcPayload:
        with with_db(self._dml) as db:
            return cast(
                AdminRemoteGcPayload,
                make_remote_ops(db, self._dml).gc(min_age_seconds=min_age_seconds, malformed=malformed),
            )


@dataclass(frozen=True)
class _AdminNamespace:
    _dml: "Dml"

    @property
    def index(self) -> _AdminIndexNamespace:
        return _AdminIndexNamespace(self._dml)

    @property
    def cache(self) -> _AdminCacheNamespace:
        return _AdminCacheNamespace(self._dml)

    @property
    def remote(self) -> _AdminRemoteNamespace:
        return _AdminRemoteNamespace(self._dml)

    @overload
    def gc(self, *, dry_run: Literal[False] = False) -> AdminGcRunPayload: ...

    @overload
    def gc(self, *, dry_run: Literal[True]) -> AdminGcDryRunPayload: ...

    def gc(self, *, dry_run: bool = False) -> AdminGcDryRunPayload | AdminGcRunPayload:
        with with_db(self._dml) as db:
            gc_ops = make_gc_ops(db)
            if dry_run:
                orphans = gc_ops.list_orphans()
                return {"dry_run": True, "would_delete": len(orphans), "orphans": orphans}
            return {"dry_run": False, "deleted": sum(gc_ops.gc().values())}


class Dml:
    def __init__(
        self,
        project_home: str | None = None,
        *,
        remote_uri: str | None = None,
        user: str | None = None,
        config_home: str | None = None,
    ):
        self._context = resolve_runtime_context(
            project_home=project_home,
            remote_uri=remote_uri,
            user=user,
            config_home=config_home,
        )

    @property
    def config(self) -> _ConfigNamespace:
        return _ConfigNamespace(self)

    @property
    def runtime(self) -> _RuntimeNamespace:
        return _RuntimeNamespace(self)

    @property
    def dag(self) -> _DagNamespace:
        return _DagNamespace(self)

    @property
    def admin(self) -> _AdminNamespace:
        return _AdminNamespace(self)

    def status(self) -> StatusPayload:
        if not self._context.project_home or not project_config_exists(
            require_project_home(self._context.project_home)
        ):
            return {
                "head": None,
                "branches": [],
                "dags": {},
                "indexes": [],
            }
        with with_db(self) as db:
            current_head_ops = make_head_ops(db)
            head_state = current_head_state(current_head_ops)
            return {
                "head": {
                    "mode": head_state.mode,
                    "branch": head_state.branch,
                    "commit": head_state.commit,
                },
                "branches": current_head_ops.list_branches(),
                "dags": dag_map_for_commit(self, head_state.commit),
                "indexes": current_head_ops.list_indexes(),
            }

    @overload
    def branch(self, *, remote: Literal[False] = False) -> BranchLocalPayload: ...
    @overload
    def branch(self, *, remote: Literal[True]) -> BranchRemotePayload: ...
    def branch(self, *, remote: bool = False) -> BranchLocalPayload | BranchRemotePayload:
        if remote:
            return {"branches": remote_tracking_branches(self), "remote": True}
        with with_db(self) as db:
            current_head_ops = make_head_ops(db)
            return {
                "branches": current_head_ops.list_branches(),
                "head": current_head_branch(current_head_ops),
                "remote": False,
            }

    def log(self, revision: str = "HEAD", *, limit: int | None = None) -> LogPayload:
        resolved = resolve_dml_revision(self, revision)
        with with_db(self) as db:
            commit_ops = make_commit_ops(db)
            refs = list(commit_ops.list(resolved.commit, limit=limit))
            commits = [cast(CommitPayload, commit_ops.describe(ref)) for ref in refs]
        return {
            "revision": revision_payload(revision, resolved),
            "commits": commits,
        }

    def show(self, revision: str = "HEAD") -> ShowPayload:
        resolved = resolve_dml_revision(self, revision)
        with with_db(self) as db:
            commit = cast(CommitPayload, make_commit_ops(db).describe(resolved.commit))
        dags = dag_map_for_commit(self, resolved.commit)
        base_commit = commit["parents"][0] if commit["parents"] else None
        base_dags = dag_map_for_commit(self, base_commit) if base_commit is not None else {}
        return {
            "revision": revision_payload(revision, resolved),
            "commit": commit,
            "dags": dags,
            "change": {"base": base_commit, **dag_map_diff(base_dags, dags)},
        }

    def diff(self, left: str = "HEAD~1", right: str = "HEAD") -> DiffPayload:
        left_resolved = resolve_dml_revision(self, left)
        right_resolved = resolve_dml_revision(self, right)
        left_dags = dag_map_for_commit(self, left_resolved.commit)
        right_dags = dag_map_for_commit(self, right_resolved.commit)
        return {
            "left": revision_payload(left, left_resolved),
            "right": revision_payload(right, right_resolved),
            **dag_map_diff(left_dags, right_dags),
        }

    def checkout(self, revision: str) -> CheckoutAttachedPayload | CheckoutDetachedPayload:
        resolved = resolve_dml_revision(self, revision)
        with with_db(self) as db:
            current_head_ops = make_head_ops(db)
            if resolved.kind == "branch" and resolved.branch is not None:
                current_head_ops.write_attached_head(resolved.branch)
                return {"mode": "attached", "branch": resolved.branch}
            current_head_ops.write_detached_head(resolved.commit)
            return {"mode": "detached", "branch": None}

    def fetch(self, remote_or_uri: str, branch: str | None, *, s3_client=None) -> Ref:
        project_home = require_project_home(self._context.project_home)
        uri = project_remote_uri(
            project_home=project_home,
            remote_or_uri=remote_or_uri,
            branch=branch,
            default_branch=self._context.default_branch,
        )
        client = s3_client or create_s3_client()
        with with_db(self) as db:
            return make_remote_ops(db, self, client=client).fetch_uri(uri)

    def pull(
        self, remote_or_uri: str, remote_branch: str | None, *, branch: str | None, user: str, s3_client=None
    ) -> Ref:
        project_home = require_project_home(self._context.project_home)
        with with_db(self) as db:
            target_branch = mutable_branch(branch=branch, head_ops=make_head_ops(db))
        uri = project_remote_uri(
            project_home=project_home,
            remote_or_uri=remote_or_uri,
            branch=remote_branch or target_branch,
            default_branch=self._context.default_branch,
        )
        client = s3_client or create_s3_client()
        with with_db(self) as db:
            return make_remote_ops(db, self, client=client).pull_uri_into_branch(uri, target_branch, user=user)

    def push(self, tag: str | None, *, branch: str | None, create: bool, force: bool, s3_client=None) -> str:
        project = load_project_config(require_project_home(self._context.project_home))
        if not project.uri:
            raise DmlRepoError("remote.project is required for project sync")
        with with_db(self) as db:
            source_branch = branch or make_head_ops(db).require_attached_head_branch()
        client = s3_client or create_s3_client()
        with with_db(self) as db:
            remote = make_remote_ops(db, self, client=client)
            if tag:
                return remote.push_project_tag(f"{project.uri}@{tag}", source_branch)
            return remote.push_project_branch(
                f"{project.uri}#{source_branch}", source_branch, create=create, force=force
            )

    def merge(self, revision: str, branch: str | None, user: str):
        revision_ref = resolve_dml_revision_ref(self, revision)
        with with_db(self) as db:
            target_branch = mutable_branch(branch=branch, head_ops=make_head_ops(db))
            return make_commit_ops(db).merge_into_head(target_branch, revision_ref, user)

    def revert(self, revision: str, branch: str | None, user: str):
        revision_ref = resolve_dml_revision_ref(self, revision)
        with with_db(self) as db:
            target_branch = mutable_branch(branch=branch, head_ops=make_head_ops(db))
            return make_commit_ops(db).revert(target_branch, revision_ref, user)

    @classmethod
    def init(
        cls,
        project_home: str = ".",
        *,
        remote_uri: str | None = None,
        user: str | None = None,
        config_home: str | None = None,
        remote_project: str | None = None,
        no_hooks: bool = False,
    ) -> InitPayload:
        root = Path(project_home).resolve()
        if not root.exists():
            raise FileNotFoundError(f"{root} does not exist")
        project_home = str(root)
        dml_dir = root / ".dml"
        dml_dir.mkdir(parents=True, exist_ok=True)

        config_existed = project_config_exists(project_home)
        db_existed = db_path_for_project(project_home).exists()

        project_cfg: DmlProjectConfig
        if config_existed:
            project_cfg = load_project_config(project_home)
        else:
            if remote_project:
                parsed = parse_dml_project_uri(remote_project)
                project_cfg = DmlProjectConfig(name=parsed.project, owner=parsed.owner, remote_uri=remote_uri or "")
            else:
                project_cfg = DmlProjectConfig(remote_uri=remote_uri or "")

        if not gitignore_exists(project_home):
            (dml_dir / ".gitignore").write_text("db\nHEAD\nrefs\n")
        if not config_existed:
            project_cfg.save(root)

        runtime = cls(project_home=project_home, remote_uri=remote_uri, user=user, config_home=config_home)
        resolved_branch = runtime._context.default_branch

        if not config_existed and project_cfg.remote_uri != runtime._context.remote_uri:
            project_cfg = DmlProjectConfig(
                name=project_cfg.name,
                owner=project_cfg.owner,
                remote_uri=runtime._context.remote_uri,
            )
            project_cfg.save(root)

        if not db_existed:
            create_db(project_home, branch=resolved_branch)

        project_cfg = load_project_config(project_home)
        run_project_hooks(
            "post-init",
            runtime._context.config.hooks.post_init,
            project_dir=project_home,
            project=project_cfg,
            config_home=runtime._context.config.config_home,
            remote_name="origin" if runtime._context.remote_uri else None,
            no_hooks=no_hooks,
        )

        if project_cfg.remote_project and not runtime._context.remote_uri:
            raise DmlRepoError("remote.root is required")

        if project_cfg.remote_project and runtime._context.remote_uri:
            try:
                fetched = runtime.fetch("origin", None)
            except DmlRepoError:
                if config_existed and not db_existed:
                    raise
            else:
                with with_db(runtime) as db:
                    make_head_ops(db).write_detached_head(fetched)

        return {
            "project_home": project_home,
            "remote_uri": runtime._context.remote_uri,
            "user": runtime._context.user,
            "config_home": runtime._context.config.config_home,
            "created": {"db": not db_existed, "config": not config_existed},
        }
