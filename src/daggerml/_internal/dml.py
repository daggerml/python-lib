from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal, NotRequired, TypedDict, cast, overload

from daggerml._internal._db import DmlDbEnv, Ref
from daggerml._internal.config import DmlProjectConfig, parse_dml_project_uri
from daggerml._internal.dml_context import (
    config_dict,
    current_head_branch,
    current_head_state,
    db_path_for_project,
    gitignore_exists,
    load_project_config,
    mutable_branch,
    project_config_exists,
    project_remote_root,
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
from daggerml._internal.types import DEFAULT_HEAD, NAMESPACES, DmlRepoError, Error, Runnable, Uri

logger = logging.getLogger(__name__)

_RUNTIME_CANCEL_MAX_ATTEMPTS = 3
_RUNTIME_CANCEL_BACKOFF_SECONDS = 0.05
_DB_MAP_SIZE = 1024**3


class ProjectConfigPayload(TypedDict):
    home: str | None


class DbConfigPayload(TypedDict):
    path: str | None


class RemoteConfigPayload(TypedDict):
    project: str | None
    root: str
    fetch_workers: int


class ConfigShowPayload(TypedDict):
    project: ProjectConfigPayload
    db: DbConfigPayload
    remote: RemoteConfigPayload
    user: str | None
    default_branch: str
    config_home: str


class ConfigShowContribPayload(ConfigShowPayload):
    contrib: dict[str, Any]


class CommitPayload(TypedDict):
    ref: Ref
    parents: list[Ref]
    dags: dict[str, Ref]
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
    nodes: list[Ref]
    names: dict[str, Ref]
    result: Ref | None
    argv: Ref | None
    kwargv: Ref | None
    ref: Ref


class NodeDescriptionPayload(TypedDict):
    ref: Ref
    type: str
    dag: NotRequired[Ref]
    argv: NotRequired[list[Ref]]
    node: NotRequired[Ref]


NodeValue = None | int | float | str | bool | Uri | Runnable | list[Ref] | dict[str, Ref]
NodeUnrolledValue = None | int | float | str | bool | Uri | Runnable | list[Any] | dict[str, Any]


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


class BranchPayload(TypedDict):
    branches: list[str]
    head: str | None
    remote: bool


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


class RuntimeDeletePayload(TypedDict):
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
    remote_root: str | None
    user: str | None
    config_home: str | None
    created: InitCreatedStatePayload


def require_exact_ref(value: Ref, expected_root_ns: str) -> Ref:
    if not isinstance(value, Ref):
        raise DmlRepoError(f"Expected {expected_root_ns} Ref, got: {type(value).__name__}")
    if value.nss()[0] != expected_root_ns:
        raise DmlRepoError(f"Expected {expected_root_ns} ref, got: {value}")
    return value


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
    return IndexOps(_db=db, remote_root=dml._context.remote_root)


def make_dag_ops(db: DmlDbEnv) -> DagOps:
    return DagOps(_db=db)


def make_node_ops(db: DmlDbEnv) -> NodeOps:
    return NodeOps(_db=db)


def make_cache_ops(db: DmlDbEnv, dml: "Dml") -> CacheOps:
    return CacheOps(_db=db, remote_root=dml._context.remote_root)


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


def make_remote_ops(db: DmlDbEnv, dml: "Dml") -> RemoteOps:
    bucket, prefix = split_remote_root(dml._context.remote_root)
    remote_kwargs: dict[str, Any] = {
        "bucket": bucket,
        "prefix": prefix,
        "fetch_workers": dml._context.config.remote.fetch_workers,
        "client": dml._s3_client,
    }
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
        dag = dict(make_dag_ops(db).describe(dag_ref))
    dag.pop("id", None)
    dag["ref"] = dag_ref
    return cast(DagSummaryPayload, dag)


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


def commit_payload(dml: "Dml", commit_ref: Ref, summary: dict[str, Any]) -> CommitPayload:
    summary = dict(summary)
    summary["dags"] = tree_dags(dml, summary.pop("tree"))
    summary.pop("id", None)
    summary["ref"] = commit_ref
    return cast(CommitPayload, summary)


def node_description_payload(node_info: dict[str, Any]) -> NodeDescriptionPayload:
    payload = dict(node_info)
    # payload.pop("id", None)
    return cast(NodeDescriptionPayload, payload)


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
    """Read and update resolved DaggerML configuration values."""

    _dml: "Dml"

    def get(
        self,
        key: Annotated[str, "Configuration setting to resolve, such as remote.root or user."],
        *,
        scope: Annotated[Literal["global", "local"], "Config scope to read from."] = "local",
    ):
        """Return the resolved value for a configuration setting in the selected scope."""
        return config_ops(self._dml).get(key, scope=scope)

    def set(
        self,
        key: Annotated[str, "Configuration setting to update."],
        value: Annotated[str, "Replacement value to write for the setting."],
        scope: Annotated[Literal["global", "local"], "Config scope to update."] = "local",
    ):
        """Persist one configuration setting in the selected config file."""
        return config_ops(self._dml).set(key, value, scope=scope)

    @overload
    def show(self, *, contrib: Literal[False] = False) -> ConfigShowPayload: ...
    @overload
    def show(self, *, contrib: Literal[True]) -> ConfigShowContribPayload: ...
    def show(
        self,
        *,
        contrib: Annotated[bool, "Include contrib runtime status alongside core config data."] = False,
    ) -> ConfigShowPayload:
        """Return the active configuration payload, optionally with contrib status."""
        payload = cast(ConfigShowPayload, config_dict(self._dml._context.config))
        if contrib:
            from daggerml.contrib import status as contrib_status

            return cast(ConfigShowContribPayload, {**payload, "contrib": contrib_status.status()})
        return payload


@dataclass(frozen=True)
class _RuntimeNamespace:
    """Manage mutable runtime indexes and staged DAG execution state."""

    _dml: "Dml"

    ######## Dag runtime operations ########
    def create(
        self,
        *,
        head: Annotated[str | None, "Branch name to base the index on."] = None,
        commit: Annotated[Ref | None, "Commit to base the index on when not using a branch."] = None,
        argv_ptr: Annotated[
            str | None,
            "Remote argv pointer to import when resuming external execution state.",
        ] = None,
        index_id: Annotated[str | None, "Explicit runtime identifier to reuse or create."] = None,
    ) -> str:
        """Create a runtime workspace from HEAD, a branch, a commit, or an argv pointer."""
        with with_db(self._dml) as db:
            if head is None and commit is None and argv_ptr is None:
                head_state = make_head_ops(db).get_head_state()
                head = head_state.branch
                commit = head_state.commit if head is None else None
            return make_index_ops(db, self._dml).create(head=head, commit=commit, argv_ptr=argv_ptr, index_id=index_id)

    def get_node(
        self,
        index_id: Annotated[str, "Runtime workspace to read from."],
        name: Annotated[str, "Named node to resolve inside the runtime workspace."],
    ) -> Ref:
        """Return the stored identifier for a named node in a runtime workspace."""
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).get_node(index_id, name)

    def get_argv(self, index_id: Annotated[str, "Runtime workspace to read from."]) -> Ref:
        """Return the argv node for a runtime workspace."""
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).get_argv(index_id)

    def put_literal(
        self,
        index_id: Annotated[str, "Runtime workspace to mutate."],
        value: Annotated[Any, "Literal value to stage into the workspace."],
        *,
        name: Annotated[str | None, "Optional name to assign to the staged value."] = None,
    ) -> Ref:
        """Stage a literal value in the runtime workspace and optionally name it."""
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).put_literal(index_id, value, name=name)

    def put_import(
        self,
        index_id: Annotated[str, "Runtime workspace to mutate."],
        dag: Annotated[Ref, "Committed DAG to import from."],
        *,
        node: Annotated[Ref | None, "Specific node to import from that DAG."] = None,
        name: Annotated[str | None, "Optional name for the imported node."] = None,
    ) -> Ref:
        """Import a committed DAG node into the runtime workspace."""
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).put_import(index_id, dag, node=node, name=name)

    def set_node_name(
        self,
        index_id: Annotated[str, "Runtime workspace to mutate."],
        name: Annotated[str, "Name to bind inside the runtime index."],
        node: Annotated[Ref, "Existing node to bind to that name."],
    ) -> Ref:
        """Bind an existing node to a name in the runtime workspace."""
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).set_node_name(index_id, name, node)

    def start_fn(
        self,
        index_id: Annotated[str, "Runtime workspace to execute in."],
        argv: Annotated[list[Ref], "Ordered arguments with the callable in the first position."],
        *,
        kwargv: Annotated[dict[str, Ref] | None, "Optional keyword arguments keyed by parameter name."] = None,
        name: Annotated[str | None, "Optional result name for the staged call."] = None,
    ) -> Ref | None:
        """Stage a function call in the runtime workspace and return the result node."""
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).start_fn(index_id, argv, kwargv=kwargv, name=name)

    def commit(
        self,
        index_id: Annotated[str, "Runtime workspace to commit."],
        value: Annotated[Ref | Error, "Final DAG result as an existing node or an Error value."],
        *,
        head: Annotated[str | None, "Branch to update; defaults to the mutable current branch."] = None,
        message: Annotated[str | None, "Commit message to store with the new commit."] = None,
        dag_name: Annotated[str | None, "Optional DAG name to update in the target commit."] = None,
    ) -> Ref:
        """Commit a runtime workspace into repository history."""
        with with_db(self._dml) as db:
            return make_index_ops(db, self._dml).commit(index_id, value, head=head, message=message, dag_name=dag_name)

    ######## Meta runtime operations ########
    def list(self) -> list[str]:
        """List runtime workspaces currently tracked in the repository."""
        with with_db(self._dml) as db:
            return make_head_ops(db).list_indexes()

    def describe(self, index_id: Annotated[str, "Runtime workspace to inspect."]) -> IndexDescribePayload:
        """Return structural metadata for a runtime workspace."""
        with with_db(self._dml) as db:
            return cast(IndexDescribePayload, make_index_ops(db, self._dml).describe(index_id))

    def delete(self, index_id: Annotated[str, "Runtime workspace to delete."]) -> RuntimeDeletePayload:
        """Delete a runtime workspace immediately."""
        with with_db(self._dml) as db:
            make_index_ops(db, self._dml).delete(index_id)
        return {"index": index_id, "deleted": True}

    def cancel(
        self,
        index_id: Annotated[str, "Runtime workspace whose active executions should be cancelled."],
    ) -> RuntimeCancelPayload:
        """Cancel tracked executions for a runtime workspace and report cancellation statistics."""
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


@dataclass(frozen=True)
class _DagNamespace:
    """Inspect committed DAG state and apply DAG-level history operations."""

    _dml: "Dml"

    def get(
        self,
        value: Annotated[str | Ref, "DAG by name or exact Ref."],
        *,
        revision: Annotated[str | None, "Optional revision when the DAG value is name-based."] = None,
    ) -> DagSummaryPayload:
        """Return one DAG summary from the selected revision."""
        with with_db(self._dml) as db:
            if isinstance(value, Ref):
                if revision is not None:
                    raise DmlRepoError("dml dag get rejects --revision with explicit dag refs")
                dag_ref = require_exact_ref(value, "dag")
            else:
                dag_ref = resolve_dag_ref(
                    value=value,
                    revision=revision,
                    commit_ops=make_commit_ops(db),
                    head_ops=make_head_ops(db),
                    project_dir=require_project_home(self._dml._context.project_home),
                    operation="get",
                ).ref
        return dag_summary_payload(self._dml, dag_ref)

    def describe_node(
        self,
        node: Annotated[
            str | Ref,
            "Node by name or exact Ref; examples: result, answer, Ref('node-literal:1').",
        ],
        *,
        dag: Annotated[
            str | Ref | None,
            "Optional DAG by name or exact Ref when node is name-based; examples: train, Ref('dag:1').",
        ] = None,
        revision: Annotated[str | None, "Optional revision selector such as HEAD or main."] = None,
    ) -> NodeDescriptionPayload:
        """Describe a committed node without loading its full value."""
        with with_db(self._dml) as db:
            if isinstance(node, Ref):
                if dag is not None or revision is not None:
                    raise DmlRepoError("dml dag describe-node rejects dag and revision with explicit node refs")
                node_ref = require_exact_ref(node, "node")
            elif isinstance(dag, Ref):
                if revision is not None:
                    raise DmlRepoError("dml dag describe-node rejects --revision with explicit dag refs")
                node_ref = make_dag_ops(db).get_node(require_exact_ref(dag, "dag"), node)
            else:
                node_ref = resolve_node_ref(
                    value=node,
                    dag=dag,
                    revision=revision,
                    commit_ops=make_commit_ops(db),
                    dag_ops=make_dag_ops(db),
                    head_ops=make_head_ops(db),
                    project_dir=require_project_home(self._dml._context.project_home),
                    operation="describe-node",
                ).ref
            return node_description_payload(make_node_ops(db).describe(node_ref))

    @overload
    def get_node(
        self,
        node: str | Ref,
        *,
        dag: str | Ref | None = None,
        revision: str | None = None,
        recursive: Literal[False] = False,
    ) -> NodeValue: ...
    @overload
    def get_node(
        self,
        node: str | Ref,
        *,
        dag: str | Ref | None = None,
        revision: str | None = None,
        recursive: Literal[True] = True,
    ) -> NodeUnrolledValue: ...
    def get_node(
        self,
        node: Annotated[
            str | Ref,
            "Node by name or exact Ref; examples: result, answer, Ref('node-literal:1').",
        ],
        *,
        dag: Annotated[
            str | Ref | None,
            "Optional DAG by name or exact Ref when node is name-based; examples: train, Ref('dag:1').",
        ] = None,
        revision: Annotated[str | None, "Optional revision selector such as HEAD or main."] = None,
        recursive: Annotated[bool, "Whether to recursively unroll the node value."] = False,
    ) -> NodeValue:
        """Return the value for a committed node."""
        with with_db(self._dml) as db:
            if isinstance(node, Ref):
                if dag is not None or revision is not None:
                    raise DmlRepoError("dml dag get-node rejects dag and revision with explicit node refs")
                node_ref = require_exact_ref(node, "node")
            elif isinstance(dag, Ref):
                if revision is not None:
                    raise DmlRepoError("dml dag get-node rejects --revision with explicit dag refs")
                node_ref = make_dag_ops(db).get_node(require_exact_ref(dag, "dag"), node)
            else:
                node_ref = resolve_node_ref(
                    value=node,
                    dag=dag,
                    revision=revision,
                    commit_ops=make_commit_ops(db),
                    dag_ops=make_dag_ops(db),
                    head_ops=make_head_ops(db),
                    project_dir=require_project_home(self._dml._context.project_home),
                    operation="get-node",
                ).ref
            if recursive:
                return cast(NodeUnrolledValue, make_node_ops(db).unroll(node_ref))
            return cast(NodeValue, make_node_ops(db).get(node_ref))

    def checkout(
        self,
        revision: Annotated[str, "Revision selector to copy from; examples: HEAD, main, origin/main."],
        dag_name: Annotated[str, "Name of the DAG to copy from the source revision."],
        *,
        branch: Annotated[str | None, "Target branch to mutate; defaults to the active attached branch."] = None,
        target_name: Annotated[str | None, "Optional new name for the checked-out DAG."] = None,
        replace: Annotated[bool, "Replace an existing DAG with the same target name if present."] = False,
        user: Annotated[str | None, "User recorded as the DAG checkout author."] = None,
    ) -> Ref:
        """Copy a DAG from a revision into a mutable branch."""
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

    def delete(
        self,
        name: Annotated[str, "Name of the DAG to remove from the branch."],
        *,
        branch: Annotated[str | None, "Target branch to mutate; defaults to the active attached branch."] = None,
        user: Annotated[str | None, "User recorded as the delete author."] = None,
    ):
        """Delete a named DAG from a mutable branch."""
        author = require_user(user or self._dml._context.user, message="user is required for dag delete")
        with with_db(self._dml) as db:
            return make_commit_ops(db).delete_dag(name, branch, author)


@dataclass(frozen=True)
class _AdminCacheNamespace:
    """Perform administrative operations against remote-backed cache state."""

    _dml: "Dml"

    def invalidate(
        self,
        cache_keys: Annotated[list[str], "Exact cache entries to invalidate; wildcards and prefixes are not accepted."],
    ) -> AdminCacheInvalidatePayload:
        """Invalidate exact remote cache keys and return the backend response."""
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
    """Inspect and clean remote project metadata stored under the configured remote root."""

    _dml: "Dml"

    @overload
    def list(self, project: None = None, *, owner: str | None = None) -> AdminRemoteProjectsPayload: ...
    @overload
    def list(self, project: str, *, owner: None = None) -> AdminRemoteProjectRefsPayload: ...
    def list(
        self,
        project: Annotated[str | None, "Bare project URI such as dml://alice/demo."] = None,
        *,
        owner: Annotated[str | None, "Filter project listing to one owner when project is omitted."] = None,
    ) -> AdminRemoteProjectsPayload | AdminRemoteProjectRefsPayload:
        """List remote projects or the branch and tag refs for one remote project."""
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

    def gc(
        self,
        *,
        min_age_seconds: Annotated[int, "Minimum object age in seconds before remote GC may delete it."] = 24 * 3600,
        malformed: Annotated[MalformedPolicy, "How to handle malformed remote metadata during GC."] = "warn",
    ) -> AdminRemoteGcPayload:
        """Delete old remote objects that are no longer live under the configured remote root."""
        with with_db(self._dml) as db:
            return cast(
                AdminRemoteGcPayload,
                make_remote_ops(db, self._dml).gc(min_age_seconds=min_age_seconds, malformed=malformed),
            )


@dataclass(frozen=True)
class _AdminNamespace:
    """Administrative maintenance surface for cache state, remotes, and GC."""

    _dml: "Dml"

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
    def gc(
        self,
        *,
        dry_run: Annotated[bool, "Report orphaned refs without deleting them."] = False,
    ) -> AdminGcDryRunPayload | AdminGcRunPayload:
        """Run local repository garbage collection or report what would be deleted."""
        with with_db(self._dml) as db:
            gc_ops = make_gc_ops(db)
            if dry_run:
                orphans = gc_ops.list_orphans()
                return {"dry_run": True, "would_delete": len(orphans), "orphans": orphans}
            return {"dry_run": False, "deleted": sum(gc_ops.gc().values())}


class Dml:
    """Shared orchestration boundary for repository, runtime, DAG, and admin workflows."""

    def __init__(
        self,
        project_home: Annotated[str | None, "Project directory containing the .dml state."] = None,
        *,
        remote_root: Annotated[str | None, "Remote root URI such as s3://bucket/prefix."] = None,
        user: Annotated[str | None, "User identity recorded for mutating operations."] = None,
        config_home: Annotated[str | None, "Override directory for global DaggerML config files."] = None,
    ):
        """Resolve runtime context for a project-scoped DaggerML session."""
        self._context = resolve_runtime_context(
            project_home=project_home,
            remote_root=remote_root,
            user=user,
            config_home=config_home,
        )
        self._s3_client = create_s3_client() if self._context.remote_root else None

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
        """Return current HEAD, branches, visible DAGs, and open runtime workspaces."""
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

    def branch(
        self,
        *,
        remote: Annotated[bool, "List remote-tracking branches instead of local branches."] = False,
    ) -> BranchPayload:
        """List local branches or discovered remote-tracking branches."""
        if remote:
            return {"branches": remote_tracking_branches(self), "head": None, "remote": True}
        with with_db(self) as db:
            current_head_ops = make_head_ops(db)
            return {
                "branches": current_head_ops.list_branches(),
                "head": current_head_branch(current_head_ops),
                "remote": False,
            }

    def log(
        self,
        revision: Annotated[str, "Revision selector such as HEAD, HEAD~1, main, or origin/main."] = "HEAD",
        *,
        limit: Annotated[int | None, "Maximum number of commits to return from newest to oldest."] = None,
    ) -> LogPayload:
        """Return commit summaries reachable from the selected revision."""
        resolved = resolve_dml_revision(self, revision)
        with with_db(self) as db:
            commit_ops = make_commit_ops(db)
            refs = list(commit_ops.list(resolved.commit, limit=limit))
            summaries = {ref: commit_ops.describe(ref) for ref in refs}
        commits = [commit_payload(self, ref, summaries[ref]) for ref in refs]
        return {
            "revision": revision_payload(revision, resolved),
            "commits": commits,
        }

    def show(
        self,
        revision: Annotated[str, "Revision selector such as HEAD, HEAD~1, main, or origin/main."] = "HEAD",
    ) -> ShowPayload:
        """Return one commit summary together with the DAG map and DAG-level diff to its first parent."""
        resolved = resolve_dml_revision(self, revision)
        with with_db(self) as db:
            commit_summary = make_commit_ops(db).describe(resolved.commit)
        commit = commit_payload(self, resolved.commit, commit_summary)
        dags = commit["dags"]
        base_commit = commit["parents"][0] if commit["parents"] else None
        base_dags = dag_map_for_commit(self, base_commit) if base_commit is not None else {}
        return {
            "revision": revision_payload(revision, resolved),
            "commit": commit,
            "change": {"base": base_commit, **dag_map_diff(base_dags, dags)},
        }

    def diff(
        self,
        left: Annotated[str, "Base revision selector; for example HEAD~1 or main."] = "HEAD~1",
        right: Annotated[str, "Compare-against revision selector; for example HEAD or origin/main."] = "HEAD",
    ) -> DiffPayload:
        """Return DAG-map additions, removals, and updates between two revisions."""
        left_resolved = resolve_dml_revision(self, left)
        right_resolved = resolve_dml_revision(self, right)
        left_dags = dag_map_for_commit(self, left_resolved.commit)
        right_dags = dag_map_for_commit(self, right_resolved.commit)
        return {
            "left": revision_payload(left, left_resolved),
            "right": revision_payload(right, right_resolved),
            **dag_map_diff(left_dags, right_dags),
        }

    def checkout(
        self,
        revision: Annotated[str, "Revision selector to attach or detach HEAD to."],
    ) -> CheckoutAttachedPayload | CheckoutDetachedPayload:
        """Move HEAD to a branch or detached commit without changing repository contents."""
        resolved = resolve_dml_revision(self, revision)
        with with_db(self) as db:
            current_head_ops = make_head_ops(db)
            if resolved.kind == "branch" and resolved.branch is not None:
                current_head_ops.write_attached_head(resolved.branch)
                return {"mode": "attached", "branch": resolved.branch}
            current_head_ops.write_detached_head(resolved.commit)
            return {"mode": "detached", "branch": None}

    def fetch(
        self,
        remote_or_uri: Annotated[
            str,
            "Remote name like origin or explicit project URI such as dml://alice/demo.",
        ],
        branch: Annotated[
            str | None, "Branch selector to fetch; defaults to the active or configured default branch."
        ] = None,
    ) -> Ref:
        """Fetch a remote branch into local history."""
        project_home = require_project_home(self._context.project_home)
        uri = project_remote_root(
            project_home=project_home,
            remote_or_uri=remote_or_uri,
            branch=branch,
            default_branch=self._context.default_branch,
        )
        with with_db(self) as db:
            return make_remote_ops(db, self).fetch_uri(uri)

    def pull(
        self,
        remote_or_uri: Annotated[
            str,
            "Remote name like origin or explicit project URI such as dml://alice/demo.",
        ],
        remote_branch: Annotated[str | None, "Remote branch selector to pull; defaults to the target branch."] = None,
        *,
        branch: Annotated[str | None, "Local branch to update; defaults to the active attached branch."] = None,
        user: Annotated[str, "User identity recorded for the merge commit created by the pull."],
    ) -> Ref:
        """Fetch a remote branch and merge it into a local branch in one operation."""
        project_home = require_project_home(self._context.project_home)
        with with_db(self) as db:
            target_branch = mutable_branch(branch=branch, head_ops=make_head_ops(db))
        uri = project_remote_root(
            project_home=project_home,
            remote_or_uri=remote_or_uri,
            branch=remote_branch or target_branch,
            default_branch=self._context.default_branch,
        )
        with with_db(self) as db:
            return make_remote_ops(db, self).pull_uri_into_branch(uri, target_branch, user=user)

    def push(
        self,
        tag: Annotated[str | None, "Optional tag name to publish instead of pushing a branch."] = None,
        *,
        branch: Annotated[str | None, "Local branch to publish; defaults to the active attached branch."] = None,
        create: Annotated[bool, "Allow creating a missing remote branch when pushing by branch."] = False,
        force: Annotated[bool, "Allow non-fast-forward remote branch updates."] = False,
    ) -> str:
        """Push a branch or tag to the configured remote project and return the remote ref path."""
        project = load_project_config(require_project_home(self._context.project_home))
        if not project.uri:
            raise DmlRepoError("remote.project is required for project sync")
        with with_db(self) as db:
            source_branch = branch or make_head_ops(db).require_attached_head_branch()
        with with_db(self) as db:
            remote = make_remote_ops(db, self)
            if tag:
                return remote.push_project_tag(f"{project.uri}@{tag}", source_branch)
            return remote.push_project_branch(
                f"{project.uri}#{source_branch}", source_branch, create=create, force=force
            )

    def merge(
        self,
        revision: Annotated[str, "Revision selector to merge into the target branch."],
        *,
        branch: Annotated[str | None, "Branch to update; defaults to the active attached branch."] = None,
        user: Annotated[str, "User identity recorded for the merge commit."],
    ):
        """Merge one revision into a mutable branch."""
        revision_ref = resolve_dml_revision_ref(self, revision)
        with with_db(self) as db:
            target_branch = mutable_branch(branch=branch, head_ops=make_head_ops(db))
            return make_commit_ops(db).merge_into_head(target_branch, revision_ref, user)

    def revert(
        self,
        revision: Annotated[str, "Revision selector whose changes should be reverted."],
        *,
        branch: Annotated[str | None, "Branch to update; defaults to the active attached branch."] = None,
        user: Annotated[str, "User identity recorded for the revert commit."],
    ):
        """Create a revert commit for one revision on a mutable branch."""
        revision_ref = resolve_dml_revision_ref(self, revision)
        with with_db(self) as db:
            target_branch = mutable_branch(branch=branch, head_ops=make_head_ops(db))
            return make_commit_ops(db).revert(target_branch, revision_ref, user)

    @classmethod
    def init(
        cls,
        project_home: Annotated[str, "Directory to initialize as a DaggerML project."] = ".",
        *,
        remote_root: Annotated[str | None, "Remote root URI such as s3://bucket/prefix."] = None,
        user: Annotated[str | None, "Default user identity for the initialized runtime."] = None,
        config_home: Annotated[str | None, "Override directory for global DaggerML config files."] = None,
        remote_project: Annotated[str | None, "Remote project URI such as dml://alice/demo to seed from."] = None,
    ) -> InitPayload:
        """Initialize project state, config, and database for a DaggerML repository."""
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
                project_cfg = DmlProjectConfig(name=parsed.project, owner=parsed.owner, remote_root=remote_root or "")
            else:
                project_cfg = DmlProjectConfig(remote_root=remote_root or "")
        if not gitignore_exists(project_home):
            (dml_dir / ".gitignore").write_text("db\nHEAD\nrefs\n")
        if not config_existed:
            project_cfg.save(root)
        runtime = cls(project_home=project_home, remote_root=remote_root, user=user, config_home=config_home)
        resolved_branch = runtime._context.default_branch
        if not config_existed and project_cfg.remote_root != runtime._context.remote_root:
            project_cfg = DmlProjectConfig(
                name=project_cfg.name,
                owner=project_cfg.owner,
                remote_root=runtime._context.remote_root,
            )
            project_cfg.save(root)
        if not db_existed:
            create_db(project_home, branch=resolved_branch)
        project_cfg = load_project_config(project_home)
        if project_cfg.remote_project and not runtime._context.remote_root:
            raise DmlRepoError("remote.root is required")
        if project_cfg.remote_project and runtime._context.remote_root:
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
            "remote_root": runtime._context.remote_root,
            "user": runtime._context.user,
            "config_home": runtime._context.config.config_home,
            "created": {"db": not db_existed, "config": not config_existed},
        }
