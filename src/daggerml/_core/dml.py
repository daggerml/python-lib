from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from functools import wraps
from importlib import resources
from pathlib import Path
from time import time
from typing import Annotated, Any, Literal, Mapping, NotRequired, TypedDict, cast, overload

from daggerml._core.commit import (
    CommitDescription,
    CommitDiffPayload,
    CommitFullDescription,
    CommitOps,
    ShallowHistoryError,
)
from daggerml._core.config import Config, flatten_dict
from daggerml._core.dag import DagDescription, DagOps, NodeDescriptionPayload
from daggerml._core.db import DmlDbKeyNotFoundError, Ref
from daggerml._core.exec_state import (
    EXECUTION_LIFECYCLES,
    ExecutionGraph,
    ExecutionRecord,
    ExecutionState,
    InvalidationResponse,
)
from daggerml._core.head import Head, UpstreamInfo
from daggerml._core.index import IndexOps
from daggerml._core.remote import Remote
from daggerml._core.s3_cas import CasItemConflict
from daggerml._core.types import DmlDB, DmlRepoError, Error, FrozenIndex, Index, TxnWithValid
from daggerml.util import get_client


def _format_graph_age(seconds: int | float | None) -> str:
    if seconds is None or seconds < 0:
        return "-"
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    minutes, secs = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


def _graph_lifecycle_style(lifecycle: str) -> str:
    return {
        "running": "yellow",
        "succeeded": "green",
        "failed": "red",
        "cancel-pending": "dark_goldenrod italic",
        "canceled": "dim",
        "pending": "cyan",
    }.get(lifecycle, "white")


def _require_remote_root(dml: "Dml") -> str:
    remote_root = dml._config.remote.root
    if not remote_root:
        raise DmlRepoError("remote.root is required")
    return remote_root


def _require_s3_client(dml: "Dml"):
    return get_client("s3", max_pool_connections=dml._config.remote.fetch_workers)


def _remote_ops(dml: "Dml") -> Remote:
    return _remote_ops_for_root(dml, _require_remote_root(dml))


def _remote_ops_for_root(dml: "Dml", root: str, *, initialize: bool = True) -> Remote:
    return Remote(
        root,
        n_workers=dml._config.remote.fetch_workers,
        client=_require_s3_client(dml),
        prune_age_seconds=dml._config.remote.prune_age_seconds,
        initialize=initialize,
    )


def _head_ops(dml: "Dml") -> Head:
    return Head(dml._config.project_home)


def _index_ops(dml: "Dml") -> IndexOps:
    return IndexOps(
        _require_remote_root(dml),
        n_workers=dml._config.remote.fetch_workers,
        client=_require_s3_client(dml),
    )


def _exec_state(dml: "Dml", cache_key=None) -> ExecutionState:
    return ExecutionState(
        _require_remote_root(dml),
        n_workers=dml._config.remote.fetch_workers,
        client=_require_s3_client(dml),
        cache_key=cache_key,
    )


_RUNTIME_MUTATION_RETRY_ERRORS = (CasItemConflict,)
_RUNTIME_MUTATION_RETRY_ATTEMPTS = 2


def _retry_runtime_mutation(fn=None, errors=_RUNTIME_MUTATION_RETRY_ERRORS, attempts=_RUNTIME_MUTATION_RETRY_ATTEMPTS):
    if fn is None:
        return lambda fn: _retry_runtime_mutation(fn, errors=errors, attempts=attempts)

    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        for attempt in range(attempts):
            try:
                return fn(self, *args, **kwargs)
            except errors:
                if attempt + 1 >= _RUNTIME_MUTATION_RETRY_ATTEMPTS:
                    raise

    return wrapper


def _render_execution_graph(graph: ExecutionGraph) -> None:
    try:
        from rich import box
        from rich.console import Console
        from rich.panel import Panel
        from rich.table import Table
        from rich.text import Text
        from rich.tree import Tree
    except ImportError as exc:
        raise DmlRepoError(
            "rich is required for rendered describe_graph output; install daggerml[terminal] or pip install rich"
        ) from exc

    now = int(time())

    def render_node(execution_id: str):
        node = graph["nodes"][execution_id]
        style = _graph_lifecycle_style(node["lifecycle"])
        age_seconds = max(0, now - node["created_at"]) if node["created_at"] else 0
        idle_seconds = max(0, now - node["updated_at"]) if node["updated_at"] else age_seconds
        title = Text.assemble(
            (node["lifecycle"].upper(), f"bold {style}"),
            (f" call {_format_graph_age(age_seconds)} -> {_format_graph_age(idle_seconds)}", "bold white"),
            (f" [{len(node['spawned'])} : {len(node['children'])}]", "dim white"),
        )
        details = Table.grid(padding=(0, 1))
        details.add_column(style="bold cyan", ratio=1)
        details.add_column(ratio=5)
        details.add_row("exec", node["execution_id"])
        if node["cache_key"]:
            details.add_row("cache", node["cache_key"])
        if node["cancel_requested_by"]:
            details.add_row("cancel", node["cancel_requested_by"])
        return Panel(details, title=title, border_style=style, box=box.ROUNDED, expand=False)

    seen: set[str] = set()

    def add_edges(tree: Tree, execution_id: str) -> None:
        if execution_id in seen:
            return
        seen.add(execution_id)
        node = graph["nodes"][execution_id]
        for target_id in [*node["children"], *node["spawned"]]:
            if target_id in seen:
                continue
            add_edges(tree.add(render_node(target_id)), target_id)

    call_tree = Tree(Text("Execution Call Stack", style="bold white"), guide_style="dim")
    if graph["roots"]:
        for root_id in graph["roots"]:
            add_edges(call_tree.add(render_node(root_id)), root_id)
    else:
        call_tree.add(Text("<no roots>", style="dim"))
    Console().print(Panel(call_tree, title="Execution Graph", expand=False))


def _require_resolved_commit(commit: Ref | None, revision: Ref | str) -> Ref:
    if commit is None:
        raise DmlRepoError(f"Revision not found: {revision}")
    return commit


def _revision_source(remote: bool, dep: str | None) -> tuple[bool, str | None]:
    if remote and dep is not None:
        raise DmlRepoError("remote and dep cannot be selected together")
    return remote, dep


def _validate_history_options(depth: int | None, unshallow: bool = False) -> None:
    if depth is not None and (isinstance(depth, bool) or depth <= 0):
        raise DmlRepoError("depth must be a positive integer")
    if depth is not None and unshallow:
        raise DmlRepoError("depth and unshallow cannot be selected together")


def _publish_shallow_state(head: Head, available: set[Ref], omitted: set[Ref]) -> None:
    shallow = head.get_shallow_commits()
    head.write_shallow_commits((shallow - available) | omitted)


class RefListItem(TypedDict):
    name: str
    commit: Ref


def _list_ref_items(dml: "Dml", *, kind: Literal["branch", "tag"], remote: bool, dep: str | None) -> list[RefListItem]:
    head = _head_ops(dml)
    if dep is None:
        tips = (
            _remote_ops_for_root(dml, _require_remote_root(dml), initialize=False).list_ref_tips(kind)
            if remote
            else head.list_local_ref_tips(kind)
        )
    else:
        dependency = head.get_dependency_config(dep)
        tips = (
            _remote_ops_for_root(dml, dependency["root"], initialize=False).list_ref_tips(kind)
            if remote
            else head.list_dependency_ref_tips(dep, kind)
        )
    return [{"name": name, "commit": commit} for name, commit in sorted(tips)]


def resolve_rev(
    head: Head, revision: Ref | str, db: DmlDB, *, remote: bool = False, dep: str | None = None
) -> Ref | None:
    remote, dep = _revision_source(remote, dep)
    if isinstance(revision, Ref):
        return revision
    if re.match(r"^[0-9a-f]{64}$", revision):
        return Ref(f"commit:{revision}")
    if re.match(r"^commit:[0-9a-f]{64}$", revision):
        return Ref(revision)
    if revision.startswith("HEAD"):
        match = re.match(r"^HEAD~([0-9]+)$", revision)
        if match is None and revision != "HEAD":
            raise DmlRepoError(f"Invalid revision: {revision}")
        n = 0 if not match else int(match.group(1), 10)
        if remote:
            current = head.get_remote_tracking_ref("main")
        elif dep is not None:
            current = head.get_dependency_ref(dep, "main")
        else:
            current = head.get_head()["commit"]
        if current is None:
            return None
        return CommitOps().get_ancestor(
            current, n, db=db, missing_commits=head.get_shallow_commits()
        )
    if revision.startswith("dml://") or revision.startswith("#"):
        raise DmlRepoError(f"Invalid revision: {revision}")
    kind = "tag" if revision.startswith("@") else "branch"
    name = revision[1:] if kind == "tag" else revision
    try:
        if remote:
            return head.get_remote_tracking_ref(name, kind=kind)
        if dep is not None:
            return head.get_dependency_ref(dep, name, kind=kind)
        return head.get_local_ref(name, kind=kind)
    except DmlRepoError as exc:
        if remote or dep is not None:
            raise DmlRepoError(f"Revision not fetched; run fetch before resolving: {revision}") from exc
        raise


_PYTHON_CONFIG_VAR_NAMES = {
    "project_home": "project_home",
    "db_path": "db_path",
    "db_map_size_headroom": "default.db_map_size_headroom",
    "db_map_size_max": "default.db_map_size_max",
    "default_branch_name": "default.branch_name",
    "remote_root": "remote.root",
    "remote_prune_age_seconds": "remote.prune_age_seconds",
    "remote_fetch_workers": "remote.fetch_workers",
    "user": "user",
    "config_home": "config_home",
}


def _python_config_vars_to_canonical(**kwargs: object) -> dict[str, object]:
    return {canonical: kwargs[name] for name, canonical in _PYTHON_CONFIG_VAR_NAMES.items()}


class RemoteConfig(TypedDict):
    root: str | None
    prune_age_seconds: int
    fetch_workers: int


class ConfigStatus(TypedDict):
    project_home: str
    db_path: str | None
    default: dict[str, int | str]
    remote: RemoteConfig
    user: str
    config_home: str
    contrib: NotRequired[dict]


@dataclass(frozen=True)
class _ConfigNamespace:
    _dml: "Dml"

    def show(self, *, contrib: Annotated[bool, "Include contrib runtime status."] = False) -> ConfigStatus:
        """Return the resolved runtime configuration."""
        payload = asdict(self._dml._config)
        if contrib:
            from daggerml.contrib import status as contrib_status

            payload["contrib"] = contrib_status.status()
        return cast(ConfigStatus, payload)

    def get(self, key: Annotated[str, "Flattened config key to read."]) -> str | int | None:
        """Return one resolved config value by flattened key."""
        return cast(str | int | None, flatten_dict(self.show()).get(key))

    @overload
    def set(
        self,
        key: Annotated[str, "Flattened config key to update."],
        value: Annotated[str, "String value to store."],
        scope: Annotated[Literal["global", "local"], "Whether to write local or global config."] = "local",
    ) -> str: ...
    @overload
    def set(
        self,
        key: Annotated[str, "Flattened config key to update."],
        value: Annotated[int, "Integer value to store."],
        scope: Annotated[Literal["global", "local"], "Whether to write local or global config."] = "local",
    ) -> int: ...
    @overload
    def set(
        self,
        key: Annotated[str, "Flattened config key to update."],
        value: Annotated[None, "Clear the config value."],
        scope: Annotated[Literal["global", "local"], "Whether to write local or global config."] = "local",
    ) -> None: ...
    def set(
        self,
        key: Annotated[str, "Flattened config key to update."],
        value: Annotated[str | int | None, "Value to store. Use null to clear it."],
        scope: Annotated[Literal["global", "local"], "Whether to write local or global config."] = "local",
    ) -> str | int | None:
        """Update one config value and reload the resolved config."""
        updated = self._dml._config.update(key, value, scope=scope)
        object.__setattr__(self._dml, "_config", Config.resolve(explicit=self._dml._explicit_config))
        return updated


class RuntimeDescribe(TypedDict):
    id: Ref
    parents: list[Ref]
    tree: Ref
    author: str
    message: str
    created: str
    dag: Ref
    state: Literal["active", "frozen"]
    frozen_message: str | None


class RuntimeListPayload(TypedDict):
    id: Ref
    parents: list[Ref]
    tree: Ref
    author: str
    message: str
    created: str
    dag: Ref
    state: Literal["active", "frozen"]
    frozen_message: str | None


class CacheDescription(TypedDict):
    execution: Ref
    dag: Ref | None
    lifecycle: EXECUTION_LIFECYCLES


def _require_runtime_ref(value: object, *, allow_frozen: bool = True) -> Ref:
    if not isinstance(value, Ref):
        raise TypeError(f"expected runtime Ref, got {type(value).__name__}")
    allowed = ("index", "frozenindex") if allow_frozen else ("index",)
    if value.ns() not in allowed:
        raise TypeError(f"expected runtime Ref in {allowed}, got {value.ns()!r}")
    return value


@dataclass(frozen=True)
class _RuntimeNamespace:
    _dml: "Dml"

    @_retry_runtime_mutation
    def create(
        self,
        cache_key: Annotated[str | None, "Cache key to reuse execution results."] = None,
        execution: Annotated[Ref | None, "Runtime ref for adapter-coordinated runs."] = None,
    ) -> Ref:
        """Create a new mutable runtime index."""
        if (cache_key is None) != (execution is None):
            raise DmlRepoError("both cache_key and execution must be provided or neither")
        execution_id = _require_runtime_ref(execution, allow_frozen=False).id() if execution is not None else None
        return _index_ops(self._dml).create(
            self._dml._config.user,
            commit=_head_ops(self._dml).get_head()["commit"],
            cache_key=cache_key,
            execution_id=execution_id,
            db=self._dml._db,
        )

    @_retry_runtime_mutation
    def put_literal(
        self,
        index: Annotated[Ref, "Runtime index to write into."],
        value: Annotated[Any, "Python value to stage into the DAG."],
        *,
        name: Annotated[str | None, "Optional node name to assign."] = None,
    ) -> Ref:
        """Stage a literal value into a runtime index."""
        return _index_ops(self._dml).put_literal(index, value, name=name, db=self._dml._db)

    @_retry_runtime_mutation
    def put_import(
        self,
        index: Annotated[Ref, "Runtime index to write into."],
        dag: Annotated[Ref, "Committed DAG to import from."],
        node: Annotated[Ref | None, "Specific node to import. Defaults to the DAG result."] = None,
        *,
        name: Annotated[str | None, "Optional node name to assign."] = None,
    ) -> Ref:
        """Import a node from a committed DAG into a runtime index."""
        return _index_ops(self._dml).put_import(index, dag, node, name=name, db=self._dml._db)

    def get_argv(self, index: Annotated[Ref, "Runtime index to inspect."]) -> Ref:
        """Return the argv node for a runtime index."""
        return _index_ops(self._dml).get_argv(index, db=self._dml._db)

    def get_node(
        self,
        index: Annotated[Ref, "Runtime index to inspect."],
        name: Annotated[str, "Node name to resolve."],
    ) -> Ref:
        """Resolve one named node from a runtime index."""
        return _index_ops(self._dml).get_node(index, name, db=self._dml._db)

    @_retry_runtime_mutation
    def set_node_name(
        self,
        index: Annotated[Ref, "Runtime index to mutate."],
        name: Annotated[str, "Name to assign."],
        node: Annotated[Ref, "Node ref to bind to the name."],
    ) -> Ref:
        """Assign a name to an existing node in a runtime index."""
        return _index_ops(self._dml).set_node_name(index, name, node, db=self._dml._db)

    @_retry_runtime_mutation
    def start_fn(
        self,
        index: Annotated[Ref, "Runtime index to execute in."],
        argv: Annotated[list[Ref], "Argument vector where argv[0] is the runnable node."],
        *,
        name: Annotated[str | None, "Optional name for the result node."] = None,
    ) -> Ref | None:
        """Start a function call in a runtime index."""
        return _index_ops(self._dml).start_fn(index, argv, name=name, db=self._dml._db)

    def commit(
        self,
        index: Annotated[Ref, "Runtime index to commit."],
        value: Annotated[Ref | Error, "Result node ref or stored error to commit."],
        message: Annotated[str | None, "Commit message for history."] = None,
        name: Annotated[str | None, "DAG name to record in the commit tree."] = None,
    ) -> Ref:
        """Finalize a runtime index and optionally record it in history."""
        db = self._dml._db
        dag_ref, commit_ref = _index_ops(self._dml).commit(
            index,
            value,
            author=self._dml._config.user,
            message=message,
            name=name,
            db=db,
        )
        if commit_ref is None:
            return dag_ref
        hops = _head_ops(self._dml)
        with hops.lock():
            head_info = hops.get_head()
            commit_ref = CommitOps().merge(head_info["commit"], commit_ref, user=self._dml._config.user, db=db)
            branch = head_info["branch"]
            if branch is not None:
                hops.update_local_ref(branch, commit_ref)
            else:
                hops.write_detached_head(commit_ref)
        return dag_ref

    @_retry_runtime_mutation
    def freeze(
        self,
        index: Annotated[Ref, "Active user runtime index to freeze."],
        *,
        message: Annotated[str | None, "Optional reason shown while the runtime is frozen."] = None,
    ) -> Ref:
        """Freeze a user runtime for later inspection or resumption."""
        return _index_ops(self._dml).freeze(index, message, db=self._dml._db)

    @_retry_runtime_mutation
    def unfreeze(self, index: Annotated[Ref, "Frozen runtime index to reactivate."]) -> Ref:
        """Reactivate a frozen runtime index."""
        return _index_ops(self._dml).unfreeze(index, db=self._dml._db)

    def describe(self, index: Annotated[Ref, "Runtime index to inspect."]) -> RuntimeDescribe:
        """Describe one runtime index."""
        db = self._dml._db
        with db.tx(readonly=True) as txn:
            idx = txn.get(index)
            if not isinstance(idx, (Index, FrozenIndex)):
                raise DmlRepoError(f"Runtime is not an index: {index}")
            return {
                "id": index,
                "parents": idx.parents,
                "tree": idx.tree,
                "author": idx.author,
                "message": idx.message,
                "created": idx.created,
                "dag": idx.dag,
                "state": "frozen" if isinstance(idx, FrozenIndex) else "active",
                "frozen_message": idx.frozen_message if isinstance(idx, FrozenIndex) else None,
            }

    def list(self) -> list[RuntimeListPayload]:
        """List open runtime indexes in reverse creation order."""
        objs: list[RuntimeListPayload] = []
        with self._dml._db.tx(readonly=True) as txn:
            index_items = []
            for namespace in ("index", "frozenindex"):
                try:
                    index_items.extend(txn.iter(namespace))
                except DmlDbKeyNotFoundError:
                    pass
            for x, obj in index_items:
                assert isinstance(obj, (Index, FrozenIndex))
                objs.append(
                    {
                        "id": x,
                        "parents": obj.parents,
                        "tree": obj.tree,
                        "author": obj.author,
                        "message": obj.message,
                        "created": obj.created,
                        "dag": obj.dag,
                        "state": "frozen" if isinstance(obj, FrozenIndex) else "active",
                        "frozen_message": obj.frozen_message if isinstance(obj, FrozenIndex) else None,
                    }
                )
        return sorted(objs, key=lambda x: x["created"], reverse=True)

    def read_execution_record(
        self,
        execution: Annotated[Ref, "Runtime execution ref to inspect."],
    ) -> ExecutionRecord:
        """Read the raw execution record for one runtime execution."""
        execution_id = _require_runtime_ref(execution).id()
        return _exec_state(self._dml).read_execution_record(execution_id)

    def cancel(
        self,
        execution: Annotated[Ref, "Runtime execution to cancel."],
        *,
        max_retries: Annotated[int, "Maximum cancellation retries after the initial attempt."] = 3,
    ) -> None:
        """Cancel active state for a runtime execution."""
        execution = _require_runtime_ref(execution)
        if not isinstance(max_retries, int) or isinstance(max_retries, bool) or max_retries < 0:
            raise TypeError("max_retries must be a nonnegative integer")
        _exec_state(self._dml).cancel(execution.id(), self._dml._config.user, self._dml._db, max_retries=max_retries)

    def describe_graph(
        self,
        *roots: Annotated[Ref, "Runtime execution roots to inspect."],
        visual: Annotated[bool, "Render a human-friendly graph view instead of returning the raw payload."] = False,
    ) -> ExecutionGraph | None:
        """Describe reachable execution lineage for one or more runtime roots."""
        execution_ids = [_require_runtime_ref(root).id() for root in roots]
        if not execution_ids:
            execution_ids = [item["id"].id() for item in self.list()]
        graph = _index_ops(self._dml).exec_state().describe_graph(execution_ids)
        if visual:
            _render_execution_graph(graph)
            return None
        return graph


@dataclass(frozen=True)
class _DagNamespace:
    _dml: "Dml"

    def describe(self, value: Annotated[Ref, "Committed DAG ref to inspect."]) -> DagDescription:
        """Describe a committed DAG."""
        return DagOps().describe(value, db=self._dml._db)

    def describe_node(self, node: Annotated[Ref, "Node ref to inspect."]) -> NodeDescriptionPayload:
        """Describe one node and any linked DAG context."""
        return DagOps().describe_node(node, db=self._dml._db)

    def get_node(
        self,
        node: Annotated[Ref, "Node ref to materialize."],
        *,
        recursive: Annotated[bool, "Recursively unroll collection values."] = False,
    ) -> Any | Error:
        """Return the stored value for a node."""
        db = self._dml._db
        with db.tx(readonly=True) as txn:
            node_obj = txn.get(node)
            datum_ref, error_ref = node_obj.datum_ref(txn)
            if error_ref is not None:
                return txn.get(error_ref)
            assert datum_ref is not None
            datum = txn.get(datum_ref)
            if recursive:
                return datum.unroll(txn)
            return datum.value(txn)

    def get_error(self, error: Annotated[Ref, "Error ref to materialize."]) -> Error:
        """Return the stored error for an error ref."""
        with self._dml._db.tx(readonly=True) as txn:
            return txn.get(TxnWithValid.require(error, "error"))

    def get_argv(self, dag: Annotated[Ref, "Committed DAG ref to inspect."]) -> Ref:
        """Return the argv node for a function DAG."""
        return DagOps().get_argv(dag, db=self._dml._db)

    def get_node_by_name(
        self,
        dag: Annotated[Ref, "Committed DAG ref to inspect."],
        name: Annotated[str, "Node name to resolve."],
    ) -> Ref:
        """Resolve one named node from a committed DAG."""
        return DagOps().get_node(dag, name, db=self._dml._db)

    def delete(self, dag: Annotated[str, "DAG name to delete from the current branch."]) -> Ref:
        """Delete a named DAG from the current branch."""
        head = _head_ops(self._dml)
        with head.lock():
            head_info = head.get_head()
            if not head_info["branch"]:
                raise DmlRepoError("Cannot delete DAG when HEAD is detached")
            new_commit = CommitOps().delete_dag(
                _require_resolved_commit(head_info["commit"], "HEAD"),
                dag,
                user=self._dml._config.user,
                db=self._dml._db,
            )
            head.update_local_ref(head_info["branch"], new_commit)
        return new_commit

    def checkout(
        self,
        revision: Annotated[Ref | str, "Revision containing the DAG."],
        dag: Annotated[str, "DAG name in the selected revision."],
        *,
        remote: Annotated[bool, "Resolve the revision from remote tracking."] = False,
        dep: Annotated[str | None, "Resolve the revision from a dependency."] = None,
        name: Annotated[str | None, "Local name for the checked out DAG."] = None,
        replace: Annotated[bool, "Allow replacing a different local DAG."] = False,
    ) -> Ref:
        """Copy a selected committed DAG into the current branch tree."""
        head = _head_ops(self._dml)
        with head.lock():
            head_info = head.get_head()
            if not head_info["branch"]:
                raise DmlRepoError("Cannot checkout DAG when HEAD is detached")
            source_commit = _require_resolved_commit(
                resolve_rev(head, revision, db=self._dml._db, remote=remote, dep=dep), revision
            )
            source_dag = CommitOps().show(source_commit, db=self._dml._db)["dags"].get(dag)
            if source_dag is None:
                raise DmlRepoError(f"DAG not found: {dag}")
            target_name = name or dag
            current = (
                CommitOps().show(head_info["commit"], db=self._dml._db)
                if head_info["commit"] is not None
                else {"dags": {}}
            )
            if not replace and target_name in current["dags"] and current["dags"][target_name] != source_dag:
                raise DmlRepoError(f"DAG already exists: {target_name}")
            new_commit = CommitOps().checkout_dag(
                head_info["commit"],
                source_dag,
                name=target_name,
                user=self._dml._config.user,
                db=self._dml._db,
            )
            head.update_local_ref(head_info["branch"], new_commit)
        return new_commit

    def add_tag(self, dag: Annotated[str, "Named DAG to tag."], tag: Annotated[str, "Opaque tag to add."]) -> Ref:
        """Add an opaque tag to a named DAG on the current branch."""
        head = _head_ops(self._dml)
        with head.lock():
            head_info = head.get_head()
            if not head_info["branch"]:
                raise DmlRepoError("Cannot add DAG tag when HEAD is detached")
            new_commit = CommitOps().add_dag_tag(
                _require_resolved_commit(head_info["commit"], "HEAD"),
                dag,
                tag,
                user=self._dml._config.user,
                db=self._dml._db,
            )
            head.update_local_ref(head_info["branch"], new_commit)
        return new_commit

    def remove_tag(
        self,
        dag: Annotated[str, "Named DAG to untag."],
        tag: Annotated[str, "Opaque tag to remove."],
    ) -> Ref:
        """Remove an opaque tag from a named DAG on the current branch."""
        head = _head_ops(self._dml)
        with head.lock():
            head_info = head.get_head()
            if not head_info["branch"]:
                raise DmlRepoError("Cannot remove DAG tag when HEAD is detached")
            new_commit = CommitOps().remove_dag_tag(
                _require_resolved_commit(head_info["commit"], "HEAD"),
                dag,
                tag,
                user=self._dml._config.user,
                db=self._dml._db,
            )
            head.update_local_ref(head_info["branch"], new_commit)
        return new_commit


@dataclass(frozen=True)
class _BranchNamespace:
    _dml: "Dml"

    def list(
        self,
        *,
        remote: Annotated[bool, "List branches directly from the selected endpoint."] = False,
        dep: Annotated[str | None, "Dependency to inspect; list fetched or remote refs."] = None,
    ) -> list[RefListItem]:
        """List branch names and exact commit tips from the selected source."""
        return _list_ref_items(self._dml, kind="branch", remote=remote, dep=dep)

    def create(
        self,
        name: Annotated[str, "Local branch name to create."],
        *,
        revision: Annotated[Ref | str | None, "Optional revision to point the new branch at."] = None,
        remote: Annotated[bool, "Resolve the revision from fetched remote tracking refs."] = False,
    ) -> str:
        """Create one local branch at a resolved revision."""
        head = _head_ops(self._dml)
        with head.lock():
            if head.local_ref_path(name, kind="branch").exists():
                raise DmlRepoError(f"Branch already exists: {name}")
            resolved_revision: Ref | str = revision or "HEAD"
            commit_ref = resolve_rev(head, resolved_revision, db=self._dml._db, remote=remote)
            if resolved_revision == "HEAD":
                head_info = head.get_head()
                if commit_ref is None and head_info["mode"] == "attached":
                    head.write_attached_head(name)
                    if remote:
                        head.set_upstream(name, name)
                    return name
            commit_ref = _require_resolved_commit(commit_ref, resolved_revision)
            created = head.create_local_ref(name, commit_ref, kind="branch")
            if remote:
                head.set_upstream(name, name)
            return created

    def set_upstream(self, upstream: Annotated[str, "Remote-root branch to track."]) -> str:
        """Set the upstream for the currently attached branch."""
        head = _head_ops(self._dml)
        with head.lock():
            current = head.get_head()["branch"]
            if current is None:
                raise DmlRepoError("Cannot set upstream when HEAD is detached")
            head.set_upstream(current, upstream)
        return upstream

    def get_upstream(
        self,
        branch: Annotated[str, "Local branch name whose configured upstream should be inspected."],
    ) -> UpstreamInfo | None:
        """Return the configured remote-root upstream for one branch."""
        return _head_ops(self._dml).get_upstream(branch)

    def move(
        self,
        name: Annotated[str, "Local branch name to repoint."],
        revision: Annotated[Ref | str, "Revision to point the branch at."],
    ) -> str:
        """Repoint one local branch to a resolved revision."""
        commit_ref = resolve_rev(_head_ops(self._dml), revision, db=self._dml._db)
        commit_ref = _require_resolved_commit(commit_ref, revision)
        return _head_ops(self._dml).update_local_ref(name, commit_ref, kind="branch")

    def rename(
        self,
        old: Annotated[str, "Existing local branch name."],
        new: Annotated[str, "New local branch name."],
    ) -> str:
        """Rename one local branch."""
        head = _head_ops(self._dml)
        with head.lock():
            was_current = head.get_head()["branch"] == old
            renamed = head.rename_local_ref(old, new, kind="branch")
            if was_current:
                head.write_attached_head(new)
        return renamed

    def delete(self, name: Annotated[str, "Local branch name to delete."]) -> None:
        """Delete one local branch."""
        head = _head_ops(self._dml)
        with head.lock():
            if head.get_head()["branch"] == name:
                raise DmlRepoError(f"Cannot delete current branch: {name}")
            head.delete_local_ref(name, kind="branch")


@dataclass(frozen=True)
class _TagNamespace:
    _dml: "Dml"

    def list(
        self,
        *,
        remote: Annotated[bool, "List tags directly from the selected endpoint."] = False,
        dep: Annotated[str | None, "Dependency to inspect; list fetched or remote refs."] = None,
    ) -> list[RefListItem]:
        """List tag names and exact commit tips from the selected source."""
        return _list_ref_items(self._dml, kind="tag", remote=remote, dep=dep)

    def create(
        self,
        name: Annotated[str, "Local tag name to create."],
        revision: Annotated[Ref | str, "Revision to point the new tag at."] = "HEAD",
        *,
        remote: Annotated[bool, "Resolve the revision from fetched remote tracking refs."] = False,
    ) -> str:
        """Create one local tag at a resolved revision."""
        commit_ref = resolve_rev(_head_ops(self._dml), revision, db=self._dml._db, remote=remote)
        commit_ref = _require_resolved_commit(commit_ref, revision)
        return _head_ops(self._dml).create_local_ref(name, commit_ref, kind="tag")

    def delete(self, name: Annotated[str, "Local tag name to delete."]) -> None:
        """Delete one local tag."""
        _head_ops(self._dml).delete_local_ref(name, kind="tag")


RemoteGCSummary = TypedDict(
    "RemoteGCSummary",
    {
        "tombstones-deleted": int,
        "cas-deleted": int,
        "cas-retained": int,
        "total-refs": int,
        "gc-time": int,
        "ref-enumeration-time": int,
        "cas-enumeration-time": int,
    },
)


def _validate_cache_key(cache_key: object) -> str:
    if not isinstance(cache_key, str) or re.fullmatch(r"[a-z0-9][a-z0-9._-]*", cache_key) is None:
        raise ValueError(
            "Invalid cache key: expected a non-empty lowercase letter or digit segment using '.', '_', or '-'"
        )
    return cache_key


@dataclass(frozen=True)
class _CacheNamespace:
    _dml: "Dml"

    def get(self, cache_key: Annotated[str, "Exact cache key to resolve."]) -> Ref | None:
        """Return the cached DAG ref for a cache key, if present."""
        validated = _validate_cache_key(cache_key)
        return _exec_state(self._dml, cache_key=validated).get_cached_result(validated, self._dml._db)

    def describe(self, cache_key: Annotated[str, "Exact cache key to inspect."]) -> CacheDescription | None:
        """Describe the execution currently bound to a cache key."""
        validated = _validate_cache_key(cache_key)
        description = _exec_state(self._dml, cache_key=validated).describe_cache(validated)
        if description is None:
            return None
        result_ref = description["result_ref"]
        return {
            "execution": Ref(f"index:{description['execution_id']}"),
            "dag": None if result_ref is None else Ref(result_ref),
            "lifecycle": description["lifecycle"],
        }

    def invalidate(
        self, *executions: Annotated[Ref, "One or more runtime executions to invalidate."]
    ) -> InvalidationResponse:
        """Invalidate one or more remote executions and their current callers."""
        if not executions:
            raise ValueError("At least one execution is required")
        execution_ids = tuple(_require_runtime_ref(execution).id() for execution in executions)
        return _exec_state(self._dml).invalidate_executions(execution_ids, self._dml._config.user)


@dataclass(frozen=True)
class _DependencyNamespace:
    _dml: "Dml"

    def add(self, name: Annotated[str, "Dependency name."], root: Annotated[str, "Dependency S3 root."]) -> str:
        """Add one import-only dependency endpoint."""
        return _head_ops(self._dml).add_dependency(name, root)

    def list(self) -> dict[str, str]:
        """List configured import-only dependency endpoints."""
        head = _head_ops(self._dml)
        return {name: head.get_dependency_config(name)["root"] for name in head.list_dependencies()}

    def delete(self, name: Annotated[str, "Dependency name."]) -> None:
        """Delete one import-only dependency endpoint."""
        _head_ops(self._dml).delete_dependency(name)


LocalGCSummary = TypedDict(
    "LocalGCSummary",
    {
        "deleted": dict[str, int],
        "ref-enumeration-time": int,
        "gc-time": int,
    },
)


def _local_gc(dml: "Dml") -> LocalGCSummary:
    """Garbage-collect objects unreachable from repository and tracking refs."""
    t0 = time()
    head = _head_ops(dml)
    with head.lock():
        refs = set()
        commit = head.get_head()["commit"]
        if commit is not None:
            refs.add(commit)
        refs |= {head.get_local_ref(name, kind="branch") for name in head.list_local_refs(kind="branch")}
        refs |= {head.get_local_ref(name, kind="tag") for name in head.list_local_refs(kind="tag")}
        refs |= set(head.iter_all_remote_tracking_refs())
        shallow = head.get_shallow_commits()
        t1 = time()
        resp = dml._db.gc(sorted(refs), shallow)
        retained_shallow: set[Ref] = set()
        with dml._db.tx(readonly=True) as txn:
            for namespace in ("commit", "index", "frozenindex"):
                try:
                    objects = txn.iter(namespace)
                    for _ref, obj in objects:
                        retained_shallow.update(
                            parent for parent in obj.parents if parent in shallow and not txn.exists(parent)
                        )
                except DmlDbKeyNotFoundError:
                    pass
        head.write_shallow_commits(retained_shallow)
    t2 = time()
    return {"gc-time": int(t2 - t1), "ref-enumeration-time": int(t1 - t0), "deleted": resp}


@dataclass(frozen=True)
class _AdminNamespace:
    _dml: "Dml"

    def agent_skill(self) -> str:
        """Print the bundled coding-agent skill as portable Markdown."""
        return resources.files("daggerml").joinpath("SKILL.md").read_text(encoding="utf-8").removesuffix("\n")


class StatusPayload(TypedDict):
    mode: str
    branch: str | None
    commit: Ref | None
    branches: list[str]
    upstream: str | None
    num_indexes: int
    ahead: int | None
    behind: int | None


class RevisionPayload(TypedDict):
    input: str
    uri: str | None
    kind: Literal["head", "commit", "ref", "unknown"]
    commit: Ref | None
    branch: str | None
    tag: str | None


class LogPayload(TypedDict):
    commits: list[CommitDescription]
    truncated: bool


class Dml:
    def _init_from_config_vars(self, explicit_config: Mapping[str, object]) -> None:
        self._explicit_config = dict(explicit_config)
        self._config = Config.resolve(explicit=self._explicit_config)
        dflt = self._config.default
        self._db = DmlDB(self._config.db_path, dflt.db_map_size_headroom, dflt.db_map_size_max)
        self._s3_client = None

    def __init__(
        self,
        project_home: Annotated[str | None, "Project root containing the .dml repository."] = None,
        *,
        db_path: Annotated[str | None, "Override path to the LMDB database."] = None,
        db_map_size_headroom: Annotated[int | None, "Extra LMDB map size headroom in bytes."] = None,
        db_map_size_max: Annotated[int | None, "Maximum LMDB map size in bytes."] = None,
        default_branch_name: Annotated[str | None, "Default branch name for attached HEAD operations."] = None,
        remote_root: Annotated[str | None, "Remote storage root URI."] = None,
        remote_prune_age_seconds: Annotated[int | None, "Remote GC prune age in seconds."] = None,
        remote_fetch_workers: Annotated[int | None, "Number of concurrent remote fetch workers."] = None,
        user: Annotated[str | None, "User name recorded in commits and runtime actions."] = None,
        config_home: Annotated[str | None, "Override config directory path."] = None,
    ):
        """Create a DaggerML session bound to one repository and config context."""
        self._init_from_config_vars(
            _python_config_vars_to_canonical(
                project_home=project_home,
                db_path=db_path,
                db_map_size_headroom=db_map_size_headroom,
                db_map_size_max=db_map_size_max,
                default_branch_name=default_branch_name,
                remote_root=remote_root,
                remote_prune_age_seconds=remote_prune_age_seconds,
                remote_fetch_workers=remote_fetch_workers,
                user=user,
                config_home=config_home,
            )
        )

    @classmethod
    def from_config_vars(
        cls,
        config_vars: Annotated[dict[str, object], "Flattened canonical config-var mapping."] | None = None,
    ) -> "Dml":
        """Create a DaggerML session from flattened canonical config vars."""
        dml = cls.__new__(cls)
        dml._init_from_config_vars(config_vars or {})
        return dml

    @classmethod
    def init(
        cls,
        project_home: Annotated[str, "Directory where the repository should be initialized."] = ".",
        *,
        db_path: Annotated[str | None, "Override path to the LMDB database."] = None,
        db_map_size_headroom: Annotated[int | None, "Extra LMDB map size headroom in bytes."] = None,
        db_map_size_max: Annotated[int | None, "Maximum LMDB map size in bytes."] = None,
        default_branch_name: Annotated[str | None, "Default branch name for attached HEAD operations."] = None,
        remote_root: Annotated[str | None, "Remote storage root URI."] = None,
        remote_prune_age_seconds: Annotated[int | None, "Remote GC prune age in seconds."] = None,
        remote_fetch_workers: Annotated[int | None, "Number of concurrent remote fetch workers."] = None,
        user: Annotated[str | None, "User name recorded in commits and runtime actions."] = None,
        config_home: Annotated[str | None, "Override config directory path."] = None,
        branch: Annotated[str | None, "Initial branch name."] = None,
    ) -> "Dml":
        """Initialize a repository with an unborn attached HEAD."""
        config = Config.init(project_home, remote_root=remote_root)
        dml = cls.from_config_vars(
            _python_config_vars_to_canonical(
                project_home=config.project_home,
                db_path=db_path,
                db_map_size_headroom=db_map_size_headroom,
                db_map_size_max=db_map_size_max,
                default_branch_name=default_branch_name,
                remote_root=remote_root,
                remote_prune_age_seconds=remote_prune_age_seconds,
                remote_fetch_workers=remote_fetch_workers,
                user=user,
                config_home=config_home,
            )
        )
        head = Head(config.project_home)
        branch = branch or dml._config.default.branch_name
        with head.lock():
            try:
                head.get_head()
            except (DmlRepoError, FileNotFoundError):
                dml._db.init()
                head.init(None, branch)
        return dml

    @classmethod
    def clone(
        cls,
        revision: Annotated[Ref | str | None, "Optional branch, tag, or commit revision."] = None,
        /,
        *,
        project_home: Annotated[str, "Directory where the repository should be cloned."] = ".",
        db_path: Annotated[str | None, "Override path to the LMDB database."] = None,
        db_map_size_headroom: Annotated[int | None, "Extra LMDB map size headroom in bytes."] = None,
        db_map_size_max: Annotated[int | None, "Maximum LMDB map size in bytes."] = None,
        default_branch_name: Annotated[str | None, "Default branch name for attached HEAD operations."] = None,
        remote_root: Annotated[str | None, "Remote storage root URI."] = None,
        remote_prune_age_seconds: Annotated[int | None, "Remote GC prune age in seconds."] = None,
        remote_fetch_workers: Annotated[int | None, "Number of concurrent remote fetch workers."] = None,
        user: Annotated[str | None, "User name recorded in commits and runtime actions."] = None,
        config_home: Annotated[str | None, "Override config directory path."] = None,
        depth: Annotated[int | None, "Positive number of commit-history generations to fetch."] = None,
    ) -> "Dml":
        """Clone one revision from the configured remote root."""
        _validate_history_options(depth)
        Path(project_home).mkdir(parents=True, exist_ok=True)
        config = Config.init(project_home, remote_root=remote_root)
        dml = cls.from_config_vars(
            _python_config_vars_to_canonical(
                project_home=config.project_home,
                db_path=db_path,
                db_map_size_headroom=db_map_size_headroom,
                db_map_size_max=db_map_size_max,
                default_branch_name=default_branch_name,
                remote_root=remote_root,
                remote_prune_age_seconds=remote_prune_age_seconds,
                remote_fetch_workers=remote_fetch_workers,
                user=user,
                config_home=config_home,
            )
        )
        _require_remote_root(dml)
        selected = revision or dml._config.default.branch_name
        exact = selected if isinstance(selected, Ref) else None
        if isinstance(selected, str) and re.match(r"^(?:commit:)?[0-9a-f]{64}$", selected):
            exact = Ref(selected if selected.startswith("commit:") else f"commit:{selected}")
        branch = (
            selected
            if exact is None and isinstance(selected, str) and not selected.startswith(("@", "HEAD"))
            else None
        )
        initial_branch = branch or dml._config.default.branch_name
        head = Head(config.project_home)
        with head.lock():
            try:
                head.get_head()
            except (DmlRepoError, FileNotFoundError):
                dml._db.init()
                head.init(None, initial_branch)
            else:
                raise DmlRepoError(f"Cannot clone into an initialized repository: {dml._config.project_home}")
        if exact is not None:
            commit, available, omitted = _remote_ops(dml).materialize_project_commit_ref(exact, dml._db, depth=depth)
            with head.lock():
                _publish_shallow_state(head, available, omitted)
                head.write_detached_head(commit)
            return dml
        dml.fetch(selected if isinstance(selected, str) else None, depth=depth)
        kind = "tag" if isinstance(selected, str) and selected.startswith("@") else "branch"
        name = selected[1:] if kind == "tag" and isinstance(selected, str) else selected
        assert isinstance(name, str)
        with head.lock():
            commit = head.get_remote_tracking_ref(name, kind=kind)
            if kind == "tag":
                head.write_detached_head(commit)
            else:
                head.update_local_ref(name, commit, kind="branch")
                head.write_attached_head(name)
                head.set_upstream(name, name)
        return dml

    def status(self) -> StatusPayload:
        """Return branch, commit, and open-runtime status for this repository."""
        head = _head_ops(self)
        head_info = head.get_head()
        ahead = behind = None
        upstream = head.get_upstream(head_info["branch"]) if head_info["branch"] is not None else None
        if upstream is not None:
            try:
                upstream_ref = head.get_remote_tracking_ref(upstream["branch"])
            except DmlRepoError:
                pass
            else:
                if head_info["commit"] is not None:
                    try:
                        ahead, behind = CommitOps().ahead_behind(
                            head_info["commit"],
                            upstream_ref,
                            db=self._db,
                            missing_commits=head.get_shallow_commits(),
                        )
                    except ShallowHistoryError:
                        pass
        with self._db.tx(readonly=True) as txn:
            num_indexes = 0
            for namespace in ("index", "frozenindex"):
                try:
                    num_indexes += sum(1 for _ in txn.iter(namespace))
                except DmlDbKeyNotFoundError:
                    pass
        return {
            "mode": head_info["mode"],
            "branch": head_info["branch"],
            "commit": head_info["commit"],
            "branches": head.list_local_refs(kind="branch"),
            "upstream": upstream["branch"] if upstream is not None else None,
            "num_indexes": num_indexes,
            "ahead": ahead,
            "behind": behind,
        }

    def log(
        self,
        revision: Annotated[Ref | str, "Revision to start the log from."] = "HEAD",
        limit: Annotated[int, "Maximum number of commits to return."] = 10,
        *,
        remote: Annotated[bool, "Resolve from fetched remote tracking refs."] = False,
        dep: Annotated[str | None, "Resolve from a fetched dependency."] = None,
    ) -> LogPayload:
        """Return commit history starting from one revision."""
        commit_ref = resolve_rev(_head_ops(self), revision, db=self._db, remote=remote, dep=dep)
        commit_ref = _require_resolved_commit(commit_ref, revision)
        commits, truncated = CommitOps().log_with_truncation(
            commit_ref,
            limit=limit,
            db=self._db,
            missing_commits=_head_ops(self).get_shallow_commits(),
        )
        return {"commits": commits, "truncated": truncated}

    def show(
        self,
        revision: Annotated[Ref | str, "Revision to describe."] = "HEAD",
        *,
        remote: bool = False,
        dep: str | None = None,
    ) -> CommitFullDescription:
        """Return a full commit description for one revision."""
        commit_ref = resolve_rev(_head_ops(self), revision, db=self._db, remote=remote, dep=dep)
        commit_ref = _require_resolved_commit(commit_ref, revision)
        return CommitOps().show(
            commit_ref,
            db=self._db,
            missing_commits=_head_ops(self).get_shallow_commits(),
        )

    def diff(
        self,
        revision: Annotated[Ref | str, "Revision to diff."] = "HEAD",
        relative_to: Annotated[Ref | str | None, "Optional base revision. Defaults to the commit parent."] = None,
        *,
        remote: Annotated[bool, "Resolve the primary revision from remote tracking."] = False,
        dep: Annotated[str | None, "Resolve the primary revision from a dependency."] = None,
    ) -> CommitDiffPayload:
        """Return DAG-level changes for one revision."""
        head = _head_ops(self)
        commit_ref = resolve_rev(head, revision, db=self._db, remote=remote, dep=dep)
        commit_ref = _require_resolved_commit(commit_ref, revision)
        if relative_to is None:
            return CommitOps().diff(
                commit_ref,
                db=self._db,
                missing_commits=head.get_shallow_commits(),
            )
        rel_to_commit = resolve_rev(head, relative_to, db=self._db)
        rel_to_commit = _require_resolved_commit(rel_to_commit, relative_to)
        return CommitOps().diff(
            commit_ref,
            rel_to_commit,
            db=self._db,
            missing_commits=head.get_shallow_commits(),
        )

    def rev_parse(
        self,
        revision: Annotated[str, "Revision expression to resolve."],
        *,
        remote: bool = False,
        dep: str | None = None,
    ) -> RevisionPayload:
        """Resolve a revision expression into a commit and ref metadata."""
        head = _head_ops(self)
        branch = tag = None
        commit_ref = resolve_rev(head, revision, db=self._db, remote=remote, dep=dep)
        if revision.startswith("HEAD"):
            kind = "head"
        elif re.match(r"^(?:commit:)?[0-9a-f]{64}$", revision):
            kind = "commit"
        else:
            kind = "ref"
        if kind == "ref":
            branch, tag = (None, revision[1:]) if revision.startswith("@") else (revision, None)
        return {
            "input": revision,
            "uri": None,
            "kind": kind,
            "commit": commit_ref,
            "branch": branch,
            "tag": tag,
        }

    def revert(
        self,
        revision: Annotated[Ref | str, "Revision whose changes should be reverted."],
        message: Annotated[str | None, "Optional commit message for the revert commit."] = None,
        *,
        remote: bool = False,
    ) -> StatusPayload:
        """Revert the changes introduced by one revision."""
        head = _head_ops(self)
        with head.lock():
            head_info = head.get_head()
            if head_info["branch"] is None:
                raise DmlRepoError("Cannot revert when HEAD is detached")
            commit_ref = resolve_rev(head, revision, db=self._db, remote=remote)
            commit_ref = _require_resolved_commit(commit_ref, revision)
            new_commit = CommitOps().revert(
                commit_ref,
                _require_resolved_commit(head_info["commit"], "HEAD"),
                user=self._config.user,
                message=message,
                db=self._db,
                missing_commits=head.get_shallow_commits(),
            )
            head.update_local_ref(head_info["branch"], new_commit, kind="branch")
        return self.status()

    def checkout(
        self, revision: Annotated[Ref | str, "Revision to check out."], *, remote: bool = False
    ) -> StatusPayload:
        """Check out a different revision.

        If the revision resolves to a local branch, HEAD stays attached to that branch.
        Other revisions detach HEAD at the resolved commit.
        """
        head = _head_ops(self)
        with head.lock():
            commit_ref = resolve_rev(head, revision, db=self._db, remote=remote)
            if (
                not remote
                and isinstance(revision, str)
                and not revision.startswith(("@", "HEAD"))
                and not re.match(r"^(?:commit:)?[0-9a-f]{64}$", revision)
            ):
                head.write_attached_head(revision)
            else:
                head.write_detached_head(_require_resolved_commit(commit_ref, revision))
        return self.status()

    def merge(
        self,
        revision: Annotated[Ref | str, "Revision to merge into the current branch."],
        ff_only: Annotated[bool, "Whether to only allow fast-forward merges."] = True,
        *,
        remote: bool = False,
    ) -> StatusPayload:
        """Merge a revision into the current HEAD."""
        head = _head_ops(self)
        with head.lock():
            head_info = head.get_head()
            if head_info["branch"] is None:
                raise DmlRepoError("Cannot merge when HEAD is detached")
            commit_ref = resolve_rev(head, revision, db=self._db, remote=remote)
            commit_ref = _require_resolved_commit(commit_ref, revision)
            new_commit = CommitOps().merge(
                head_info["commit"],
                commit_ref,
                user=self._config.user,
                ff_only=ff_only,
                db=self._db,
                missing_commits=head.get_shallow_commits(),
            )
            head.update_local_ref(head_info["branch"], new_commit)
        return self.status()

    def rebase(
        self, revision: Annotated[Ref | str, "Revision to rebase the current branch onto."], *, remote: bool = False
    ) -> StatusPayload:
        """Rebase the current HEAD onto a different revision."""
        head = _head_ops(self)
        with head.lock():
            head_info = head.get_head()
            if head_info["branch"] is None:
                raise DmlRepoError("Cannot rebase when HEAD is detached")
            commit_ref = resolve_rev(head, revision, db=self._db, remote=remote)
            new_commit = CommitOps().rebase(
                _require_resolved_commit(head_info["commit"], "HEAD"),
                _require_resolved_commit(commit_ref, revision),
                user=self._config.user,
                db=self._db,
                missing_commits=head.get_shallow_commits(),
            )
            head.update_local_ref(head_info["branch"], new_commit)
        return self.status()

    def fetch(
        self,
        revision: Annotated[str | None, "Branch or @tag to fetch."] = None,
        /,
        *,
        dep: Annotated[str | None, "Named dependency endpoint to fetch from."] = None,
        depth: Annotated[int | None, "Positive number of commit-history generations to fetch."] = None,
        unshallow: Annotated[bool, "Fetch all history through existing shallow boundaries."] = False,
    ) -> None:
        """Fetch one branch or tag from remote.root or a named dependency."""
        _validate_history_options(depth, unshallow)
        selector = revision or self._config.default.branch_name
        kind = "tag" if selector.startswith("@") else "branch"
        name = selector[1:] if kind == "tag" else selector
        head = _head_ops(self)
        if dep is None:
            remote_ops = _remote_ops(self)
        else:
            config = head.get_dependency_config(dep)
            remote_ops = Remote(
                config["root"],
                n_workers=self._config.remote.fetch_workers,
                client=_require_s3_client(self),
                prune_age_seconds=self._config.remote.prune_age_seconds,
            )
        materialized = remote_ops.get_project_commit_ref(
            kind,
            name,
            db=self._db,
            depth=depth,
            unshallow=unshallow,
        )
        if materialized is None:
            raise DmlRepoError(f"Remote {kind} ref not found: {selector}")
        commit, available, omitted = materialized
        with head.lock():
            _publish_shallow_state(head, available, omitted)
            if dep is None:
                head.update_remote_tracking_ref(name, commit, kind=kind)
            else:
                head.update_dependency_ref(dep, name, commit, kind=kind)

    def pull(
        self,
        ff_only: Annotated[bool, "Whether to only allow fast-forward merges."] = True,
        *,
        depth: Annotated[int | None, "Positive number of commit-history generations to fetch."] = None,
    ) -> StatusPayload:
        """Fetch and merge the current branch's configured upstream."""
        _validate_history_options(depth)
        head = _head_ops(self)
        head_info = head.get_head()
        if head_info["branch"] is None:
            raise DmlRepoError("Cannot pull when HEAD is detached")
        upstream = head.get_upstream(head_info["branch"])
        if upstream is None:
            raise DmlRepoError(f"Cannot pull untracked branch: {head_info['branch']}")
        self.fetch(upstream["branch"], depth=depth)
        self.merge(upstream["branch"], ff_only=ff_only, remote=True)
        return self.status()

    def push(
        self,
        *,
        revision: Annotated[Ref | str, "Revision to push. Defaults to the current HEAD."] = "HEAD",
        force: Annotated[bool, "Overwrite a remote branch or tag without publication checks."] = False,
    ) -> None:
        """Publish the attached branch to its configured upstream."""
        head = _head_ops(self)
        if revision != "HEAD":
            raise DmlRepoError("Push only publishes the current attached branch")
        head_info = head.get_head()
        branch = head_info["branch"]
        if branch is None:
            raise DmlRepoError("Cannot push when HEAD is detached")
        commit_ref = _require_resolved_commit(head_info["commit"], "HEAD")
        upstream = head.get_upstream(branch)
        if upstream is None:
            upstream = {"branch": branch}
            set_upstream = True
        else:
            set_upstream = False
        remote = _remote_ops(self)
        remote.put_ref(
            commit_ref,
            kind="branch",
            name=upstream["branch"],
            db=self._db,
            force=force,
            missing_commits=head.get_shallow_commits(),
        )
        if set_upstream:
            with head.lock():
                head.set_upstream(branch, upstream["branch"])

    def gc(
        self,
        *,
        remote: Annotated[bool, "Garbage-collect configured remote state instead of local objects."] = False,
    ) -> LocalGCSummary | RemoteGCSummary:
        """Garbage-collect unreachable local or configured remote state."""
        if not remote:
            return _local_gc(self)
        return cast(RemoteGCSummary, _remote_ops(self).gc())

    @property
    def branch(self) -> Annotated[_BranchNamespace, "Branch inspection and lifecycle commands."]:
        """Expose branch inspection and lifecycle commands."""
        return _BranchNamespace(self)

    @property
    def dep(self) -> Annotated[_DependencyNamespace, "Import-only dependency lifecycle commands."]:
        """Expose import-only dependency lifecycle commands."""
        return _DependencyNamespace(self)

    @property
    def cache(self) -> Annotated[_CacheNamespace, "Remote execution cache inspection and control commands."]:
        """Expose remote execution cache inspection and control commands."""
        return _CacheNamespace(self)

    @property
    def tag(self) -> Annotated[_TagNamespace, "Tag inspection and lifecycle commands."]:
        """Expose tag inspection and lifecycle commands."""
        return _TagNamespace(self)

    @property
    def config(self) -> Annotated[_ConfigNamespace, "Configuration commands."]:
        """Expose configuration commands."""
        return _ConfigNamespace(self)

    @property
    def runtime(self) -> Annotated[_RuntimeNamespace, "Runtime mutation and execution-state inspection commands."]:
        """Expose runtime mutation and execution-state inspection commands."""
        return _RuntimeNamespace(self)

    @property
    def dag(self) -> Annotated[_DagNamespace, "Committed DAG inspection commands."]:
        """Expose committed DAG inspection commands."""
        return _DagNamespace(self)

    @property
    def admin(self) -> Annotated[_AdminNamespace, "Administrative support commands."]:
        """Expose remaining repository administration commands."""
        return _AdminNamespace(self)
