from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from functools import wraps
from pathlib import Path
from time import time
from typing import Annotated, Any, Literal, Mapping, NotRequired, TypedDict, cast, overload

from daggerml._core.commit import CommitDescription, CommitDiffPayload, CommitFullDescription, CommitOps
from daggerml._core.config import Config, flatten_dict
from daggerml._core.dag import DagDescription, DagOps, NodeDescriptionPayload
from daggerml._core.db import DmlDbKeyNotFoundError, Ref
from daggerml._core.exec_state import ExecutionGraph, ExecutionRecord, ExecutionState, InvalidationResponse
from daggerml._core.head import Head
from daggerml._core.index import IndexOps
from daggerml._core.remote import Remote
from daggerml._core.s3_cas import CasItemConflict
from daggerml._core.types import DmlDB, DmlRepoError, Error, TxnWithValid
from daggerml._core.uri import ProjectUri
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
        "cancel-requested": "dark_goldenrod italic",
        "cancel-ready": "dark_orange",
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
    return Remote(
        _require_remote_root(dml),
        n_workers=dml._config.remote.fetch_workers,
        client=_require_s3_client(dml),
        prune_age_seconds=dml._config.remote.prune_age_seconds,
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


def _reject_named_remote_selector(revision: str) -> None:
    if revision.startswith("origin/"):
        raise DmlRepoError(f"Unsupported named-remote selector: {revision}")


def _require_resolved_commit(commit: Ref | None, revision: Ref | str) -> Ref:
    if commit is None:
        raise DmlRepoError(f"Revision not found: {revision}")
    return commit


def resolve_rev(head: Head, revision: Ref | str, db: DmlDB) -> tuple[Ref | None, ProjectUri | None]:
    if isinstance(revision, Ref):
        return revision, None
    _reject_named_remote_selector(revision)
    if re.match(r"^[0-9a-f]{64}$", revision):
        return Ref(f"commit:{revision}"), None
    if re.match(r"^commit:[0-9a-f]{64}$", revision):
        return Ref(revision), None
    if revision.startswith("HEAD"):
        match = re.match(r"^HEAD~([0-9]+)$", revision)
        n = 0 if not match else int(match.group(1), 10)
        current = head.get_head()["commit"]
        if current is None:
            return None, None
        return CommitOps().get_ancestor(current, n, db=db), None
    if not revision.startswith(("dml://", "#", "@")):
        revision = f"#{revision}"  # treat it as a local branch by default
    uri = ProjectUri.from_uri(revision)

    def doit():
        if not uri.project:
            # local tag or branch
            if uri.branch:
                return head.get_local_ref(uri.branch, kind="branch")
            if uri.tag:
                return head.get_local_ref(uri.tag, kind="tag")
            raise DmlRepoError(f"Invalid local revision: {revision} (no branch nor tag specified)")
        # remote ref
        assert uri.owner is not None and uri.project is not None
        if uri.branch:
            return head.get_remote_ref(uri.owner, uri.project, uri.branch, kind="branch")
        if uri.tag:
            return head.get_remote_ref(uri.owner, uri.project, uri.tag, kind="tag")
        raise DmlRepoError(f"Invalid remote revision: {revision} (no branch nor tag specified)")

    return doit(), uri


_PYTHON_CONFIG_VAR_NAMES = {
    "project_home": "project_home",
    "db_path": "db_path",
    "db_map_size_headroom": "default.db_map_size_headroom",
    "db_map_size_max": "default.db_map_size_max",
    "default_branch_name": "default.branch_name",
    "remote_project": "remote.project",
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
    project: str | None
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


class RuntimeListPayload(TypedDict):
    id: Ref
    parents: list[Ref]
    tree: Ref
    author: str
    message: str
    created: str
    dag: Ref


RuntimeCancelSummary = TypedDict(
    "RuntimeCancelSummary",
    {
        "id": Ref,
        "active-callers": list[Ref],
        "inactive": list[Ref],
        "cancelled": list[Ref],
        "timeout": list[Ref],
        "error": list[Ref],
    },
)


@dataclass(frozen=True)
class _RuntimeNamespace:
    _dml: "Dml"

    @_retry_runtime_mutation
    def create(
        self,
        cache_key: Annotated[str | None, "Cache key to reuse execution results."] = None,
        execution_id: Annotated[str | None, "Execution id for adapter-coordinated runs."] = None,
    ) -> Ref:
        """Create a new mutable runtime index."""
        if (cache_key is None) != (execution_id is None):
            raise DmlRepoError("both cache_key and execution_id must be provided or neither")
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

    def describe(self, index: Annotated[Ref, "Runtime index to inspect."]) -> RuntimeDescribe:
        """Describe one runtime index."""
        db = self._dml._db
        with db.tx(readonly=True) as txn:
            idx = txn.get(index)
            return {
                "id": index,
                "parents": idx.parents,
                "tree": idx.tree,
                "author": idx.author,
                "message": idx.message,
                "created": idx.created,
                "dag": idx.dag,
            }

    def list(self) -> list[RuntimeListPayload]:
        """List open runtime indexes in reverse creation order."""
        objs: list[RuntimeListPayload] = []
        with self._dml._db.tx(readonly=True) as txn:
            try:
                index_items = list(txn.iter("index"))
            except DmlDbKeyNotFoundError:
                index_items = []
            for x, obj in index_items:
                objs.append(
                    {
                        "id": x,
                        "parents": obj.parents,
                        "tree": obj.tree,
                        "author": obj.author,
                        "message": obj.message,
                        "created": obj.created,
                        "dag": obj.dag,
                    }
                )
        return sorted(objs, key=lambda x: x["created"], reverse=True)

    def read_execution_record(
        self,
        execution: Annotated[Ref | str, "Runtime index ref or execution id to inspect."],
    ) -> ExecutionRecord:
        """Read the raw execution record for one runtime execution."""
        execution_id = execution.id() if isinstance(execution, Ref) else execution
        return _exec_state(self._dml).read_execution_record(execution_id)

    def cancel(
        self,
        index: Annotated[Ref | str, "Runtime index to cancel."],
        *,
        mode: Annotated[Literal["full", "drive"], "Cancellation mode."] = "full",
    ) -> RuntimeCancelSummary:
        """Cancel active execution state for a runtime index."""
        if isinstance(index, str) and index.startswith("index:"):
            index = Ref(index)
        requested_by = self._dml._config.user if mode == "full" else None
        idx = index.id() if isinstance(index, Ref) else index
        resp = _exec_state(self._dml).cancel(idx, requested_by, self._dml._db, mode=mode)
        return cast(RuntimeCancelSummary, {"id": index, **resp})

    def describe_graph(
        self,
        *roots: Annotated[Ref | str, "Execution roots to inspect."],
        visual: Annotated[bool, "Render a human-friendly graph view instead of returning the raw payload."] = False,
    ) -> ExecutionGraph | None:
        """Describe reachable execution lineage for one or more runtime roots."""
        execution_ids = [root.id() if isinstance(root, Ref) else root for root in roots]
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
        dag: Annotated[Ref, "Committed DAG ref to copy into the current branch."],
        name: Annotated[str, "Name to assign to the checked out DAG."],
    ) -> None:
        """Copy a committed DAG into the current branch under a new name."""
        head = _head_ops(self._dml)
        with head.lock():
            head_info = head.get_head()
            if not head_info["branch"]:
                raise DmlRepoError("Cannot checkout DAG when HEAD is detached")
            new_commit = CommitOps().checkout_dag(
                head_info["commit"],
                dag,
                name=name,
                user=self._dml._config.user,
                db=self._dml._db,
            )
            head.update_local_ref(head_info["branch"], new_commit)

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

    def list(self) -> list[str]:
        """List local branch names."""
        return _head_ops(self._dml).list_local_refs(kind="branch")

    def create(
        self,
        name: Annotated[str, "Local branch name to create."],
        revision: Annotated[Ref | str, "Revision to point the new branch at."] = "HEAD",
    ) -> str:
        """Create one local branch at a resolved revision."""
        head = _head_ops(self._dml)
        with head.lock():
            commit_ref, _ = resolve_rev(head, revision, db=self._dml._db)
            if revision == "HEAD":
                head_info = head.get_head()
                if commit_ref is None and head_info["mode"] == "attached":
                    if head.local_ref_path(name, kind="branch").exists():
                        raise DmlRepoError(f"Branch already exists: {name}")
                    head.write_attached_head(name)
                    return name
            commit_ref = _require_resolved_commit(commit_ref, revision)
            return head.create_local_ref(name, commit_ref, kind="branch")

    def move(
        self,
        name: Annotated[str, "Local branch name to repoint."],
        revision: Annotated[Ref | str, "Revision to point the branch at."],
    ) -> str:
        """Repoint one local branch to a resolved revision."""
        commit_ref, _ = resolve_rev(_head_ops(self._dml), revision, db=self._dml._db)
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

    def list(self) -> list[str]:
        """List local tag names."""
        return _head_ops(self._dml).list_local_refs(kind="tag")

    def create(
        self,
        name: Annotated[str, "Local tag name to create."],
        revision: Annotated[Ref | str, "Revision to point the new tag at."] = "HEAD",
    ) -> str:
        """Create one local tag at a resolved revision."""
        commit_ref, _ = resolve_rev(_head_ops(self._dml), revision, db=self._dml._db)
        commit_ref = _require_resolved_commit(commit_ref, revision)
        return _head_ops(self._dml).create_local_ref(name, commit_ref, kind="tag")

    def delete(self, name: Annotated[str, "Local tag name to delete."]) -> None:
        """Delete one local tag."""
        _head_ops(self._dml).delete_local_ref(name, kind="tag")


GCSummary = TypedDict(
    "GCSummary",
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


class RemoteRefListPayload(TypedDict):
    refs: list[str]


@dataclass(frozen=True)
class _RemoteNamespace:
    _dml: "Dml"

    def get_cache(self, cache_key: Annotated[str, "Cache key to resolve."]) -> Ref | None:
        """Return the cached DAG ref for a cache key, if present."""
        return _remote_ops(self._dml).get_cache(cache_key, raw=False, db=self._dml._db)

    def invalidate_cache(
        self, *cache_key: Annotated[str, "One or more cache keys to invalidate."]
    ) -> InvalidationResponse:
        """Invalidate one or more remote execution cache keys."""
        return _exec_state(self._dml).invalidate_cache(cache_key, self._dml._config.user)

    def gc(self) -> GCSummary:
        """Garbage-collect remote CAS data and tombstones."""
        return cast(GCSummary, _remote_ops(self._dml).gc())

    def list_projects(
        self, owner: Annotated[str | None, "Optional owner to filter by."] = None
    ) -> RemoteRefListPayload:
        """List remote projects visible under the configured remote root."""
        return {"refs": [str(x) for x in _remote_ops(self._dml).list_projects(owner)]}

    def list_refs(
        self,
        project_uri: Annotated[str, "Project URI whose refs should be listed."],
        kind: Annotated[Literal["tag", "branch"], "Whether to list branches or tags."] = "branch",
    ) -> RemoteRefListPayload:
        """List remote branch or tag refs for one project."""
        uri = ProjectUri.from_uri(project_uri).ensure_project()
        # TODO: replace with proper pagination if there are many refs
        return {"refs": [str(x) for x in _remote_ops(self._dml).list_refs(uri, kind=kind)]}


LocalGCSummary = TypedDict(
    "LocalGCSummary",
    {
        "deleted": dict[str, int],
        "ref-enumeration-time": int,
        "gc-time": int,
    },
)


@dataclass(frozen=True)
class _AdminNamespace:
    _dml: "Dml"

    @property
    def remote(self) -> Annotated[_RemoteNamespace, "Remote cache, refs, and GC commands."]:
        """Expose remote administration commands."""
        return _RemoteNamespace(self._dml)

    def gc(self) -> LocalGCSummary:
        """Garbage-collect unreachable local objects."""
        # get all commits referenced by all local branches, tags, etc.
        t0 = time()
        head = _head_ops(self._dml)
        refs = set()
        commit = head.get_head()["commit"]
        if commit is not None:
            refs.add(commit)
        refs |= {head.get_local_ref(name, kind="branch") for name in head.list_local_refs(kind="branch")}
        refs |= {head.get_local_ref(name, kind="tag") for name in head.list_local_refs(kind="tag")}
        refs |= {
            head.get_remote_ref(owner, project, name, kind="branch")
            for owner, project in head.list_remote_projects()
            for name in head.list_remote_refs(owner, project, kind="branch")
        }
        refs |= {
            head.get_remote_ref(owner, project, name, kind="tag")
            for owner, project in head.list_remote_projects()
            for name in head.list_remote_refs(owner, project, kind="tag")
        }
        t1 = time()
        resp = self._dml._db.gc(sorted(refs))
        t2 = time()
        return {"gc-time": int(t2 - t1), "ref-enumeration-time": int(t1 - t0), "deleted": resp}


class StatusPayload(TypedDict):
    mode: str
    branch: str | None
    commit: Ref | None
    branches: list[str]
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
        remote_project: Annotated[str | None, "Default remote project URI."] = None,
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
                remote_project=remote_project,
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
        remote_project: Annotated[str | None, "Default remote project URI to store in config."] = None,
        remote_root: Annotated[str | None, "Remote storage root URI."] = None,
        remote_prune_age_seconds: Annotated[int | None, "Remote GC prune age in seconds."] = None,
        remote_fetch_workers: Annotated[int | None, "Number of concurrent remote fetch workers."] = None,
        user: Annotated[str | None, "User name recorded in commits and runtime actions."] = None,
        config_home: Annotated[str | None, "Override config directory path."] = None,
        branch: Annotated[str | None, "Initial branch name."] = None,
    ) -> "Dml":
        """Initialize a repository with an unborn attached HEAD."""
        config = Config.init(project_home, remote_root=remote_root, remote_project=remote_project)
        dml = cls.from_config_vars(
            _python_config_vars_to_canonical(
                project_home=config.project_home,
                db_path=db_path,
                db_map_size_headroom=db_map_size_headroom,
                db_map_size_max=db_map_size_max,
                default_branch_name=default_branch_name,
                remote_project=remote_project,
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
        project_uri: Annotated[str, "Remote project, branch, or tag URI to clone."],
        project_home: Annotated[str, "Directory where the repository should be cloned."] = ".",
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
    ) -> "Dml":
        """Clone a remote project ref into a newly initialized local repository."""
        uri = ProjectUri.from_uri(project_uri)
        if uri.owner is None or uri.project is None:
            raise DmlRepoError(f"Clone requires a full remote project URI: {project_uri}")
        Path(project_home).mkdir(parents=True, exist_ok=True)
        remote_project = str(ProjectUri(uri.owner, uri.project))
        config = Config.init(project_home, remote_root=remote_root, remote_project=remote_project)
        dml = cls.from_config_vars(
            _python_config_vars_to_canonical(
                project_home=config.project_home,
                db_path=db_path,
                db_map_size_headroom=db_map_size_headroom,
                db_map_size_max=db_map_size_max,
                default_branch_name=default_branch_name,
                remote_project=remote_project,
                remote_root=remote_root,
                remote_prune_age_seconds=remote_prune_age_seconds,
                remote_fetch_workers=remote_fetch_workers,
                user=user,
                config_home=config_home,
            )
        )
        branch = uri.branch or (None if uri.tag is not None else dml._config.default.branch_name)
        initial_branch = branch or dml._config.default.branch_name
        clone_uri = str(ProjectUri(uri.owner, uri.project, branch=branch, tag=uri.tag))
        head = Head(config.project_home)
        with head.lock():
            try:
                head.get_head()
            except (DmlRepoError, FileNotFoundError):
                dml._db.init()
                head.init(None, initial_branch)
            else:
                raise DmlRepoError(f"Cannot clone into an initialized repository: {dml._config.project_home}")
        dml.fetch(clone_uri)
        name = uri.tag or branch
        assert name is not None
        kind = "tag" if uri.tag is not None else "branch"
        with head.lock():
            commit = head.get_remote_ref(uri.owner, uri.project, name, kind=kind)
            if kind == "tag":
                head.write_detached_head(commit)
            else:
                head.update_local_ref(name, commit, kind="branch")
                head.write_attached_head(name)
        return dml

    def status(self) -> StatusPayload:
        """Return branch, commit, and open-runtime status for this repository."""
        head = _head_ops(self)
        head_info = head.get_head()
        ahead = behind = None
        if head_info["branch"] is not None and self._config.remote.project is not None:
            uri = ProjectUri.from_uri(self._config.remote.project).ensure_project()
            try:
                upstream = head.get_remote_ref(uri.owner, uri.project, head_info["branch"], kind="branch")
            except DmlRepoError:
                pass
            else:
                if head_info["commit"] is not None:
                    ahead, behind = CommitOps().ahead_behind(head_info["commit"], upstream, db=self._db)
        with self._db.tx(readonly=True) as txn:
            try:
                num_indexes = sum(1 for _ in txn.iter("index"))
            except DmlDbKeyNotFoundError:
                num_indexes = 0
        return {
            "mode": head_info["mode"],
            "branch": head_info["branch"],
            "commit": head_info["commit"],
            "branches": head.list_local_refs(kind="branch"),
            "num_indexes": num_indexes,
            "ahead": ahead,
            "behind": behind,
        }

    def log(
        self,
        revision: Annotated[Ref | str, "Revision to start the log from."] = "HEAD",
        limit: Annotated[int, "Maximum number of commits to return."] = 10,
    ) -> LogPayload:
        """Return commit history starting from one revision."""
        commit_ref, _ = resolve_rev(_head_ops(self), revision, db=self._db)
        commit_ref = _require_resolved_commit(commit_ref, revision)
        return {"commits": CommitOps().log(commit_ref, limit=limit, db=self._db)}

    def show(self, revision: Annotated[Ref | str, "Revision to describe."] = "HEAD") -> CommitFullDescription:
        """Return a full commit description for one revision."""
        commit_ref, _ = resolve_rev(_head_ops(self), revision, db=self._db)
        commit_ref = _require_resolved_commit(commit_ref, revision)
        return CommitOps().show(commit_ref, db=self._db)

    def diff(
        self,
        revision: Annotated[Ref | str, "Revision to diff."] = "HEAD",
        relative_to: Annotated[Ref | str | None, "Optional base revision. Defaults to the commit parent."] = None,
    ) -> CommitDiffPayload:
        """Return DAG-level changes for one revision."""
        head = _head_ops(self)
        commit_ref, _ = resolve_rev(head, revision, db=self._db)
        commit_ref = _require_resolved_commit(commit_ref, revision)
        if relative_to is None:
            return CommitOps().diff(commit_ref, db=self._db)
        rel_to_commit, _ = resolve_rev(head, relative_to, db=self._db)
        rel_to_commit = _require_resolved_commit(rel_to_commit, relative_to)
        return CommitOps().diff(commit_ref, rel_to_commit, db=self._db)

    def rev_parse(self, revision: Annotated[str, "Revision expression to resolve."]) -> RevisionPayload:
        """Resolve a revision expression into a commit and ref metadata."""
        head = _head_ops(self)
        branch = tag = None
        commit_ref, uri = resolve_rev(head, revision, db=self._db)
        if uri is None:
            if revision.startswith("HEAD"):
                kind = "head"
            elif re.match(r"^[0-9a-f]{64}$", revision) or re.match(r"^commit:[0-9a-f]{64}$", revision):
                kind = "commit"
            else:
                kind = "unknown"
        else:
            branch, tag, kind = uri.branch, uri.tag, "ref"
        return {
            "input": revision,
            "uri": str(uri) if uri else None,
            "kind": kind,
            "commit": commit_ref,
            "branch": branch,
            "tag": tag,
        }

    def revert(
        self,
        revision: Annotated[Ref | str, "Revision whose changes should be reverted."],
        message: Annotated[str | None, "Optional commit message for the revert commit."] = None,
    ) -> StatusPayload:
        """Revert the changes introduced by one revision."""
        head = _head_ops(self)
        with head.lock():
            head_info = head.get_head()
            if head_info["branch"] is None:
                raise DmlRepoError("Cannot revert when HEAD is detached")
            commit_ref, _ = resolve_rev(head, revision, db=self._db)
            commit_ref = _require_resolved_commit(commit_ref, revision)
            new_commit = CommitOps().revert(
                commit_ref,
                _require_resolved_commit(head_info["commit"], "HEAD"),
                user=self._config.user,
                message=message,
                db=self._db,
            )
            head.update_local_ref(head_info["branch"], new_commit, kind="branch")
        return self.status()

    def checkout(self, revision: Annotated[Ref | str, "Revision to check out."]) -> StatusPayload:
        """Check out a different revision.

        If the revision resolves to a local branch, HEAD stays attached to that branch.
        Other revisions detach HEAD at the resolved commit.
        """
        head = _head_ops(self)
        with head.lock():
            commit_ref, uri = resolve_rev(head, revision, db=self._db)
            if uri is None or uri.branch is None or uri.project is not None:
                commit_ref = _require_resolved_commit(commit_ref, revision)
                head.write_detached_head(commit_ref)
            elif uri.project is None and uri.branch is not None:
                # attached
                head.write_attached_head(uri.branch)
            else:
                raise DmlRepoError(f"Unsupported revision URI: {uri}")
        return self.status()

    def merge(
        self,
        revision: Annotated[Ref | str, "Revision to merge into the current branch."],
        ff_only: Annotated[bool, "Whether to only allow fast-forward merges."] = True,
    ) -> StatusPayload:
        """Merge a revision into the current HEAD."""
        head = _head_ops(self)
        with head.lock():
            head_info = head.get_head()
            if head_info["branch"] is None:
                raise DmlRepoError("Cannot merge when HEAD is detached")
            commit_ref, _ = resolve_rev(head, revision, db=self._db)
            commit_ref = _require_resolved_commit(commit_ref, revision)
            new_commit = CommitOps().merge(
                head_info["commit"],
                commit_ref,
                user=self._config.user,
                ff_only=ff_only,
                db=self._db,
            )
            head.update_local_ref(head_info["branch"], new_commit)
        return self.status()

    def rebase(self, revision: Annotated[Ref | str, "Revision to rebase the current branch onto."]) -> StatusPayload:
        """Rebase the current HEAD onto a different revision."""
        head = _head_ops(self)
        with head.lock():
            head_info = head.get_head()
            if head_info["branch"] is None:
                raise DmlRepoError("Cannot rebase when HEAD is detached")
            commit_ref, _ = resolve_rev(head, revision, db=self._db)
            new_commit = CommitOps().rebase(
                _require_resolved_commit(head_info["commit"], "HEAD"),
                _require_resolved_commit(commit_ref, revision),
                user=self._config.user,
                db=self._db,
            )
            head.update_local_ref(head_info["branch"], new_commit)
        return self.status()

    def fetch(self, project_uri: Annotated[str, "Remote project, branch, or tag URI to fetch."]) -> None:
        """Fetch branches and tags from a remote project.

        Note: `project_uri` may be a bare tag or branch (e.g. #my-branch or @my-tag)
        """
        uri = ProjectUri.from_uri(project_uri)
        if uri.project is None or uri.owner is None:
            if self._config.remote.project is None:
                raise DmlRepoError(f"Remote project must be specified in URI or config to fetch: {project_uri}")
            conf_uri = ProjectUri.from_uri(self._config.remote.project)
            uri = ProjectUri(
                owner=conf_uri.owner,
                project=conf_uri.project,
                branch=uri.branch,
                tag=uri.tag,
            )
        assert uri.owner is not None and uri.project is not None
        name = uri.tag or uri.branch or self._config.default.branch_name
        kind = "tag" if uri.tag is not None else "branch"
        head = _head_ops(self)
        commit = _remote_ops(self).get_ref(uri.owner, uri.project, kind, name, db=self._db)
        if commit is None:
            raise DmlRepoError(f"Remote {kind} ref not found: {uri}")
        with head.lock():
            head.update_remote_ref(uri.owner, uri.project, name, commit, kind=kind)

    def pull(self, ff_only: Annotated[bool, "Whether to only allow fast-forward merges."] = True) -> StatusPayload:
        """Pull the latest changes from the remote project of the current branch, if any."""
        # TODO: should enforce FF-only with a force: False option to overwrite local.
        if self._config.remote.project is None:
            raise DmlRepoError("Cannot pull without remote.project configured")
        head = _head_ops(self)
        head_info = head.get_head()
        if head_info["branch"] is None:
            raise DmlRepoError("Cannot pull when HEAD is detached")
        fetch_uri = f"{self._config.remote.project}#{head_info['branch']}"
        self.fetch(fetch_uri)
        self.merge(fetch_uri, ff_only=ff_only)
        return self.status()

    def push(
        self,
        revision: Annotated[Ref | str, "Revision to push. Defaults to the current HEAD."] = "HEAD",
        *,
        delete: Annotated[bool, "Delete the selected remote branch or tag instead of publishing it."] = False,
        force: Annotated[bool, "Overwrite a remote branch or tag without publication checks."] = False,
    ) -> None:
        """Push or delete a branch or tag on the configured remote project."""
        if self._config.remote.project is None:
            raise DmlRepoError("Cannot push without remote.project configured")
        if delete and revision == "HEAD":
            raise DmlRepoError("push --delete requires an explicit branch or tag selector")
        head = _head_ops(self)
        commit_ref = None
        parsed_uri = None
        if delete:
            if isinstance(revision, Ref):
                raise DmlRepoError(f"Unsupported revision for push: {revision}")
            _reject_named_remote_selector(revision)
            selector = revision if revision.startswith(("dml://", "#", "@")) else f"#{revision}"
            parsed_uri = ProjectUri.from_uri(selector)
            if parsed_uri.project is None or parsed_uri.owner is None:
                remote_project = ProjectUri.from_uri(self._config.remote.project)
                parsed_uri = ProjectUri(
                    owner=remote_project.owner,
                    project=remote_project.project,
                    branch=parsed_uri.branch,
                    tag=parsed_uri.tag,
                )
            name = parsed_uri.tag or parsed_uri.branch
            kind = "tag" if parsed_uri.tag is not None else "branch"
            if name is None:
                raise DmlRepoError(f"Unsupported revision for push: {revision} (no branch or tag specified)")
        elif revision == "HEAD":
            head_info = head.get_head()
            commit_ref = head_info["commit"]
            name = head_info["branch"]
            kind = "branch"
            if name is None:
                raise DmlRepoError(f"Cannot push detached HEAD revision: {revision}")
            commit_ref = _require_resolved_commit(commit_ref, revision)
        else:
            commit_ref, parsed_uri = resolve_rev(head, revision, db=self._db)
            if parsed_uri is None:
                raise DmlRepoError(f"Unsupported revision for push: {revision}")
            commit_ref = _require_resolved_commit(commit_ref, revision)
            name = parsed_uri.tag or parsed_uri.branch
            kind = "tag" if parsed_uri.tag is not None else "branch"
            if name is None:
                raise DmlRepoError(f"Unsupported revision for push: {revision} (no branch or tag specified)")
            if parsed_uri.project is not None:
                remote_project = f"dml://{parsed_uri.owner}/{parsed_uri.project}"
                if remote_project != self._config.remote.project:
                    raise DmlRepoError(
                        f"Revision {revision} does not match remote.project {self._config.remote.project}"
                    )
        uri = ProjectUri.from_uri(self._config.remote.project)
        assert uri.owner is not None and uri.project is not None
        remote = _remote_ops(self)
        if delete:
            remote.delete_ref(uri.owner, uri.project, kind=kind, name=name)
            return
        assert commit_ref is not None
        remote.put_ref(commit_ref, uri.owner, uri.project, kind=kind, name=name, db=self._db, force=force)

    @property
    def branch(self) -> Annotated[_BranchNamespace, "Local branch lifecycle commands."]:
        """Expose local branch lifecycle commands."""
        return _BranchNamespace(self)

    @property
    def tag(self) -> Annotated[_TagNamespace, "Local tag lifecycle commands."]:
        """Expose local tag lifecycle commands."""
        return _TagNamespace(self)

    @property
    def config(self) -> Annotated[_ConfigNamespace, "Configuration commands."]:
        """Expose configuration commands."""
        return _ConfigNamespace(self)

    @property
    def runtime(self) -> Annotated[_RuntimeNamespace, "Mutable runtime index commands."]:
        """Expose runtime index commands."""
        return _RuntimeNamespace(self)

    @property
    def dag(self) -> Annotated[_DagNamespace, "Committed DAG inspection commands."]:
        """Expose committed DAG inspection commands."""
        return _DagNamespace(self)

    @property
    def admin(self) -> Annotated[_AdminNamespace, "Administrative and remote maintenance commands."]:
        """Expose repository administration commands."""
        return _AdminNamespace(self)
