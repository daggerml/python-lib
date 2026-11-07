"""Commit pseudo-mutation operations.

This module is for creating commits that retain the parent. The active runtime while building a commit.

This module delegates to:
- `remote.py` for remote interactions
- `exec_state.py` for execution state
- `builtins.py` for built-in function execution
- `dag.py` for DAG read-only queries

This module does NOT handle index files, pointers, or branches.

Public API:
    IndexOps - Class for index and execution operations
"""

from __future__ import annotations

import json
import logging
from dataclasses import InitVar, dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Optional, cast

from daggerml._core.builtins import BUILTIN_FNS
from daggerml._core.db import Ref
from daggerml._core.exec_state import ExecutionState
from daggerml._core.remote import Remote
from daggerml._core.types import (
    ArgvNode,
    Commit,
    Dag,
    DictDatum,
    DmlDB,
    DmlRepoError,
    Error,
    FnNode,
    FrozenIndex,
    ImportNode,
    Index,
    ListDatum,
    LiteralNode,
    Runnable,
    RunnableDatum,
    ScalarDatum,
    Tree,
    Uri,
    UriDatum,
)
from daggerml._core.util import now, unnest, uuid7

if TYPE_CHECKING:
    import boto3

logger = logging.getLogger(__name__)


@dataclass
class IndexOps:
    remote_root: str
    n_workers: InitVar[int]
    client: InitVar["boto3.client"]
    _remote: Remote = field(init=False)

    def __post_init__(self, n_workers, client):
        self._remote = Remote(self.remote_root, n_workers=n_workers, client=client)

    def exec_state(self, cache_key: str | None = None) -> ExecutionState:
        kw = {"n_workers": self._remote.n_workers, "client": self._remote._store.client}
        return ExecutionState(self.remote_root, cache_key=cache_key, **kw)

    def _require_mutation(self, index: Ref, db: DmlDB, *, mode: Literal["activation", "mutation"] = "mutation") -> None:
        self.exec_state().require_mutation(index.id(), db, mode=mode)

    @staticmethod
    def _normalize_tags(tags: list[str] | None) -> list[str]:
        if tags is None:
            return []
        if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
            raise DmlRepoError("DAG tags must be a list of strings")
        return sorted(set(tags))

    def _update_dag(self, index: Ref, node: Ref, name: Optional[str], ctx, txn) -> Ref:
        if node not in ctx.dag.nodes:
            raise DmlRepoError("Fin node must be part of DAG.")
        if name is not None:
            ctx.dag.names[name] = node
        cast(Index, ctx.commit).dag = txn.put(ctx.dag)
        txn.put(ctx.commit, to=index)
        return node

    def create(
        self,
        author: str,
        commit: Ref | None,
        cache_key: str | None = None,
        execution_id: str | None = None,
        tags: list[str] | None = None,
        *,
        db: DmlDB,
    ) -> Ref:
        if (cache_key is None) != (execution_id is None):
            raise DmlRepoError("Must provide either both (execution_id and cache_key) or neither.")
        state = self.exec_state(cache_key=cache_key)
        exec_id = execution_id or uuid7().hex
        if execution_id is not None:
            record, owner = state.activate(exec_id, db)
            try:
                state.require_mutation(exec_id, db, mode="activation")
                metadata = record["metadata"]
                if metadata["cache_key"] != cache_key or metadata["argv_ref"] is None:
                    raise DmlRepoError(f"Invalid execution payload for cache key: {cache_key}")
                state.mark_running(exec_id, owner)
            except Exception:
                state.unlock(exec_id, owner)
                raise
            argv = self._remote.materialize_ref(Ref(metadata["argv_ref"]), db)
        else:
            argv = None
            _, owner, _ = state.reserve_execution(None, execution_id=exec_id)
            state.unlock(exec_id, owner)

        def create_index(txn) -> Ref:
            nodes: list[Ref] = [argv] if argv is not None else []
            parents: list[Ref] = []
            if commit is None:
                base_tree = txn.put(Tree(dags={}))
            else:
                base_commit: Commit = txn.get(commit)
                base_tree = base_commit.tree
                parents = [commit]
            # create db state
            dag_ref = txn.put(Dag(nodes=nodes, names={}, tags=self._normalize_tags(tags), argv=argv))
            return txn.put(
                Index(
                    parents=parents,
                    tree=base_tree,
                    author=author,
                    message="",
                    dag=dag_ref,
                ),
                to=Ref(f"index:{exec_id}"),
            )

        if execution_id is not None:
            state.require_mutation(exec_id, db, mode="mutation")
        return db.write_with_growth(create_index)

    def freeze(self, index: Ref, message: str | None, *, db: DmlDB) -> Ref:
        """Preserve local state before publishing a durable suspension fence."""
        frozen_ref = Ref(f"frozenindex:{index.id()}")
        with db.tx(readonly=True) as txn:
            staged = txn.get(frozen_ref) if txn.exists(frozen_ref) and not txn.exists(index) else None
        if staged is not None:
            self.exec_state().suspend_runtime(index.id(), staged.previous_generation, staged.runtime_generation)
            return frozen_ref
        self._require_mutation(index, db)
        checkpoint = uuid7().hex
        with db.tx(readonly=True) as txn:
            original = txn.get(index)

        def freeze_index(txn) -> Ref:
            idx = txn.get(index)
            if not isinstance(idx, Index):
                raise DmlRepoError(f"Runtime is not an active index: {index}")
            frozen = txn.put(
                FrozenIndex(
                    parents=idx.parents,
                    tree=idx.tree,
                    author=idx.author,
                    message=idx.message,
                    created=idx.created,
                    dag=idx.dag,
                    frozen_message=message,
                    runtime_generation=checkpoint,
                    previous_generation=idx.runtime_generation,
                ),
                to=Ref(f"frozenindex:{index.id()}"),
            )
            txn.delete(index)
            return frozen

        frozen = db.write_with_growth(freeze_index)
        self.exec_state().suspend_runtime(index.id(), original.runtime_generation, checkpoint)
        return frozen

    def export_snapshot(self, index: Ref, *, db: DmlDB) -> str:
        """Export a frozen runtime and its complete repository-object closure as JSON."""
        with db.tx(readonly=True) as txn:
            frozen = txn.get(index)
            if not isinstance(frozen, FrozenIndex):
                raise DmlRepoError("Snapshot export requires a frozen runtime")
            objects = {}
            pending = [index]
            while pending:
                ref = pending.pop()
                if ref.to in objects:
                    continue
                obj = txn.get(ref)
                objects[ref.to] = self._remote._dump_cas_object(obj)
                deps: set[Ref] = set()
                self._remote._collect_direct_refs(obj, deps)
                pending.extend(deps)
        return json.dumps({"schema": 1, "remote_root": self.remote_root.rstrip("/"),
                           "root": index.to, "objects": objects}, sort_keys=True, separators=(",", ":"))

    def _import_snapshot(self, snapshot: str, db: DmlDB) -> Ref:
        try:
            data = json.loads(snapshot)
            if (not isinstance(data, dict) or set(data) != {"schema", "remote_root", "root", "objects"}
                    or type(data["schema"]) is not int or data["schema"] != 1
                    or data["remote_root"] != self.remote_root.rstrip("/")):
                raise ValueError("Invalid snapshot schema or remote.root mismatch")
            root = Ref(data["root"])
            if root.ns() != "frozenindex" or not isinstance(data["objects"], dict):
                raise ValueError("Snapshot must contain a frozen runtime")
            objects = {}
            for key, payload in data["objects"].items():
                ref = Ref(key)
                if ref != root and ref.ns() in {"index", "frozenindex", "deletable"}:
                    raise ValueError("Snapshot contains an unrelated mutable object")
                objects[ref] = self._remote._load_cas_object(ref, payload)
            if root not in objects or not isinstance(objects[root], FrozenIndex):
                raise ValueError("Snapshot root is missing")
            reached: set[Ref] = set()
            pending = [root]
            while pending:
                ref = pending.pop()
                if ref in reached:
                    continue
                reached.add(ref)
                deps: set[Ref] = set()
                self._remote._collect_direct_refs(objects[ref], deps)
                pending.extend(deps - reached)
            if reached != set(objects):
                raise ValueError("Snapshot contains unreachable objects")
            current = self.exec_state().read_execution_record(root.id())["state"]
            checkpoint = objects[root].runtime_generation
            with db.tx(readonly=True) as txn:
                active = Ref(f"index:{root.id()}")
                recovering = (txn.exists(root) and txn.exists(active)
                              and current["lifecycle"] == "running"
                              and txn.get(active).runtime_generation == current.get("runtime_generation"))
            if not recovering and not (
                current["lifecycle"] == "suspended" and current.get("runtime_generation") == checkpoint
            ):
                raise DmlRepoError("Snapshot is stale or runtime is not suspended")

            def restore(txn):
                if txn.exists(Ref(f"index:{root.id()}")) and not txn.exists(root):
                    raise ValueError("Runtime is already active locally")
                for ref, obj in objects.items():
                    if txn.exists(ref):
                        if self._remote._dump_cas_object(txn.get(ref)) != self._remote._dump_cas_object(obj):
                            raise ValueError(f"Snapshot object conflicts with local state: {ref}")
                    elif ref == root:
                        txn.put(obj, to=ref)
                    elif txn.put(obj) != ref:
                        raise ValueError(f"Snapshot object identity mismatch: {ref}")

            db.write_with_growth(restore)
            return root
        except (KeyError, TypeError, ValueError) as exc:
            raise DmlRepoError(f"Invalid runtime snapshot: {exc}") from exc

    def unfreeze(self, index: Ref | None = None, *, snapshot: str | None = None, db: DmlDB) -> Ref:
        """Restore graph state, then claim a single activation in the original remote."""
        if (index is None) == (snapshot is None):
            raise DmlRepoError("Provide exactly one of index or snapshot")
        if snapshot is not None:
            index = self._import_snapshot(snapshot, db)
        assert index is not None
        active = Ref(f"index:{index.id()}")
        state = self.exec_state()

        def prepare_frozen(txn):
            frozen = txn.get(index)
            if not isinstance(frozen, FrozenIndex):
                raise DmlRepoError(f"Runtime is not a frozen index: {index}")
            # Older user checkpoints predate remote suspension generations.
            if frozen.runtime_generation is None:
                frozen.runtime_generation = uuid7().hex
                txn.put(frozen, to=index)
            return frozen

        frozen = db.write_with_growth(prepare_frozen)
        current = state.read_execution_record(index.id())["state"]
        if (current["lifecycle"] == "running"
                and current.get("runtime_generation") == frozen.previous_generation):
            state.suspend_runtime(index.id(), frozen.previous_generation, frozen.runtime_generation)
            current = state.read_execution_record(index.id())["state"]
        with db.tx(readonly=True) as txn:
            recovering = (txn.exists(active) and current["lifecycle"] == "running"
                          and txn.get(active).runtime_generation == current.get("runtime_generation"))
        if not recovering and not (
            current["lifecycle"] == "suspended" and current.get("runtime_generation") == frozen.runtime_generation
        ):
            raise DmlRepoError("Snapshot is stale or runtime is not suspended")

        def unfreeze_index(txn) -> Ref:
            frozen = txn.get(index)
            if not isinstance(frozen, FrozenIndex):
                raise DmlRepoError(f"Runtime is not a frozen index: {index}")
            if txn.exists(active):
                return active
            candidate = txn.put(
                Index(
                    parents=frozen.parents,
                    tree=frozen.tree,
                    author=frozen.author,
                    message=frozen.message,
                    created=frozen.created,
                    dag=frozen.dag,
                    runtime_generation=uuid7().hex,
                ),
                to=Ref(f"index:{index.id()}"),
            )
            return candidate

        # Stage the candidate generation before the remote CAS. A fresh process
        # can finish activation after a crash without issuing a second claim.
        db.write_with_growth(unfreeze_index)
        with db.tx(readonly=True) as txn:
            frozen = txn.get(index)
            generation = txn.get(active).runtime_generation
        current = state.read_execution_record(index.id())["state"]
        if not (current["lifecycle"] == "running" and current.get("runtime_generation") == generation):
            state.resume_runtime(index.id(), frozen.runtime_generation, generation)
        db.write_with_growth(lambda txn: txn.delete(index))
        return active

    def put_import(self, index: Ref, dag: Ref, node: Optional[Ref], name: Optional[str] = None, *, db: DmlDB) -> Ref:
        """Import a node from another DAG into the current index DAG."""
        self._require_mutation(index, db)

        def put_import(txn) -> Ref:
            ctx = txn.get_ctx(index)
            index_obj = cast(Index, ctx.commit)
            if index_obj.dag == dag:
                raise DmlRepoError("Cannot import from the current DAG")
            dag_obj: Dag = txn.get(dag)
            if not dag_obj.is_finished():
                raise DmlRepoError("Cannot import from an unfinished DAG")
            if node is None and dag_obj.error is not None:
                msg = f"Dag: {dag} is finished with error: {dag_obj.error}, cannot import result node."
                raise DmlRepoError(msg)
            node_ = cast(Ref, node or dag_obj.result)
            if node_ not in dag_obj.nodes:
                raise DmlRepoError("Node to import not found in source DAG")
            imp_node = txn.put(ImportNode(dag, node_))
            ctx.dag.nodes = sorted({*ctx.dag.nodes, imp_node})
            if name is not None:
                ctx.dag.names[name] = imp_node
            index_obj.dag = txn.put(ctx.dag)
            txn.put(index_obj, to=index)
            return imp_node

        return db.write_with_growth(put_import)

    def _run_builtin(self, argv_node_refs: list[Ref], dag, txn) -> Ref:
        argv_refs = []
        for node_ref in argv_node_refs:
            datum_ref, error_ref = txn.get(node_ref).datum_ref(txn)
            if error_ref is not None:
                raise txn.get(error_ref)
            assert datum_ref is not None
            argv_refs.append(datum_ref)
        runnable = txn.get(argv_refs[0]).value(txn)
        # TODO: Builtins should decide what to extract from the DB instead of
        # materializing the full argv here. For values[1:3], get only needs an
        # op, the collection datum refs, and the key; the collection can remain
        # a list of refs.
        args = [txn.get(x).unroll(txn) for x in argv_refs[1:]]
        fn_uri = runnable.target.uri
        fn_argv_node_ref = txn.put(ArgvNode(value=txn.put(ListDatum(argv_refs))))
        fndag = Dag(nodes=[fn_argv_node_ref], names={}, tags=[], argv=fn_argv_node_ref)
        try:
            bi_fn = BUILTIN_FNS.get(fn_uri.split(":", 1)[-1])
            if bi_fn is None:
                raise DmlRepoError(f"Unknown built-in function: {fn_uri}")
            resp = bi_fn(*args)
        except Exception as e:
            fndag.error = txn.put(Error.from_ex(e))
        else:
            fndag.result = self._put_literal(resp, fndag, txn)
        fnnode = txn.put(FnNode(argv=argv_node_refs, dag=txn.put(fndag)))
        dag.nodes = sorted({*dag.nodes, fnnode})
        return fnnode

    def _put_literal(self, value, dag, txn) -> Ref:
        def bi(fn, *xs) -> Ref:
            fn_uri = txn.put(UriDatum(f"daggerml:{fn}"))
            fn_kwargs = txn.put(DictDatum(data={}))
            runnable = Runnable(target=fn_uri, sub=None, kwargs=fn_kwargs, adapter="")
            ys = [self._put_literal(x, dag, txn) for x in [runnable, *xs]]
            return self._run_builtin(ys, dag, txn)

        def _put(x) -> Ref:
            if isinstance(x, Ref):
                if x.nss()[0] == "node":
                    if x not in dag.nodes:
                        raise DmlRepoError(f"Referenced node is not part of DAG: {x}")
                    return x
                if x.nss()[0] == "datum":
                    return x
                raise DmlRepoError(f"Invalid reference namespace for literal value: {x.ns()}")
            if isinstance(x, Runnable):
                target_ref = _put(x.target)
                sub_ref = _put(x.sub) if x.sub is not None else None
                kwargs_ref = _put(x.kwargs)
                if (
                    target_ref.nss()[0] == "node"
                    or (kwargs_ref.nss()[0] == "node")
                    or (sub_ref is not None and sub_ref.nss()[0] == "node")
                    or isinstance(x.adapter, Ref)
                ):
                    return bi("runnable", target_ref, sub_ref, kwargs_ref, x.adapter)
                return txn.put(RunnableDatum(target=target_ref, sub=sub_ref, kwargs=kwargs_ref, adapter=x.adapter))
            if isinstance(x, Uri):
                return txn.put(UriDatum(x.uri))
            if isinstance(x, list):
                ys = [_put(v) for v in x]
                if any(v.nss()[0] == "node" for v in ys):
                    return bi("list", *ys)
                return txn.put(ListDatum(ys))
            if isinstance(x, dict):
                ys = {k: _put(v) for k, v in x.items()}
                if any(x.nss()[0] == "node" for x in ys.values()):
                    return bi("dict", *unnest(zip(ys.keys(), ys.values(), strict=True)))
                return txn.put(DictDatum(ys))
            return txn.put(ScalarDatum(x))

        result_ref = _put(value)
        if result_ref.nss()[0] == "datum":
            result_ref = txn.put(LiteralNode(value=result_ref))
        dag.nodes = sorted({result_ref, *dag.nodes})
        return result_ref

    def put_literal(self, index: Ref, value: Any, name: Optional[str] = None, *, db: DmlDB) -> Ref:
        self._require_mutation(index, db)

        def put_literal(txn) -> Ref:
            ctx = txn.get_ctx(index)
            node_ref = self._put_literal(value, ctx.dag, txn)
            return self._update_dag(index, node_ref, name, ctx, txn)

        return db.write_with_growth(put_literal)

    def get_argv(self, index: Ref, *, db: DmlDB) -> Ref:
        """Return the argv node for an index (raises if missing)."""
        with db.tx(readonly=True) as txn:
            ctx = txn.get_ctx(index)
            if ctx.dag is None or ctx.dag.argv is None:
                raise DmlRepoError("Only function dags have argv nodes.")
            return ctx.dag.argv

    def get_node(self, index: Ref, name: str, *, db: DmlDB) -> Ref:
        """Return a named node from an index's DAG."""
        with db.tx(readonly=True) as txn:
            ctx = txn.get_ctx(index)
            if name not in ctx.dag.names:
                raise DmlRepoError(f"Node '{name}' not found in DAG")
            return ctx.dag.names[name]

    def set_node_name(self, index: Ref, name: str, node_ref: Ref, *, db: DmlDB) -> Ref:
        """Set or replace a node name in the index DAG."""
        self._require_mutation(index, db)

        def set_node_name(txn) -> Ref:
            ctx = txn.get_ctx(index)
            return self._update_dag(index, node_ref, name, ctx, txn)

        return db.write_with_growth(set_node_name)

    def update_tags(self, index: Ref, tag: str, *, add: bool, db: DmlDB) -> Ref:
        self._require_mutation(index, db)
        if not isinstance(tag, str):
            raise DmlRepoError("DAG tag must be a string")

        def update(txn) -> Ref:
            if not isinstance(txn.get(index), Index):
                raise DmlRepoError(f"Runtime is not an active index: {index}")
            ctx = txn.get_ctx(index)
            tags = set(ctx.dag.tags)
            if add:
                tags.add(tag)
            else:
                tags.discard(tag)
            ctx.dag.tags = sorted(tags)
            cast(Index, ctx.commit).dag = txn.put(ctx.dag)
            txn.put(ctx.commit, to=index)
            return cast(Index, ctx.commit).dag

        return db.write_with_growth(update)

    def start_fn(self, index: Ref, argv: list[Ref], name: Optional[str] = None, *, db: DmlDB) -> Optional[Ref]:
        self._require_mutation(index, db)

        def prepare(txn) -> tuple[Runnable, Ref, str] | Ref:
            ctx = txn.get_ctx(index)
            if not set(argv).issubset(set(ctx.dag.nodes)):
                raise DmlRepoError("All argv nodes must be part of current DAG.")
            # Keep the runnable read narrow so we do not unroll argv values early.
            runnable_ref, error_ref = txn.get(argv[0]).datum_ref(txn)
            if error_ref is not None:
                raise RuntimeError("Cannot start function with error in argv: " + str(txn.get(error_ref)))
            assert runnable_ref is not None
            runnable = txn.get(runnable_ref).value(txn)
            if not isinstance(runnable, Runnable):
                raise DmlRepoError("First argv node must resolve to a Runnable datum.")
            if runnable.adapter == "":
                resp = self._run_builtin(argv, ctx.dag, txn)
                return self._update_dag(index, resp, name, ctx, txn)
            argv_refs = []
            for node_ref in argv:
                datum_ref, error_ref = txn.get(node_ref).datum_ref(txn)
                if error_ref is not None:
                    raise RuntimeError("Cannot start function with error in argv: " + str(txn.get(error_ref)))
                assert datum_ref is not None
                argv_refs.append(datum_ref)
            argv_node = ArgvNode(value=txn.put(ListDatum(argv_refs)))
            cache_key = argv_node.cache_key(txn)
            argv_node_ref = txn.put(argv_node)
            return runnable, argv_node_ref, cache_key

        prepared = db.write_with_growth(prepare)
        if isinstance(prepared, Ref):
            return prepared
        runnable, argv_node_ref, cache_key = prepared
        state = self.exec_state(cache_key=cache_key)
        resp = state.get_or_start_fn(index, runnable, argv_node_ref, db)
        if resp is None:
            # execution is still running, caller should poll for completion and then import the result node when done.
            return None
        self._require_mutation(index, db)

        def attach_result(txn) -> Ref:
            # add FnNode pointing to this dag
            hydrated = txn.get(resp)
            if not hydrated.is_finished():
                raise DmlRepoError(f"Cached DAG {cache_key} is not finished.")
            ctx = txn.get_ctx(index)
            node = txn.put(FnNode(argv=argv, dag=resp))
            ctx.dag.nodes = sorted({*ctx.dag.nodes, node})
            self._update_dag(index, node, name, ctx, txn)
            return node

        return db.write_with_growth(attach_result)

    def commit(
        self,
        index: Ref,
        value: Ref | Error,
        author: str,
        message: str | None = None,
        name: str | None = None,
        *,
        db: DmlDB,
    ) -> tuple[Ref, Ref | None]:
        self._require_mutation(index, db)
        created = now()

        def commit_index(txn) -> tuple[Ref, Ref | None, str | None]:
            ctx = txn.get_ctx(index)
            index_obj = cast(Index, ctx.commit)
            if ctx.dag is None:
                raise DmlRepoError("Index commit has no DAG.")
            if isinstance(value, Error):
                ctx.dag.error = txn.put(value)
            else:
                if value not in ctx.dag.nodes:
                    raise DmlRepoError("Value node is not part of DAG.")
                ctx.dag.result = value
            dag_ref = txn.put(ctx.dag)
            commit_ref = None
            if name is not None:
                ctx.tree.dags[name] = dag_ref
                commit_ref = txn.put(
                    Commit(
                        parents=list(index_obj.parents),
                        tree=txn.put(ctx.tree),
                        author=author,
                        message=message or "",
                        created=created,
                    )
                )
            return dag_ref, commit_ref, index_obj.runtime_generation

        dag_ref, commit_ref, generation = db.write_with_growth(commit_index)
        self.exec_state().finish_execution(index.id(), dag_ref, db, generation=generation)

        def delete_index(txn) -> None:
            txn.delete(index)

        db.write_with_growth(delete_index)
        return dag_ref, commit_ref
