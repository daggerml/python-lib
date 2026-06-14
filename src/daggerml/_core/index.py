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

import logging
import time
from dataclasses import InitVar, dataclass, field
from typing import TYPE_CHECKING, Any, Optional, cast

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
        *,
        db: DmlDB,
    ) -> Ref:
        if (cache_key is None) != (execution_id is None):
            raise DmlRepoError("Must provide either both (execution_id and cache_key) or neither.")
        # FIXME: Consider using `call_w_resize` for this write transaction.
        with db.tx() as txn:
            argv = None
            nodes: list[Ref] = []
            parents: list[Ref] = []
            if commit is None:
                base_tree = txn.put(Tree(dags={}))
            else:
                base_commit: Commit = txn.get(commit)
                base_tree = base_commit.tree
                parents = [commit]
            if execution_id is not None:
                argv_manifest = self._remote.get_active(cast(str, cache_key), raw=True)
                argv = self._remote._materialize_manifest(
                    cast(dict, argv_manifest),
                    txn,
                    expected_root_ns="node-argv",
                )
                nodes.append(argv)
            # Create initial execution record.
            state = self.exec_state(cache_key=cache_key)
            now_ts = int(time.time())
            exec_id = execution_id or uuid7().hex
            state.create_execution_record(
                {
                    "execution_id": exec_id,
                    "cache_key": cache_key,
                    "lifecycle": "running",
                    "updated_at": now_ts,
                    "created_at": now_ts,
                    "spawned_execution_ids": [],
                    "child_execution_ids": [],
                    "cancellation_requested_by": None,
                }
            )
            # create db state
            dag_ref = txn.put(Dag(nodes=nodes, names={}, argv=argv))
            index = txn.put(
                Index(
                    parents=parents,
                    tree=base_tree,
                    author=author,
                    message="",
                    dag=dag_ref,
                ),
                to=Ref(f"index:{exec_id}"),
            )
        return index

    def put_import(self, index: Ref, dag: Ref, node: Optional[Ref], name: Optional[str] = None, *, db: DmlDB) -> Ref:
        """Import a node from another DAG into the current index DAG."""
        with db.tx() as txn:
            ctx = txn.get_ctx(index)
            index_obj = cast(Index, ctx.commit)
            if index_obj.dag == dag:
                raise DmlRepoError("Cannot import from the current DAG")
            dag_obj: Dag = txn.get(dag)
            if not dag_obj.is_finished():
                raise DmlRepoError("Cannot import from an unfinished DAG")
            if node is None and dag_obj.error is not None:
                raise DmlRepoError(f"Dag: {dag} is finished with error: {dag_obj.error}, cannot import result node.")
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

    def _run_builtin(self, argv_node_refs: list[Ref], dag, txn) -> Ref:
        argv_refs = [txn.get(x).datum_ref(txn) for x in argv_node_refs]
        runnable = txn.get(argv_refs[0]).value(txn)
        # TODO: Builtins should decide what to extract from the DB instead of
        # materializing the full argv here. For values[1:3], get only needs an
        # op, the collection datum refs, and the key; the collection can remain
        # a list of refs.
        args = [txn.get(x).unroll(txn) for x in argv_refs[1:]]
        fn_uri = runnable.target.uri
        fn_argv_node_ref = txn.put(ArgvNode(value=txn.put(ListDatum(argv_refs))))
        fndag = Dag(nodes=[fn_argv_node_ref], names={}, argv=fn_argv_node_ref)
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
        # FIXME: Consider using `call_w_resize` for this write transaction.
        with db.tx() as txn:
            ctx = txn.get_ctx(index)
            node_ref = self._put_literal(value, ctx.dag, txn)
            return self._update_dag(index, node_ref, name, ctx, txn)

    def get_argv(self, index: Ref, *, db: DmlDB) -> Ref:
        """Return the argv node for an index (raises if missing)."""
        with db.tx() as txn:
            ctx = txn.get_ctx(index)
            if ctx.dag is None or ctx.dag.argv is None:
                raise DmlRepoError("Only function dags have argv nodes.")
            return ctx.dag.argv

    def get_node(self, index: Ref, name: str, *, db: DmlDB) -> Ref:
        """Return a named node from an index's DAG."""
        with db.tx() as txn:
            ctx = txn.get_ctx(index)
            if name not in ctx.dag.names:
                raise DmlRepoError(f"Node '{name}' not found in DAG")
            return ctx.dag.names[name]

    def set_node_name(self, index: Ref, name: str, node_ref: Ref, *, db: DmlDB) -> Ref:
        """Set or replace a node name in the index DAG."""
        with db.tx() as txn:
            ctx = txn.get_ctx(index)
            return self._update_dag(index, node_ref, name, ctx, txn)

    def start_fn(self, index: Ref, argv: list[Ref], name: Optional[str] = None, *, db: DmlDB) -> Optional[Ref]:
        # FIXME: Consider using `call_w_resize` for this write transaction.
        with db.tx() as txn:
            ctx = txn.get_ctx(index)
            if not set(argv).issubset(set(ctx.dag.nodes)):
                raise DmlRepoError("All argv nodes must be part of current DAG.")
            runnable = txn.get(txn.get(argv[0]).datum_ref(txn)).value(txn)  # prevent premature unroll if not runnable
            if not isinstance(runnable, Runnable):
                raise DmlRepoError("First argv node must resolve to a Runnable datum.")
            if runnable.adapter == "":
                resp = self._run_builtin(argv, ctx.dag, txn)
                return self._update_dag(index, resp, name, ctx, txn)
            # adapter-backed function call.
            # Step 0: construct argv node and compute cache key
            argv_node = ArgvNode(value=txn.put(ListDatum([txn.get(x).datum_ref(txn) for x in argv])))
            cache_key = argv_node.cache_key(txn)
            argv_node_ref = txn.put(argv_node)
        state = self.exec_state(cache_key=cache_key)
        resp = state.get_or_start_fn(index, runnable, argv_node_ref, db)
        if resp is None:
            # execution is still running, caller should poll for completion and then import the result node when done.
            return None
        # FIXME: Consider using `call_w_resize` for this write transaction.
        with db.tx() as txn:
            # add FnNode pointing to this dag
            hydrated = txn.get(resp)
            if not hydrated.is_finished():
                raise DmlRepoError(f"Cached DAG {cache_key} is not finished.")
            ctx = txn.get_ctx(index)
            node = txn.put(FnNode(argv=argv, dag=resp))
            ctx.dag.nodes = sorted({*ctx.dag.nodes, node})
            self._update_dag(index, node, name, ctx, txn)
        if hydrated.error is not None:
            with db.tx() as txn:
                err = txn.get(hydrated.error)
                raise err
        return node

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
        # FIXME: Consider using `call_w_resize` for this write transaction.
        with db.tx() as txn:
            ctx = txn.get_ctx(index)
            if ctx.dag is None:
                raise DmlRepoError("Index commit has no DAG.")
            index_obj = cast(Index, ctx.commit)
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
                        created=now(),
                    )
                )
            txn.delete(index)
        self.exec_state().finish_execution(index.id(), dag_ref, db)
        return dag_ref, commit_ref
