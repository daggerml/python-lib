"""Index operations for managing working state and function execution.

Public API:
    IndexOps - Class for index and execution operations
"""

from __future__ import annotations

import json
import shutil
import sys
from dataclasses import dataclass
from subprocess import run
from typing import Any, Iterator, Mapping, Optional, cast
from urllib.parse import urlparse
from uuid import uuid4

from daggerml._internal._db import Ref
from daggerml._internal.builtins import BUILTIN_FNS
from daggerml._internal.codec import CodecContext, apply_codec
from daggerml._internal.exec_state import ExecutionRecord, ExecutionState
from daggerml._internal.ops.base_ops import BaseOps, with_retry
from daggerml._internal.ops.cache import CacheOps
from daggerml._internal.ops.dag import DagOps
from daggerml._internal.ops.node import NodeOps
from daggerml._internal.types import (
    ArgvNode,
    Commit,
    Dag,
    Datum,
    DictDatum,
    DmlRepoError,
    Error,
    FnNode,
    ImportNode,
    Index,
    KwargvNode,
    ListDatum,
    LiteralNode,
    Node,
    Runnable,
    RunnableDatum,
    ScalarDatum,
    Tree,
    Uri,
    require_ref,
)
from daggerml._internal.util import now, unnest


def _random_ref(ns: str) -> Ref:
    return Ref(f"{ns}:{uuid4().hex}")


@dataclass(frozen=True)
class _PreparedAdapterCall:
    argv_ref: Ref
    adapter_path: str
    cache_key: str
    runnable: dict[str, Any]
    adapter_cmd: tuple[str, ...] | None = None
    caller_index_id: str | None = None
    caller_cache_key: str | None = None


@dataclass
class IndexOps(BaseOps):
    remote_root: str

    def _remote_ops(self):
        if not self.remote_root:
            raise DmlRepoError("Remote context required for argv_ptr")
        from daggerml._internal.ops.remote import RemoteOps

        parsed = urlparse(self.remote_root)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise DmlRepoError(f"Invalid remote root URI: {self.remote_root!r}")
        prefix = parsed.path.strip("/")
        return RemoteOps(_db=self._db, bucket=parsed.netloc, prefix=f"{prefix}/dml" if prefix else "dml")

    @staticmethod
    def _kwargv_ref_from_nodes(dag: Dag, txn) -> Ref | None:
        matches = []
        for node_ref in dag.nodes:
            node = txn.get(node_ref)
            if isinstance(node, KwargvNode):
                matches.append(node_ref)
        if len(matches) > 1:
            raise DmlRepoError("DAG has multiple kwargv nodes")
        return matches[0] if matches else None

    def list(self) -> Iterator[Ref]:
        """Get all index objects in db.

        Yields
        ------
        Ref
            Index object references.
        """
        with self._tx(readonly=True) as txn:
            for index_ref in txn.iter("index"):
                yield index_ref

    def start_fn(
        self,
        index_ref: Ref,
        argv: list[Ref],
        kwargv: Optional[dict[str, Ref]] = None,
        name: Optional[str] = None,
    ) -> Optional[Ref]:
        self._validate_index_ref(index_ref)
        codec_ctx = self._make_codec_ctx(index_ref)
        argv = [self._normalize_codec_value(arg, ctx=codec_ctx) for arg in argv]
        kwargv = kwargv or {}
        kwargv = {k: self._normalize_codec_value(v, ctx=codec_ctx) for k, v in kwargv.items()}
        with self._tx(readonly=False) as txn:
            argv_ref = self._prepare_fn(index_ref, argv, kwargv, txn)
            dag_ref = self._run_builtin(argv_ref, txn)
            if dag_ref is not None:
                return self._finish_fn_result(dag_ref, argv, name, txn, index_ref)
            cops = CacheOps(_db=self._db, remote_root=self.remote_root)
            dag_ref = cops._get(argv_ref, txn)
            if dag_ref is not None:
                return self._finish_fn_result(dag_ref, argv, name, txn, index_ref)
            prepared = self._prepare_adapter_call(index_ref, argv_ref, txn)
        argv_ptr = self._remote_ops().put_ref_manifest(prepared.argv_ref)
        es = ExecutionState(prepared.cache_key, remote_root=self.remote_root)

        # Step 1: try to acquire the mutex
        if not es.lock():
            # Another process is driving this cycle
            return None

        # Step 2: post-lock cache check
        with self._tx(readonly=False) as txn:
            cops = CacheOps(_db=self._db, remote_root=self.remote_root)
            dag_ref = cops._get(prepared.argv_ref, txn)
            if dag_ref is not None:
                es.unlock()
                return self._finish_fn_result(dag_ref, argv, name, txn, index_ref)

        execution_number = es.read_active_execution_number()
        execution_record = None
        if execution_number is not None:
            # get execution record
            execution_record = es.read_execution_record(execution_number)
            # start over if record is missing
            if execution_record is None:
                es.delete_active_execution()
                execution_number = None
        # submit new or check existing?
        if execution_number is None:
            assert execution_record is None
            self._record_call_edges(prepared, es)
            execution_number = es.next_execution_number()
            execution_id = f"{prepared.cache_key}-{execution_number}"
            state = None
        else:
            assert execution_record is not None
            execution_id = cast(str, execution_record["execution_id"])
            state = cast(dict[str, Any], execution_record["state"])

        # Step 3: call adapter (holding the lock)
        try:
            result = self._call_adapter(prepared, argv_ptr, execution_id=execution_id, state=state)
        except Exception:
            es.unlock()
            raise

        # Step 4: handle adapter result
        status = result["status"]
        if status == "succeeded":
            self._publish_terminal_state(prepared.argv_ref, result)
            es.delete_active_execution()
            es.unlock()
            with self._tx(readonly=False) as txn:
                cops = CacheOps(_db=self._db, remote_root=self.remote_root)
                dag_ref = cops._get(prepared.argv_ref, txn)
                if dag_ref is not None:
                    return self._finish_fn_result(dag_ref, argv, name, txn, index_ref)
            raise DmlRepoError("Adapter reported success but no cached DAG was published")
        elif status == "failed":
            self._publish_terminal_state(prepared.argv_ref, result)
            es.delete_active_execution()
            es.unlock()
            with self._tx(readonly=False) as txn:
                cops = CacheOps(_db=self._db, remote_root=self.remote_root)
                dag_ref = cops._get(prepared.argv_ref, txn)
                if dag_ref is not None:
                    return self._finish_fn_result(dag_ref, argv, name, txn, index_ref)
            raise DmlRepoError("Adapter reported failure but no cached failed DAG was published")
        else:
            if state is None:
                record: ExecutionRecord = {
                    "execution_number": execution_number,
                    "execution_id": execution_id,
                    "cache_key": prepared.cache_key,
                    "status": "running",
                    "state": result["state"],
                }
                if not es.create_execution_record(execution_number, record):
                    es.unlock()
                    raise DmlRepoError(f"Execution record already exists: {prepared.cache_key}/{execution_number}")
                if not es.create_active_execution(execution_number):
                    es.unlock()
                    raise DmlRepoError(f"Active execution already exists for cache key: {prepared.cache_key}")
            # running — adapter is still working asynchronously
            es.unlock()
            return None

    def delete(self, index_ref: Ref) -> None:
        """Delete an index object from db."""
        with self._tx(readonly=False) as txn:
            txn.delete(self._validate_index_ref(index_ref))

    @with_retry
    def create(
        self,
        head: Optional[Ref] = None,
        argv_ptr: Optional[str] = None,
    ) -> Ref:
        """Create a new index object.

        Parameters
        ----------
        head : Ref, optional
            Reference to the head to base the index on. If None, uses the active head.
        argv_ptr : str, optional
            Optional remote manifest OID to initialize argv from.

        Returns
        -------
        Ref
            Reference to the newly created index object.
        """
        if head is not None and head.ns() != "head":
            raise DmlRepoError(f"Expected head ref, got: {head}")
        modes = [head is not None, argv_ptr is not None]
        if sum(modes) != 1:
            raise DmlRepoError("Provide exactly one of head or argv_ptr.")
        kw = {}
        if argv_ptr is not None:
            kw["argv"] = self._remote_ops().load_ptr(argv_ptr, expected_root_ns="node-argv")
        with self._tx(readonly=False) as txn:
            index = self._create(head=head, **kw, txn=txn)
        return index

    @with_retry
    def get_kwargv(self, index_ref: Ref) -> Ref:
        """Return the kwargv node for an index (raises if missing)."""
        with self._tx(readonly=True) as txn:
            ctx = txn.get_ctx(self._validate_index_ref(index_ref))
        return DagOps(_db=self._db).get_kwargv(cast(Ref, ctx.commit.dag))

    @with_retry
    def get_argv(self, index_ref: Ref) -> Ref:
        """Return the argv node for an index (raises if missing)."""
        with self._tx(readonly=True) as txn:
            ctx = txn.get_ctx(self._validate_index_ref(index_ref))
        return DagOps(_db=self._db).get_argv(cast(Ref, ctx.commit.dag))

    @with_retry
    def get_node(self, index_ref: Ref, name: str) -> Ref:
        """Return a named node from an index's DAG."""
        with self._tx(readonly=True) as txn:
            ctx = txn.get_ctx(self._validate_index_ref(index_ref))
            # Access the dag's names directly to avoid nested transactions
            dag: Dag = txn.get(cast(Ref, ctx.commit.dag))
            if name not in dag.names:
                raise DmlRepoError(f"Node '{name}' not found in DAG")
            return dag.names[name]

    @with_retry
    def describe(self, index_ref: Ref) -> dict[str, Any]:
        """Describe the current index state."""
        with self._tx(readonly=True) as txn:
            ctx = txn.get_ctx(self._validate_index_ref(index_ref))
            dag_ref = ctx.commit.dag
            return {
                "id": index_ref.id(),
                "head": index_ref,
                "commit": ctx.head.commit,
                "dag": dag_ref,
                "nodes": list(ctx.dag.nodes) if ctx.dag is not None else [],
                "names": dict(ctx.dag.names) if ctx.dag is not None else {},
                "result": (ctx.dag.result if ctx.dag is not None else None),
                "argv": (ctx.dag.argv if ctx.dag is not None else None),
                "kwargv": (self._kwargv_ref_from_nodes(ctx.dag, txn) if ctx.dag is not None else None),
            }

    @with_retry
    def set_node_name(self, index_ref: Ref, name: str, node_ref: Ref) -> Ref:
        """Set or replace a node name in the index DAG."""
        self._validate_index_ref(index_ref)
        require_ref(node_ref, ["node"], "set_node_name node_ref")
        with self._tx(readonly=False) as txn:
            ctx = txn.get_ctx(index_ref)
            if ctx.dag is None:
                raise DmlRepoError("Index commit has no DAG.")
            if node_ref not in ctx.dag.nodes:
                raise DmlRepoError("Node is not part of current DAG.")
            ctx.dag.names[name] = node_ref
            ctx.commit.dag = txn.put(ctx.dag)
            ctx.commit.modified = now()
            ctx.head.commit = txn.put(ctx.commit)
            txn.put(ctx.head, to=index_ref)
            return node_ref

    @with_retry
    def put_import(self, index_ref: Ref, dag: Ref, node: Optional[Ref] = None, name: Optional[str] = None) -> Ref:
        """Import a node from another DAG into the current index DAG."""
        self._validate_index_ref(index_ref)
        with self._tx(readonly=False) as txn:
            ctx = txn.get_ctx(index_ref)
            dag_obj: Dag = txn.get(dag)
            # determine node (prefer explicit `node` arg)
            imported_node = node if node is not None else dag_obj.result
            if imported_node is None:
                raise DmlRepoError("Cannot import from a DAG with no result node")
            if dag == ctx.commit.dag:
                # importing from current dag is not allowed
                raise DmlRepoError("Cannot import from the current DAG")
            node_obj = ImportNode(dag, imported_node)
            return self._put_node(node_obj, name=name, txn=txn, index_ref=index_ref)

    @with_retry
    def put_literal(self, index_ref: Ref, value: Any, name: Optional[str] = None) -> Ref:
        self._validate_index_ref(index_ref)
        codec_ctx = self._make_codec_ctx(index_ref)
        value = self._normalize_codec_value(value, ctx=codec_ctx)
        with self._tx(readonly=False) as txn:
            return self._put_literal(value, name=name, txn=txn, index_ref=index_ref)

    @with_retry
    def commit(
        self,
        index_ref: Ref,
        value: Ref | Error,
        head: Optional[Ref] = None,
        message: Optional[str] = None,
        dag_name: Optional[str] = None,
    ) -> Ref:
        """Commit the current index state with the given value as the result node.

        Returns
        -------
        Ref
            Reference to the newly created commit.

        Raises
        ------
        DmlRepoError
            If the commit operation fails.
        """
        self._validate_index_ref(index_ref)
        with self._tx(readonly=False) as txn:
            ctx = txn.get_ctx(index_ref)
            if ctx.dag is None:
                raise DmlRepoError("Index commit has no DAG.")
            if isinstance(value, Error):
                ctx.dag.error = txn.put(value)
            else:
                if value not in ctx.dag.nodes:
                    raise DmlRepoError("Value node is not part of DAG.")
                ctx.dag.result = value
            ctx.commit.dag = txn.put(ctx.dag)
            if dag_name is not None:
                ctx.tree.dags[dag_name] = ctx.commit.dag
                ctx.commit.tree = txn.put(ctx.tree)
            if message is not None:
                ctx.commit.message = message
            ctx.commit.modified = now()
            commit_ref = txn.put(ctx.commit)
            if head is not None:
                head_ctx = txn.get_ctx(head)
                head_ctx.head.commit = commit_ref
                txn.put(head_ctx.head, to=head)
            txn.delete(index_ref)
        if ctx.dag.argv is not None:
            # automatically cache the DAG if it has an argv (i.e. is runnable)
            cops = CacheOps(_db=self._db, remote_root=self.remote_root)
            cops.put(ctx.commit.dag)
        return commit_ref

    def _validate_index_ref(self, index_ref: Ref) -> Ref:
        if index_ref is None:
            raise DmlRepoError("No active index reference.")
        if index_ref.ns() != "index":
            raise DmlRepoError(f"Expected index ref, got: {index_ref}")
        return index_ref

    def current_dag_ref(self, index_ref: Ref) -> Ref:
        self._validate_index_ref(index_ref)
        with self._tx(readonly=True) as txn:
            return cast(Ref, txn.get_ctx(index_ref).commit.dag)

    def resolve_dag_node(self, index_ref: Ref, dag_name: str, node_name: Optional[str] = None) -> tuple[Ref, Ref]:
        self._validate_index_ref(index_ref)
        with self._tx(readonly=True) as txn:
            ctx = txn.get_ctx(index_ref)
            tree: Tree = txn.get(ctx.commit.tree)
            dag_ref = tree.dags.get(dag_name)
            if dag_ref is None:
                raise DmlRepoError(f"DAG '{dag_name}' not found")
            dag: Dag = txn.get(dag_ref)
            if node_name is None:
                if dag.result is None:
                    raise DmlRepoError(f"DAG '{dag_name}' has no result node")
                return dag_ref, dag.result
            node_ref = dag.names.get(node_name)
            if node_ref is None:
                raise DmlRepoError(f"Node '{node_name}' not found in DAG '{dag_name}'")
            return dag_ref, node_ref

    def _resolve_node_value_ref(self, node_ref: Ref, txn) -> Ref:
        # Validate node ref using NodeOps then return its underlying datum ref
        node_ref = NodeOps(_db=self._db)._require_node_ref(node_ref)
        node = txn.get(node_ref)
        return node.datum_ref(txn)

    def _make_codec_ctx(self, index_ref: Ref) -> CodecContext:
        return CodecContext(
            index_ref=index_ref,
            index_ops=self,
        )

    def _normalize_codec_value(self, value: Any, *, ctx: CodecContext) -> Any:
        value = apply_codec(value, ctx=ctx)
        if isinstance(value, list):
            return [self._normalize_codec_value(v, ctx=ctx) for v in value]
        if isinstance(value, tuple):
            return tuple(self._normalize_codec_value(v, ctx=ctx) for v in value)
        if isinstance(value, dict):
            return {k: self._normalize_codec_value(v, ctx=ctx) for k, v in value.items()}
        if isinstance(value, Runnable):
            target = self._normalize_codec_value(value.target, ctx=ctx)
            sub = self._normalize_codec_value(value.sub, ctx=ctx) if value.sub is not None else None
            kwargs = {k: self._normalize_codec_value(v, ctx=ctx) for k, v in value.kwargs.items()}
            return Runnable(target=target, adapter=value.adapter, kwargs=kwargs, sub=sub)
        return value

    def _put_node(self, node: Node, txn, index_ref: Ref, name: Optional[str] = None) -> Ref:
        self._validate_index_ref(index_ref)
        ctx = txn.get_ctx(index_ref)
        if ctx.dag is None:
            raise DmlRepoError("Index commit has no DAG.")
        node_ref = txn.put(node)
        ctx.dag.nodes = sorted({node_ref, *ctx.dag.nodes})
        if name is not None:
            ctx.dag.names[name] = node_ref
        ctx.commit.dag = txn.put(ctx.dag)
        ctx.commit.modified = now()
        ctx.head.commit = txn.put(ctx.commit)
        txn.put(ctx.head, to=index_ref)
        return node_ref

    def _create(
        self,
        *,
        head: Optional[Ref] = None,
        author: Optional[str] = None,
        argv: Optional[Ref] = None,  # -> ArgvNode
        txn,
    ) -> Ref:
        nodes: list[Ref] = []
        kw: dict[str, Any] = {"author": author or "DaggerML User"}
        if head is not None:
            if head.ns() != "head":
                raise DmlRepoError(f"Expected head ref, got: {head}")
            if argv is not None:
                raise DmlRepoError("Cannot provide both head and argv.")
            base_ctx = txn.get_ctx(head)
            kw.update({"parents": [base_ctx.head.commit], "tree": base_ctx.commit.tree})
        elif argv is not None:
            argv_obj: ArgvNode = txn.get(argv)
            if not isinstance(argv_obj, ArgvNode):
                raise DmlRepoError("Argv node required")
            nodes.append(argv)
            nodes.append(self._kwargv_from_argv(argv, txn))
            kw.update({"parents": [], "tree": txn.put(Tree(dags={}))})
        else:
            raise DmlRepoError("Either head or argv must be provided.")
        dag_ref = txn.put(Dag(nodes=nodes, names={}, result=None, argv=argv))
        commit_ref = txn.put(Commit(message="", dag=dag_ref, **kw))
        idx = txn.put(Index(commit=commit_ref), to=_random_ref("index"))
        return idx

    # ~~~~~~~~~~~ START_FN ~~~~~~~~~~~
    def _runnable_chain(self, runnable_ref: Ref, txn) -> list[tuple[Ref, RunnableDatum]]:
        chain: list[tuple[Ref, RunnableDatum]] = []
        seen: set[Ref] = set()
        current = runnable_ref
        while True:
            if current in seen:
                raise DmlRepoError("Runnable sub cycle detected")
            seen.add(current)
            runnable: RunnableDatum = txn.get(current)
            if not isinstance(runnable, RunnableDatum):
                raise DmlRepoError("First arg must resolve to a Runnable datum")
            chain.append((current, runnable))
            if runnable.sub is None:
                break
            current = runnable.sub
        return chain

    def _innermost_runnable(self, runnable_ref: Ref, txn) -> RunnableDatum:
        return self._runnable_chain(runnable_ref, txn)[-1][1]

    def _kwargv_from_argv(self, argv_ref: Ref, txn) -> Ref:
        argv_node: ArgvNode = txn.get(argv_ref)
        argv_datum: ListDatum = txn.get(argv_node.value)
        if len(argv_datum.data) == 0:
            raise DmlRepoError("argv is empty")
        runnable_ref = argv_datum.data[0]
        if runnable_ref.ns() != "datum-runnable":
            raise DmlRepoError("First arg must resolve to a Runnable datum")
        runnable = self._innermost_runnable(runnable_ref, txn)
        return txn.put(KwargvNode(value=runnable.kwargs))

    def _resolve_runnable_kwargs(self, runnable_ref: Ref, kwargv: dict[str, Ref], index_ref: Ref, txn) -> Ref:
        ctx = txn.get_ctx(index_ref)
        if ctx.dag is None:
            raise DmlRepoError("Index commit has no DAG.")
        chain = self._runnable_chain(runnable_ref, txn)
        resolved: dict[Ref, dict[str, Ref]] = {}
        for ref, runnable in chain:
            kwargs_datum: DictDatum = txn.get(runnable.kwargs)
            resolved[ref] = dict(kwargs_datum.data)

        for key, value in kwargv.items():
            require_ref(value, ["node"], "start_fn kwargv values")
            if value not in ctx.dag.nodes:
                raise DmlRepoError("kwargv nodes must be part of the current DAG.")
            value_ref = self._resolve_node_value_ref(value, txn)
            assigned = False
            for ref, _runnable in reversed(chain):
                if key in resolved[ref]:
                    resolved[ref][key] = value_ref
                    assigned = True
                    break
            if not assigned:
                raise DmlRepoError(f"Unknown kwarg: {key}")

        sub_ref: Optional[Ref] = None
        for ref, runnable in reversed(chain):
            kwargs_ref = txn.put(DictDatum(data=resolved[ref]))
            sub_ref = txn.put(
                RunnableDatum(
                    target=runnable.target,
                    sub=sub_ref,
                    kwargs=kwargs_ref,
                    adapter=runnable.adapter,
                )
            )
        assert sub_ref is not None
        return sub_ref

    def _prepare_fn(
        self,
        index_ref: Ref,
        argv: list[Ref],
        kwargv: dict[str, Ref],
        txn,
    ) -> Ref:
        if len(argv) == 0:
            raise DmlRepoError("argv is empty")
        [require_ref(arg, ["node"], "start_fn argv elements") for arg in argv]
        ctx = txn.get_ctx(index_ref)
        if ctx.dag is None:
            raise DmlRepoError("Index commit has no DAG.")
        if not set(argv).issubset(set(ctx.dag.nodes)):
            raise DmlRepoError("All argv nodes must be part of current DAG.")
        fn_datum_ref = self._resolve_node_value_ref(argv[0], txn)
        if fn_datum_ref.nss()[0] != "datum":
            raise DmlRepoError("First arg must resolve to a Datum.")
        fn_datum: Datum = txn.get(fn_datum_ref)
        if not isinstance(fn_datum, RunnableDatum):
            raise DmlRepoError("First arg must resolve to a Runnable datum")
        runnable_ref = self._resolve_runnable_kwargs(fn_datum_ref, kwargv, index_ref, txn)
        argv_ref = txn.put(ListDatum([runnable_ref, *[self._resolve_node_value_ref(arg, txn) for arg in argv[1:]]]))
        return txn.put(ArgvNode(value=argv_ref))

    def _run_builtin(self, argv_ref: Ref, txn) -> Optional[Ref]:
        argv_node: ArgvNode = txn.get(argv_ref)
        argv_datum: ListDatum = txn.get(argv_node.datum_ref(txn))
        if len(argv_datum.data) == 0:
            raise DmlRepoError("argv is empty")
        fn_runnable_ref = argv_datum.data[0]
        if fn_runnable_ref.ns() != "datum-runnable":
            raise DmlRepoError("First arg must resolve to a Runnable datum")
        fn_runnable = self._innermost_runnable(fn_runnable_ref, txn)
        if fn_runnable.adapter != "":
            return None
        fn_uri_obj: Uri = txn.get(fn_runnable.target)
        if not isinstance(fn_uri_obj, Uri):
            raise DmlRepoError("Runnable target must resolve to a Uri datum.")
        fn_uri = fn_uri_obj.uri
        fn_parsed = urlparse(fn_uri)
        if fn_parsed.scheme != "daggerml":
            raise DmlRepoError(f"Invalid builtin URI scheme: {fn_parsed.scheme}")
        fpath = fn_parsed.path.lstrip("/")
        if fpath not in BUILTIN_FNS:
            raise DmlRepoError(f"Unknown builtin: {fn_parsed} -- path: {fpath}")
        kwargv_datum: DictDatum = txn.get(fn_runnable.kwargs)
        if kwargv_datum.data != {}:
            raise DmlRepoError("Keyword arguments are not supported for builtin functions.")
        node_ops = NodeOps(_db=self._db)
        args = [node_ops._unroll_datum_ref(arg, txn) for arg in argv_datum.data[1:]]
        result = BUILTIN_FNS[fpath](*args)
        # Create a new index for the function DAG within the current txn
        fn_index_ref = self._create(argv=argv_ref, txn=txn)
        # Insert the result node into the newly created index using the same txn
        result_node_ref = self._put_literal(result, name=None, txn=txn, index_ref=fn_index_ref)
        # Finalize the commit for the function index within the same txn (avoid opening new txns)
        idx_ctx = txn.get_ctx(fn_index_ref)
        if idx_ctx.dag is None:
            raise DmlRepoError("Function index has no DAG.")
        idx_ctx.dag.result = result_node_ref
        idx_ctx.commit.dag = txn.put(idx_ctx.dag)
        idx_ctx.commit.modified = now()
        commit_ref = txn.put(idx_ctx.commit)
        commit_obj: Commit = txn.get(commit_ref)
        if commit_obj.dag is None:
            raise DmlRepoError("Function commit has no DAG.")
        # clean up the temporary index object to avoid unbounded DB growth
        txn.delete(fn_index_ref)
        return commit_obj.dag

    def _runnable_envelope(self, runnable_ref: Ref, txn, node_ops: NodeOps) -> dict[str, Any]:
        runnable: RunnableDatum = txn.get(runnable_ref)
        target: Uri = txn.get(runnable.target)
        if not isinstance(target, Uri):
            raise DmlRepoError("Runnable target must resolve to a Uri datum.")
        kwargs_datum: DictDatum = txn.get(runnable.kwargs)
        sub = None
        if runnable.sub is not None:
            sub = self._runnable_envelope(runnable.sub, txn, node_ops)
        return {
            "target": target.uri,
            "kwargs": {k: node_ops._unroll_datum_ref(v, txn) for k, v in kwargs_datum.data.items()},
            "adapter": runnable.adapter,
            "sub": sub,
        }

    @staticmethod
    def _validate_adapter_output(payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise DmlRepoError("Adapter output schema invalid")
        status = payload.get("status")
        if status not in {"running", "succeeded", "failed"}:
            raise DmlRepoError("Adapter output schema invalid")
        if status == "succeeded":
            allowed = {"status", "error", "dag_id"}
            if not set(payload.keys()).issubset(allowed):
                raise DmlRepoError("Adapter output schema invalid")
            dag_id = payload.get("dag_id")
            if not isinstance(dag_id, str) or not dag_id:
                raise DmlRepoError("Adapter output schema invalid: succeeded requires dag_id")
            if payload.get("error") is not None:
                raise DmlRepoError("Adapter output schema invalid")
        elif status == "failed":
            if set(payload.keys()) != {"status", "error"}:
                raise DmlRepoError("Adapter output schema invalid")
            if payload.get("error") is None:
                raise DmlRepoError("Adapter output schema invalid")
        else:
            if set(payload.keys()) != {"status", "error", "state"}:
                raise DmlRepoError("Adapter output schema invalid")
            if payload.get("error") is not None:
                raise DmlRepoError("Adapter output schema invalid")
            if not isinstance(payload.get("state"), dict):
                raise DmlRepoError("Adapter output schema invalid: running requires state")
        return payload

    @staticmethod
    def _caller_identity(index_ref: Ref, txn) -> tuple[str | None, str | None]:
        ctx = txn.get_ctx(index_ref)
        if ctx.dag is None:
            raise DmlRepoError("Index commit has no DAG.")
        if ctx.dag.argv is None:
            return index_ref.id(), None
        return None, ctx.dag.argv.id()

    def _prepare_adapter_call(self, index_ref: Ref, argv_ref: Ref, txn) -> _PreparedAdapterCall:
        argv_node: ArgvNode = txn.get(argv_ref)
        argv_datum: ListDatum = txn.get(argv_node.datum_ref(txn))
        if len(argv_datum.data) == 0:
            raise DmlRepoError("argv is empty")
        fn_runnable_ref = argv_datum.data[0]
        fn_runnable: RunnableDatum = txn.get(fn_runnable_ref)
        if not isinstance(fn_runnable, RunnableDatum):
            raise DmlRepoError("First arg must resolve to a Runnable datum")
        adapter = fn_runnable.adapter
        adapter_path = shutil.which(adapter) if "/" not in adapter else adapter
        adapter_cmd: tuple[str, ...] | None = None
        if not adapter_path:
            from daggerml.contrib.adapter_registry import get_adapter, list_adapters

            adapter_spec = None
            for adapter_name in list_adapters():
                candidate = get_adapter(adapter_name)
                if getattr(candidate, "executable", None) == fn_runnable.adapter:
                    adapter_spec = candidate
                    break
            if adapter_spec is None:
                raise DmlRepoError(f"No such adapter: {fn_runnable.adapter}")
            module_name = getattr(adapter_spec, "__module__", None)
            qualname = getattr(adapter_spec, "__qualname__", None)
            if not isinstance(module_name, str) or not isinstance(qualname, str):
                raise DmlRepoError(f"No such adapter: {fn_runnable.adapter}")
            adapter_path = adapter
            adapter_cmd = (
                sys.executable,
                "-c",
                (
                    "import importlib, sys\n"
                    f"obj = importlib.import_module({module_name!r})\n"
                    f"for part in {qualname.split('.')!r}:\n"
                    "    obj = getattr(obj, part)\n"
                    "raise SystemExit(obj.cli())\n"
                ),
            )
        node_ops = NodeOps(_db=self._db)
        caller_index_id, caller_cache_key = self._caller_identity(index_ref=index_ref, txn=txn)
        return _PreparedAdapterCall(
            argv_ref=argv_ref,
            adapter_path=adapter_path,
            adapter_cmd=adapter_cmd,
            cache_key=argv_ref.id(),
            runnable=self._runnable_envelope(fn_runnable_ref, txn, node_ops),
            caller_index_id=caller_index_id,
            caller_cache_key=caller_cache_key,
        )

    def _load_remote_dag(self, dag_id: str) -> Ref:
        remote_ops = self._remote_ops()
        dag_ref = remote_ops._decode_ref(remote_ops._remote_get_dag_ref(dag_id))
        return remote_ops.load_ptr(dag_ref["target"], expected_root_ns="dag")

    def _build_failed_execution_dag(self, argv_ref: Ref, error_message: str) -> Ref:
        with self._tx(readonly=False) as txn:
            fn_index_ref = self._create(argv=argv_ref, txn=txn)
            idx_ctx = txn.get_ctx(fn_index_ref)
            if idx_ctx.dag is None:
                raise DmlRepoError("Function index has no DAG.")
            idx_ctx.dag.error = txn.put(Error.from_ex(DmlRepoError(error_message)))
            idx_ctx.commit.dag = txn.put(idx_ctx.dag)
            idx_ctx.commit.modified = now()
            commit_ref = txn.put(idx_ctx.commit)
            commit_obj: Commit = txn.get(commit_ref)
            if commit_obj.dag is None:
                raise DmlRepoError("Function commit has no DAG.")
            txn.delete(fn_index_ref)
            return commit_obj.dag

    def _publish_terminal_state(self, argv_ref: Ref, state: Mapping[str, Any]) -> None:
        cops = CacheOps(_db=self._db, remote_root=self.remote_root)
        if state["status"] == "succeeded":
            dag_id = state.get("dag_id")
            if not isinstance(dag_id, str) or not dag_id:
                raise DmlRepoError("Execution state succeeded but dag_id is missing")
            dag_ref = self._load_remote_dag(dag_id)
        elif state["status"] == "failed":
            dag_ref = self._build_failed_execution_dag(argv_ref, state.get("error") or "Adapter reported failure")
        else:
            raise DmlRepoError(f"Cannot publish non-terminal execution state: {state['status']}")
        cops.put(dag_ref)

    @staticmethod
    def _record_call_edges(prepared: _PreparedAdapterCall, state: ExecutionState) -> None:
        if prepared.caller_index_id is not None:
            state.record_index_call(index_id=prepared.caller_index_id, callee_cache_key=prepared.cache_key)
            return
        if prepared.caller_cache_key is not None:
            state.record_fn_call(caller_cache_key=prepared.caller_cache_key, callee_cache_key=prepared.cache_key)

    def _call_adapter(
        self,
        prepared: _PreparedAdapterCall,
        argv_ptr: str,
        *,
        execution_id: str,
        state: dict[str, Any] | None,
    ) -> dict[str, Any]:
        envelope = {
            "argv_ptr": argv_ptr,
            "cache_key": prepared.cache_key,
            "execution_id": execution_id,
            "remote": {
                "root": self.remote_root,
            },
            "runnable": prepared.runnable,
            "state": state,
        }
        cmd = list(prepared.adapter_cmd) if prepared.adapter_cmd is not None else [prepared.adapter_path]
        if prepared.adapter_cmd is None and prepared.adapter_path.endswith(".py"):
            cmd = [sys.executable, prepared.adapter_path]
        result_data = run(
            cmd,
            input=json.dumps(envelope, default=lambda x: x.uri if isinstance(x, Uri) else x),
            capture_output=True,
            text=True,
        )
        if result_data.returncode != 0:
            raise DmlRepoError(f"Adapter call failed: {result_data.stderr}")
        try:
            stdout = json.loads(result_data.stdout)
        except json.JSONDecodeError as e:
            raise DmlRepoError("Adapter output must be JSON") from e
        return self._validate_adapter_output(stdout)

    def _finish_fn_result(self, dag_ref: Ref, argv: list[Ref], name: Optional[str], txn, index_ref: Ref) -> Ref:
        dag_obj: Dag = txn.get(dag_ref)
        if dag_obj.result is None and dag_obj.error is None:
            raise DmlRepoError("Function DAG has no result node.")
        out = self._put_node(FnNode(argv, dag_ref), name=name, txn=txn, index_ref=index_ref)
        if dag_obj.error is not None:
            err = txn.get(dag_obj.error)
            raise err
        return out

    def _put_literal(self, value: Any, txn, index_ref: Ref, name: Optional[str] = None) -> Ref:
        self._validate_index_ref(index_ref)
        idx_ctx = txn.get_ctx(index_ref)
        if idx_ctx.dag is None:
            raise DmlRepoError("Index commit has no DAG.")

        def _put(x) -> Ref:
            if isinstance(x, Ref):
                if not txn.exists(x):
                    raise DmlRepoError(f"Referenced object does not exist: {x}")
                if x.nss()[0] == "node":
                    latest_ctx = txn.get_ctx(index_ref)
                    if latest_ctx.dag is None:
                        raise DmlRepoError("Index commit has no DAG.")
                    if x not in latest_ctx.dag.nodes:
                        raise DmlRepoError(f"Referenced node is not part of DAG: {x}")
                    return x
                if x.nss()[0] == "datum":
                    return x
                raise DmlRepoError(f"Invalid reference namespace for literal value: {x.ns()}")
            if isinstance(x, Runnable):
                target_ref = _put(x.target)
                if target_ref.nss()[0] == "node":
                    target_ref = self._resolve_node_value_ref(target_ref, txn)
                if target_ref.ns() != "datum-uri":
                    raise DmlRepoError("Runnable target must resolve to a Uri datum.")
                sub_ref = None
                if x.sub is not None:
                    sub_ref = _put(x.sub)
                    if sub_ref.nss()[0] == "node":
                        sub_ref = self._resolve_node_value_ref(sub_ref, txn)
                    if sub_ref.ns() != "datum-runnable":
                        raise DmlRepoError("Runnable sub must resolve to a Runnable datum.")
                kwargs_data = {}
                for key, value in x.kwargs.items():
                    if not isinstance(key, str):
                        raise DmlRepoError("Runnable kwargs keys must be strings.")
                    value_ref = _put(value)
                    if value_ref.nss()[0] == "node":
                        value_ref = self._resolve_node_value_ref(value_ref, txn)
                    if value_ref.nss()[0] != "datum":
                        raise DmlRepoError("Runnable kwargs values must resolve to datum refs.")
                    kwargs_data[key] = value_ref
                kwargs_ref = txn.put(DictDatum(data=kwargs_data))
                return txn.put(RunnableDatum(target=target_ref, sub=sub_ref, kwargs=kwargs_ref, adapter=x.adapter))
            if isinstance(x, Datum):
                return txn.put(x)
            if isinstance(x, set):
                raise DmlRepoError("Set literals are not supported.")
            if isinstance(x, tuple):
                x = list(x)
            if isinstance(x, list):
                ys = [_put(v) for v in x]
                if any(isinstance(v, Ref) and v.nss()[0] == "node" for v in ys):
                    ys = [self._put_literal(v, txn, index_ref) if v.nss()[0] != "node" else v for v in ys]
                    fn_uri = txn.put(Uri("daggerml:list"))
                    fn_kwargs = txn.put(DictDatum(data={}))
                    fn = self._put_literal(
                        RunnableDatum(target=fn_uri, sub=None, kwargs=fn_kwargs, adapter=""), txn, index_ref
                    )
                    argv_refs = [fn, *ys]
                    argv_ref = self._prepare_fn(index_ref, argv_refs, {}, txn)
                    dag_ref = self._run_builtin(argv_ref, txn)
                    assert dag_ref is not None
                    dag_obj: Dag = txn.get(dag_ref)
                    if dag_obj.result is None and dag_obj.error is None:
                        raise DmlRepoError("Function DAG has no result node.")
                    resp = self._put_node(FnNode(argv_refs, dag_ref), name=None, txn=txn, index_ref=index_ref)
                    if dag_obj.error is not None:
                        raise txn.get(dag_obj.error)
                    return resp
                return txn.put(ListDatum(ys))
            if isinstance(x, dict):
                ys = {k: _put(v) for k, v in x.items()}
                if any(isinstance(v, Ref) and v.nss()[0] == "node" for v in ys.values()):
                    yks = [self._put_literal(k, txn, index_ref) for k in ys.keys()]
                    yvs = [self._put_literal(v, txn, index_ref) if v.nss()[0] != "node" else v for v in ys.values()]
                    fn_uri = txn.put(Uri("daggerml:dict"))
                    fn_kwargs = txn.put(DictDatum(data={}))
                    fn = self._put_literal(
                        RunnableDatum(target=fn_uri, sub=None, kwargs=fn_kwargs, adapter=""), txn, index_ref
                    )
                    argv_refs = [fn, *unnest(zip(yks, yvs, strict=True))]
                    argv_ref = self._prepare_fn(index_ref, argv_refs, {}, txn)
                    dag_ref = self._run_builtin(argv_ref, txn)
                    assert dag_ref is not None
                    dag_obj: Dag = txn.get(dag_ref)
                    if dag_obj.result is None and dag_obj.error is None:
                        raise DmlRepoError("Function DAG has no result node.")
                    resp = self._put_node(FnNode(argv_refs, dag_ref), name=None, txn=txn, index_ref=index_ref)
                    if dag_obj.error is not None:
                        raise txn.get(dag_obj.error)
                    return resp
                return txn.put(DictDatum(ys))
            return txn.put(ScalarDatum(x))

        result_ref = _put(value)
        if result_ref.nss()[0] == "node":
            if name is not None:
                latest = txn.get_ctx(index_ref)
                if latest.dag is None:
                    raise DmlRepoError("Index commit has no DAG.")
                latest.dag.nodes = sorted({result_ref, *latest.dag.nodes})
                latest.dag.names[name] = result_ref
                latest.commit.dag = txn.put(latest.dag)
                latest.commit.modified = now()
                latest.head.commit = txn.put(latest.commit)
                txn.put(latest.head, to=index_ref)
            return result_ref
        # Create literal node directly in transaction
        node_ref = txn.put(LiteralNode(value=result_ref))
        idx_ctx.dag.nodes = sorted({node_ref, *idx_ctx.dag.nodes})
        if name is not None:
            idx_ctx.dag.names[name] = node_ref
        idx_ctx.commit.dag = txn.put(idx_ctx.dag)
        idx_ctx.commit.modified = now()
        idx_ctx.head.commit = txn.put(idx_ctx.commit)
        txn.put(idx_ctx.head, to=index_ref)
        return node_ref
