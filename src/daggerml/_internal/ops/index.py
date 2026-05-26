"""Index operations for managing working state and function execution.

Public API:
    IndexOps - Class for index and execution operations
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from subprocess import run
from typing import Any, Optional, cast
from urllib.parse import urlparse

from daggerml._internal._db import Ref
from daggerml._internal.builtins import BUILTIN_FNS
from daggerml._internal.exec_state import ExecutionRecord, ExecutionState, LaunchState
from daggerml._internal.ops.base_ops import BaseOps, with_retry
from daggerml._internal.ops.cache import CacheOps
from daggerml._internal.ops.dag import DagOps
from daggerml._internal.ops.head import HeadOps
from daggerml._internal.ops.node import NodeOps
from daggerml._internal.ops.remote import RemoteOps
from daggerml._internal.types import (
    ArgvNode,
    Commit,
    Dag,
    Datum,
    DictDatum,
    DmlPointerConflictError,
    DmlRepoError,
    Error,
    FnNode,
    ImportNode,
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
from daggerml._internal.util import now, unnest, uuid7

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _PreparedAdapterCall:
    argv_ref: Ref
    adapter_path: str
    cache_key: str
    runnable: dict[str, Any]
    caller_execution_id: str | None = None


@dataclass
class IndexOps(BaseOps):
    remote_root: str

    def _remote_ops(self):
        parsed = urlparse(self.remote_root)
        prefix = parsed.path.strip("/")
        return RemoteOps(_db=self._db, bucket=parsed.netloc, prefix=f"{prefix}/dml" if prefix else "dml")

    def start_fn(
        self,
        execution_id: str,
        argv: list[Ref],
        kwargv: Optional[dict[str, Ref]] = None,
        name: Optional[str] = None,
    ) -> Optional[Ref]:
        kwargv = kwargv or {}
        dag_ref = self._start_fn(execution_id, argv, kwargv=kwargv)
        if dag_ref is not None:
            out = self._retry_index_publication(
                execution_id,
                lambda old_commit, retry_txn: self._put_node_retry(FnNode(argv, dag_ref), name, old_commit, retry_txn),
            )
            with self._tx(readonly=True) as txn:
                dag_obj: Dag = txn.get(dag_ref)
                if dag_obj.result is None and dag_obj.error is None:
                    raise DmlRepoError("Function DAG has no result node.")
                if dag_obj.error is not None:
                    err = txn.get(dag_obj.error)
                    if not isinstance(err, Error):
                        raise DmlRepoError(f"Expected Error object, got: {type(err).__name__}")
                    raise err
            return out

    def _start_fn(
        self,
        execution_id: str,
        argv: list[Ref],
        kwargv: dict[str, Ref],
    ) -> Optional[Ref]:
        # Important: if the called function produced a DaggerML Error, we still
        # want any DB + pointer updates performed while finishing the call to be
        # committed. We therefore capture the error inside the transaction and
        # raise it only after the txn scope exits successfully.
        cops = CacheOps(_db=self._db, remote_root=self.remote_root)
        with self._tx(readonly=False) as txn:
            argv_ref = self._prepare_fn(execution_id, argv, kwargv, txn)
            dag_ref = self._run_builtin(argv_ref, txn)
            if dag_ref is not None:
                return dag_ref
            else:
                dag_ref = cops.get(argv_ref, txn)
                if dag_ref is not None:
                    return dag_ref
                else:
                    prepared = self._prepare_adapter_call(execution_id, argv_ref, txn)
        argv_ptr = self._remote_ops().put_ref_manifest(prepared.argv_ref)
        es = ExecutionState(prepared.cache_key, remote_root=self.remote_root)
        # Step 1: try to acquire the mutex
        if not es.lock():
            # Another process is driving this cycle
            return None
        try:
            # Step 2: post-lock cache check
            with self._tx(readonly=False) as txn:
                dag_ref = cops.get(prepared.argv_ref, txn)
                if dag_ref is not None:
                    es.unlock()
                    return dag_ref
            # Step 2.1: get existing execution record
            callee_execution_id = es.read_active_execution_id()
            if callee_execution_id is None:
                # no existing execution
                callee_execution_id = uuid7().hex
                state = None
            else:
                launch_state = es.read_launch_state(callee_execution_id) or {}
                state = launch_state.get("resume_state")
            # Step 3: call adapter (holding the lock)
            result = self._call_adapter(
                prepared,
                argv_ptr,
                execution_id=callee_execution_id,
                state=state,
                execution_status="running",
                cancel_requested_by=None,
            )
            self._record_call_edges(prepared, es, execution_id=callee_execution_id)
            # Step 4: handle adapter result
            status = result["status"]
            if status in {"succeeded", "failed"}:
                es.delete_active_execution()
                if status == "failed":
                    # set failed execution record
                    try:
                        callee_record = es.read_execution_record(callee_execution_id)
                    except DmlRepoError:
                        # no record exists
                        es.create_execution_record(
                            {
                                "execution_id": callee_execution_id,
                                "cache_key": prepared.cache_key,
                                "lifecycle": "failed",
                                "updated_at": int(time.time()),
                                "spawned_execution_ids": [],
                                "cancellation_requested_by": None,
                            }
                        )
                    else:
                        # record exists -- update
                        if callee_record["lifecycle"] in {"running", "succeeded"}:
                            callee_record.update({"lifecycle": "failed", "updated_at": int(time.time())})
                            es.update_execution_record(callee_record)
                    error = Error.from_ex(DmlRepoError(result.get("error") or "Adapter failure"))
                    with self._tx(readonly=False) as txn:
                        dag_ref = self._build_scratch_dag_in_txn(prepared.argv_ref, txn, error=error)
                elif result["dag_id"] is None:
                    raise DmlRepoError("Adapter reported success but no cached DAG was published")
                else:
                    remote_ops = self._remote_ops()
                    manifest_oid = remote_ops._decode_ref(remote_ops._remote_get_dag_ref(result["dag_id"]))["target"]
                    dag_ref = remote_ops.load_ptr(
                        manifest_oid,
                        expected_root_ns="dag",
                    )
                    with self._tx(readonly=True) as txn:
                        targets = remote_ops._targets_for_root(txn, dag_ref)
                    remote_ops.put_cache_ref(
                        prepared.cache_key,
                        manifest_oid,
                        targets=targets,
                        execution_id=callee_execution_id,
                    )
                    return dag_ref
                cops.put(dag_ref, execution_id=callee_execution_id)
                return dag_ref
            elif status == "cancelled":
                es.delete_active_execution()
                return None
            else:
                if state is None:
                    launch_state_record: LaunchState = {
                        "execution_id": callee_execution_id,
                        "cache_key": prepared.cache_key,
                        "resume_state": cast(dict[str, Any], result["state"]),
                        "created_at": int(time.time()),
                    }
                    es.create_launch_state(launch_state_record)
                    if not es.create_active_execution(callee_execution_id):
                        raise DmlRepoError(f"Active execution already exists for cache key: {prepared.cache_key}")
                return None
        finally:
            es.unlock()

    def delete(self, execution_id: str) -> None:
        """Delete an index object from db."""
        HeadOps(_db=self._db).delete_index(execution_id)

    def cancel(self, execution_id: str, requested_by: str, max_workers: int) -> dict[str, list[str]]:
        # step 1: move index pointer to cancelled (if it exists) to prevent new executions from starting
        head_ops = HeadOps(_db=self._db)
        live_path = head_ops._index_path(execution_id)
        cancelled_path = head_ops._local_indexes_dir() / ".cancelled" / head_ops._validate_index_id(execution_id)
        moved_index_path: Path | None = None
        if live_path.exists() or cancelled_path.exists():
            with head_ops._pointer_lock(live_path):
                if live_path.exists():
                    cancelled_path.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(live_path, cancelled_path)
                moved_index_path = cancelled_path
        # step 2: set status to "cancelled" using `execution_id` and return spawned_execution_ids
        es = ExecutionState.from_execution_id(execution_id, remote_root=self.remote_root)
        execution_ids, record = self._set_cancelled(es, execution_id, requested_by=requested_by)
        # step 3: submit cancellation tasks for each spawned execution, accumulating stats on the way.
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(self._run_cancel_tasks, eid, requested_by=execution_id): eid for eid in execution_ids
            }
            stats = {"cancelled": [], "skipped": []}
            for future in as_completed(futures):
                eid = futures[future]
                try:
                    result = future.result()
                    for k, v in result.items():
                        stats[k] = sorted(set(v + stats.get(k, [])))
                except Exception as exc:
                    logger.error(f"Error cancelling execution {eid}: {exc}")
        stats["skipped"] = [x for x in stats["skipped"] if x not in stats["cancelled"]]
        # step 4: mark the current execution as cancelled.
        if record is not None:
            es.update_execution_record({**record, "lifecycle": "cancelled", "updated_at": int(time.time())})
        # step 5: delete the moved index pointer.
        if moved_index_path is not None:
            try:
                moved_index_path.unlink()
            except FileNotFoundError:
                pass
        # return cancellation stats
        return stats

    @with_retry
    def create(
        self,
        execution_id: str,
        head: Optional[str] = None,
        commit: Optional[Ref] = None,
        argv_ptr: Optional[str] = None,
    ) -> str:
        """Create a new index object.

        Parameters
        ----------
        execution_id : str
            execution id to use
        head : str, optional
            Branch name to base the index on.
        commit : Ref, optional
            Commit reference to base the index on.
        argv_ptr : str, optional
            Optional remote manifest OID to initialize argv from.

        Returns
        -------
        str
            Opaque index id for the newly created index pointer.
        """
        cache_key = None
        modes = [head is not None, commit is not None, argv_ptr is not None]
        if sum(modes) != 1:
            raise DmlRepoError("Provide exactly one of branch, commit, or argv_ptr.")
        kw = {}
        if head is not None:
            kw["commit"] = HeadOps(_db=self._db).get_branch_commit(head)
        elif commit is not None:
            kw["commit"] = commit
        if argv_ptr is not None:
            kw["argv"] = self._remote_ops().load_ptr(argv_ptr, expected_root_ns="node-argv")
            with self._tx(readonly=True) as txn:
                cache_key = CacheOps(_db=self._db, remote_root=self.remote_root).get_cache_key(kw["argv"], txn)
        with self._tx(readonly=False) as txn:
            commit_ref = self._create(**kw, txn=txn)
        HeadOps(_db=self._db).create_index(commit_ref, execution_id=execution_id)
        # Create initial execution record.
        state = ExecutionState("__bogus__", remote_root=self.remote_root)
        state.create_execution_record(
            {
                "execution_id": execution_id,
                "cache_key": cache_key,
                "lifecycle": "running",
                "updated_at": int(time.time()),
                "spawned_execution_ids": [],
                "cancellation_requested_by": None,
            }
        )
        return execution_id

    @with_retry
    def get_kwargv(self, execution_id: str) -> Ref:
        """Return the kwargv node for an index (raises if missing)."""
        commit_ref = HeadOps(_db=self._db).get_index_commit(execution_id)
        with self._tx(readonly=True) as txn:
            ctx = txn.get_commit_ctx(commit_ref)
        return DagOps(_db=self._db).get_kwargv(cast(Ref, ctx.commit.dag))

    @with_retry
    def get_argv(self, execution_id: str) -> Ref:
        """Return the argv node for an index (raises if missing)."""
        commit_ref = HeadOps(_db=self._db).get_index_commit(execution_id)
        with self._tx(readonly=True) as txn:
            ctx = txn.get_commit_ctx(commit_ref)
        return DagOps(_db=self._db).get_argv(cast(Ref, ctx.commit.dag))

    @with_retry
    def get_node(self, execution_id: str, name: str) -> Ref:
        """Return a named node from an index's DAG."""
        commit_ref = HeadOps(_db=self._db).get_index_commit(execution_id)
        with self._tx(readonly=True) as txn:
            ctx = txn.get_commit_ctx(commit_ref)
            # Access the dag's names directly to avoid nested transactions
            dag: Dag = txn.get(cast(Ref, ctx.commit.dag))
            if name not in dag.names:
                raise DmlRepoError(f"Node '{name}' not found in DAG")
            return dag.names[name]

    @with_retry
    def describe(self, execution_id: str) -> dict[str, Any]:
        """Describe the current index state."""
        commit_ref = HeadOps(_db=self._db).get_index_commit(execution_id)
        with self._tx(readonly=True) as txn:
            ctx = txn.get_commit_ctx(commit_ref)
            return {"id": execution_id, "commit": commit_ref, "dags": ctx.tree.dags, "dag": ctx.commit.dag}

    @with_retry
    def set_node_name(self, execution_id: str, name: str, node_ref: Ref) -> Ref:
        """Set or replace a node name in the index DAG."""
        require_ref(node_ref, ["node"], "set_node_name node_ref")

        def _build(old_commit: Ref, txn):
            ctx = txn.get_commit_ctx(old_commit)
            if ctx.dag is None:
                raise DmlRepoError("Index commit has no DAG.")
            if node_ref not in ctx.dag.nodes:
                raise DmlRepoError("Node is not part of current DAG.")
            ctx.dag.names[name] = node_ref
            ctx.commit.dag = txn.put(ctx.dag)
            ctx.commit.modified = now()
            new_commit = txn.put(ctx.commit)
            return node_ref, new_commit

        return self._retry_index_publication(execution_id, _build)

    @with_retry
    def put_import(self, execution_id: str, dag: Ref, node: Optional[Ref], name: Optional[str] = None) -> Ref:
        """Import a node from another DAG into the current index DAG."""
        # NOTE: This is where we check to ensure `dag` is finished and `node in dag`. Nowhere else.

        def _build(old_commit: Ref, txn):
            ctx = txn.get_commit_ctx(old_commit)
            dag_obj: Dag = txn.get(dag)
            if dag_obj is None or dag_obj.result is None:
                raise DmlRepoError("Cannot import from a DAG with no result node")
            node_ = node or dag_obj.result
            if node_ not in dag_obj.nodes:
                raise DmlRepoError("Node to import not found in source DAG")
            if dag == ctx.commit.dag:
                raise DmlRepoError("Cannot import from the current DAG")
            node_obj = ImportNode(dag, node_)
            return self._put_node_retry(node_obj, name, old_commit, txn)

        return self._retry_index_publication(execution_id, _build)

    @with_retry
    def put_literal(self, execution_id: str, value: Any, name: Optional[str] = None) -> Ref:
        return self._retry_index_publication(
            execution_id,
            lambda old_commit, txn: self._put_literal_retry(
                value,
                name=name,
                txn=txn,
                execution_id=execution_id,
                old_commit=old_commit,
            ),
        )

    @with_retry
    def commit(
        self,
        execution_id: str,
        value: Ref | Error,
        head: Optional[str] = None,
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
        head_ops = HeadOps(_db=self._db)
        old_commit = head_ops.get_index_commit(execution_id)
        while True:
            with self._tx(readonly=False) as txn:
                ctx = txn.get_commit_ctx(old_commit)
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
                committed_dag_ref = ctx.commit.dag
            if head is None:
                break
            try:
                branch_commit = head_ops.get_branch_commit(head)
                head_ops.update_branch_commit(head, branch_commit, commit_ref)
                break
            except DmlPointerConflictError as err:
                branch_commit = err.current_commit
        es = ExecutionState.from_execution_id(execution_id, remote_root=self.remote_root)
        # A committed Error value still means the execution successfully produced
        # and finalized a DAG result. Runtime failed is reserved for execution
        # path failures that prevent a DAG from being committed at all.
        exec_record = es.read_execution_record(execution_id)
        exec_record.update({"lifecycle": "succeeded", "updated_at": int(time.time()), "spawned_execution_ids": []})
        es.update_execution_record(exec_record)
        # check if argv
        if ctx.dag.argv is not None:
            self._remote_ops().put_ref_manifest(committed_dag_ref)
        head_ops.delete_index(execution_id)
        return commit_ref

    def _resolve_node_value_ref(self, node_ref: Ref, txn) -> Ref:
        # Validate node ref using NodeOps then return its underlying datum ref
        node_ref = NodeOps(_db=self._db)._require_node_ref(node_ref)
        node = txn.get(node_ref)
        return node.datum_ref(txn)

    def _create(
        self,
        *,
        commit: Optional[Ref] = None,
        argv: Optional[Ref] = None,  # -> ArgvNode
        txn,
    ) -> Ref:
        nodes: list[Ref] = []
        kw: dict[str, Any] = {"author": "DaggerML User"}
        if commit is not None:
            if argv is not None:
                raise DmlRepoError("Cannot provide both commit and argv.")
            base_ctx = txn.get_commit_ctx(commit)
            kw.update({"parents": [commit], "tree": base_ctx.commit.tree})
        elif argv is not None:
            argv_obj: ArgvNode = txn.get(argv)
            if not isinstance(argv_obj, ArgvNode):
                raise DmlRepoError("Argv node required")
            nodes.append(argv)
            nodes.append(self._kwargv_from_argv(argv, txn))
            kw.update({"parents": [], "tree": txn.put(Tree(dags={}))})
        else:
            raise DmlRepoError("Either commit or argv must be provided.")
        dag_ref = txn.put(Dag(nodes=nodes, names={}, result=None, argv=argv))
        return txn.put(Commit(message="", dag=dag_ref, **kw))

    ##################################################
    #################### START_FN ####################
    ##################################################
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

    def _resolve_runnable_kwargs(self, runnable_ref: Ref, kwargv: dict[str, Ref], execution_id: str, txn) -> Ref:
        ctx = txn.get_commit_ctx(HeadOps(_db=self._db).get_index_commit(execution_id))
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
        execution_id: str,
        argv: list[Ref],
        kwargv: dict[str, Ref],
        txn,
        ctx=None,
    ) -> Ref:
        if len(argv) == 0:
            raise DmlRepoError("argv is empty")
        [require_ref(arg, ["node"], "start_fn argv elements") for arg in argv]
        ctx = ctx or txn.get_commit_ctx(HeadOps(_db=self._db).get_index_commit(execution_id))
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
        runnable_ref = self._resolve_runnable_kwargs(fn_datum_ref, kwargv, execution_id, txn)
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
        return self._build_scratch_dag_in_txn(argv_ref, txn, result=result)

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
        if status not in {"running", "succeeded", "failed", "cancelled"}:
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
        elif status == "cancelled":
            if set(payload.keys()) != {"status", "error"}:
                raise DmlRepoError("Adapter output schema invalid")
            if payload.get("error") is not None:
                raise DmlRepoError("Adapter output schema invalid")
        else:
            if set(payload.keys()) != {"status", "error", "state"}:
                raise DmlRepoError("Adapter output schema invalid")
            if payload.get("error") is not None:
                raise DmlRepoError("Adapter output schema invalid")
            if not isinstance(payload.get("state"), dict):
                raise DmlRepoError("Adapter output schema invalid: running requires state")
        return payload

    def _prepare_adapter_call(
        self,
        execution_id: str,
        argv_ref: Ref,
        txn,
    ) -> _PreparedAdapterCall:
        argv_datum: ListDatum = txn.get(txn.get(argv_ref).datum_ref(txn))
        if len(argv_datum.data) == 0:
            raise DmlRepoError("argv is empty")
        fn_runnable_ref = argv_datum.data[0]
        fn_runnable: RunnableDatum = txn.get(fn_runnable_ref)
        if not isinstance(fn_runnable, RunnableDatum):
            raise DmlRepoError("First arg must resolve to a Runnable datum")
        adapter_path = shutil.which(fn_runnable.adapter)
        if not adapter_path:
            raise DmlRepoError(f"No such adapter: {fn_runnable.adapter}")
        node_ops = NodeOps(_db=self._db)
        return _PreparedAdapterCall(
            argv_ref=argv_ref,
            adapter_path=adapter_path,
            cache_key=CacheOps(_db=self._db, remote_root=self.remote_root).get_cache_key(argv_ref, txn),
            runnable=self._runnable_envelope(fn_runnable_ref, txn, node_ops),
            caller_execution_id=execution_id,
        )

    @staticmethod
    def _record_call_edges(prepared: _PreparedAdapterCall, state: ExecutionState, *, execution_id: str) -> None:
        if prepared.caller_execution_id is None:
            return
        state.record_execution_dependency(
            caller_execution_id=prepared.caller_execution_id,
            callee_execution_id=execution_id,
        )
        exec_record = state.read_execution_record(prepared.caller_execution_id)
        exec_ids = sorted(set([*exec_record["spawned_execution_ids"], execution_id]))
        exec_record.update({"spawned_execution_ids": exec_ids, "updated_at": int(time.time())})
        state.update_execution_record(exec_record)

    ################################################
    #################### CANCEL ####################
    ################################################
    def _set_cancelled(self, state, execution_id: str, *, requested_by: str) -> tuple[set[str], ExecutionRecord | None]:
        """Mark one execution cancel-pending and return its direct children."""
        record = state.read_execution_record(execution_id)
        if record is None:
            return set(), None
        if record["cache_key"] is not None:
            while not state.lock():
                time.sleep(0.05)
            try:
                record = state.read_execution_record(execution_id)
                state.delete_active_execution()
                state.update_execution_record(
                    {
                        **record,
                        "lifecycle": "cancel-pending",
                        "updated_at": int(time.time()),
                        "cancellation_requested_by": requested_by,
                    }
                )
            finally:
                state.unlock()
        return set(record["spawned_execution_ids"]), record

    def _run_cancel_tasks(self, callee_execution_id: str, *, requested_by: str) -> dict[str, list[str]]:
        """Remove one caller edge and notify the child adapter if orphaned."""
        state = ExecutionState.from_execution_id(callee_execution_id, remote_root=self.remote_root)
        # step 0: acquire lock for the callee cache key to prevent race conditions
        while not state.lock():
            time.sleep(0.05)
        try:
            # step 1: drop the caller edge from the parent execution record
            state.delete_execution_dependency(
                caller_execution_id=requested_by,
                callee_execution_id=callee_execution_id,
            )
            # step 2.1: short circuit return if there are still callers
            if state.list_execution_callers(callee_execution_id):
                return {"skipped": [callee_execution_id]}
            # step 2.2: short circuit return if execution is already finished
            record = state.read_execution_record(callee_execution_id)
            if record is None or record["lifecycle"] in {"cancelled", "succeeded", "failed"}:
                return {"skipped": [callee_execution_id]}
            # step 3: remove "active" pointer to this execution
            state.delete_active_execution()
        finally:
            state.unlock()
        # step 4: call the child adapter with cancel notification
        argv_ref = Ref(f"node-argv:{cast(str, record['cache_key'])}")
        with self._tx(readonly=True) as txn:
            prepared = self._prepare_adapter_call(callee_execution_id, argv_ref, txn)
            argv_ptr = self._remote_ops().put_ref_manifest(prepared.argv_ref)
            launch_state = state.read_launch_state(callee_execution_id) or {}
            result = self._call_adapter(
                prepared,
                argv_ptr,
                execution_id=callee_execution_id,
                state=launch_state.get("resume_state"),
                execution_status="cancel-pending",
                cancel_requested_by=requested_by,
            )
        logger.info(
            "cancel child adapter response caller=%s callee=%s result=%s",
            requested_by,
            callee_execution_id,
            result,
        )
        # return merged stats from adapter response with dropped caller edge
        resp = cast(dict, result.get("state", {}))
        resp["cancelled"] = sorted({*resp.get("cancelled", []), callee_execution_id})
        resp["skipped"] = [x for x in resp.get("skipped", []) if x != callee_execution_id]
        return resp

    ##################################################
    ################## ADAPTER CALL ##################
    ##################################################
    def _call_adapter(
        self,
        prepared: _PreparedAdapterCall,
        argv_ptr: str,
        *,
        execution_id: str,
        state: dict[str, Any] | None,
        execution_status: str | None,
        cancel_requested_by: str | None,
    ) -> dict[str, Any]:
        envelope = {
            "argv_ptr": argv_ptr,
            "cache_key": prepared.cache_key,
            "execution_id": execution_id,
            "remote": {"root": self.remote_root},
            "runnable": prepared.runnable,
            "state": state,
            "execution_status": execution_status,
            "cancel_requested_by": cancel_requested_by,
        }
        result_data = run(
            [prepared.adapter_path],
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

    def _put_literal(self, value: Any, txn, execution_id: str, name: Optional[str] = None, idx_ctx=None) -> Ref:
        if idx_ctx is None:
            idx_ctx = txn.get_commit_ctx(HeadOps(_db=self._db).get_index_commit(execution_id))
        if idx_ctx.dag is None:
            raise DmlRepoError("Index commit has no DAG.")

        def _put(x) -> Ref:
            if isinstance(x, Ref):
                if not txn.exists(x):
                    raise DmlRepoError(f"Referenced object does not exist: {x}")
                if x.nss()[0] == "node":
                    if x not in idx_ctx.dag.nodes:
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
                    ys = [
                        self._put_literal(v, txn, execution_id, idx_ctx=idx_ctx) if v.nss()[0] != "node" else v
                        for v in ys
                    ]
                    fn_uri = txn.put(Uri("daggerml:list"))
                    fn_kwargs = txn.put(DictDatum(data={}))
                    fn = self._put_literal(
                        RunnableDatum(target=fn_uri, sub=None, kwargs=fn_kwargs, adapter=""),
                        txn,
                        execution_id,
                        idx_ctx=idx_ctx,
                    )
                    argv_refs = [fn, *ys]
                    argv_ref = self._prepare_fn(execution_id, argv_refs, {}, txn, ctx=idx_ctx)
                    dag_ref = self._run_builtin(argv_ref, txn)
                    assert dag_ref is not None
                    dag_obj: Dag = txn.get(dag_ref)
                    if dag_obj.result is None and dag_obj.error is None:
                        raise DmlRepoError("Function DAG has no result node.")
                    resp = self._put_node_in_ctx(idx_ctx, txn, FnNode(argv_refs, dag_ref))
                    if dag_obj.error is not None:
                        raise txn.get(dag_obj.error)
                    return resp
                return txn.put(ListDatum(ys))
            if isinstance(x, dict):
                ys = {k: _put(v) for k, v in x.items()}
                if any(isinstance(v, Ref) and v.nss()[0] == "node" for v in ys.values()):
                    yks = [self._put_literal(k, txn, execution_id, idx_ctx=idx_ctx) for k in ys.keys()]
                    yvs = [
                        self._put_literal(v, txn, execution_id, idx_ctx=idx_ctx) if v.nss()[0] != "node" else v
                        for v in ys.values()
                    ]
                    fn_uri = txn.put(Uri("daggerml:dict"))
                    fn_kwargs = txn.put(DictDatum(data={}))
                    fn = self._put_literal(
                        RunnableDatum(target=fn_uri, sub=None, kwargs=fn_kwargs, adapter=""),
                        txn,
                        execution_id,
                        idx_ctx=idx_ctx,
                    )
                    argv_refs = [fn, *unnest(zip(yks, yvs, strict=True))]
                    argv_ref = self._prepare_fn(execution_id, argv_refs, {}, txn, ctx=idx_ctx)
                    dag_ref = self._run_builtin(argv_ref, txn)
                    assert dag_ref is not None
                    dag_obj: Dag = txn.get(dag_ref)
                    if dag_obj.result is None and dag_obj.error is None:
                        raise DmlRepoError("Function DAG has no result node.")
                    resp = self._put_node_in_ctx(idx_ctx, txn, FnNode(argv_refs, dag_ref))
                    if dag_obj.error is not None:
                        raise txn.get(dag_obj.error)
                    return resp
                return txn.put(DictDatum(ys))
            return txn.put(ScalarDatum(x))

        result_ref = _put(value)
        if result_ref.nss()[0] == "node":
            idx_ctx.dag.nodes = sorted({result_ref, *idx_ctx.dag.nodes})
            if name is not None:
                idx_ctx.dag.names[name] = result_ref
            idx_ctx.commit.dag = txn.put(idx_ctx.dag)
            idx_ctx.commit.modified = now()
            return result_ref
        # Create literal node directly in transaction
        node_ref = txn.put(LiteralNode(value=result_ref))
        idx_ctx.dag.nodes = sorted({node_ref, *idx_ctx.dag.nodes})
        if name is not None:
            idx_ctx.dag.names[name] = node_ref
        idx_ctx.commit.dag = txn.put(idx_ctx.dag)
        return node_ref

    def _put_literal_retry(
        self,
        value: Any,
        txn,
        execution_id: str,
        old_commit: Ref,
        name: Optional[str] = None,
    ) -> tuple[Ref, Ref]:
        idx_ctx = txn.get_commit_ctx(old_commit)
        node_ref = self._put_literal(value, txn, execution_id, name=name, idx_ctx=idx_ctx)
        idx_ctx.commit.modified = now()
        return node_ref, txn.put(idx_ctx.commit)

    def _put_node_retry(self, node: Node, name: Optional[str], old_commit: Ref, txn) -> tuple[Ref, Ref]:
        ctx = txn.get_commit_ctx(old_commit)
        if ctx.dag is None:
            raise DmlRepoError("Index commit has no DAG.")
        node_ref = self._put_node_in_ctx(ctx, txn, node, name=name)
        ctx.commit.modified = now()
        return node_ref, txn.put(ctx.commit)

    def _put_node_in_ctx(self, ctx, txn, node: Node, name: Optional[str] = None) -> Ref:
        node_ref = txn.put(node)
        ctx.dag.nodes = sorted({node_ref, *ctx.dag.nodes})
        if name is not None:
            ctx.dag.names[name] = node_ref
        ctx.commit.dag = txn.put(ctx.dag)
        return node_ref

    def _retry_index_publication(self, execution_id: str, build):
        head_ops = HeadOps(_db=self._db)
        old_commit = head_ops.get_index_commit(execution_id)
        while True:
            with self._tx(readonly=False) as txn:
                result, new_commit = build(old_commit, txn)
            try:
                head_ops.update_index_commit(execution_id, old_commit, new_commit)
                return result
            except DmlPointerConflictError as err:
                old_commit = err.current_commit

    def _store_scratch_value(self, value: Any, txn) -> Ref:
        if isinstance(value, Datum):
            return txn.put(value)
        if isinstance(value, Runnable):
            target_ref = self._store_scratch_value(value.target, txn)
            if target_ref.ns() != "datum-uri":
                raise DmlRepoError("Runnable target must resolve to a Uri datum.")
            sub_ref = None
            if value.sub is not None:
                sub_ref = self._store_scratch_value(value.sub, txn)
                if sub_ref.ns() != "datum-runnable":
                    raise DmlRepoError("Runnable sub must resolve to a Runnable datum.")
            kwargs_ref = self._store_scratch_value(value.kwargs, txn)
            if kwargs_ref.ns() != "datum-dict":
                raise DmlRepoError("Runnable kwargs must resolve to a Dict datum.")
            return txn.put(RunnableDatum(target=target_ref, sub=sub_ref, kwargs=kwargs_ref, adapter=value.adapter))
        if isinstance(value, set):
            raise DmlRepoError("Set literals are not supported.")
        if isinstance(value, tuple):
            value = list(value)
        if isinstance(value, list):
            return txn.put(ListDatum([self._store_scratch_value(v, txn) for v in value]))
        if isinstance(value, dict):
            return txn.put(DictDatum(data={k: self._store_scratch_value(v, txn) for k, v in value.items()}))
        return txn.put(ScalarDatum(value))

    def _build_scratch_dag_in_txn(self, argv_ref: Ref, txn, *, result: Any = None, error: Error | None = None) -> Ref:
        nodes = [argv_ref, self._kwargv_from_argv(argv_ref, txn)]
        dag = Dag(nodes=nodes, names={}, result=None, argv=argv_ref)
        if error is not None:
            dag.error = txn.put(error)
        else:
            result_ref = txn.put(LiteralNode(value=self._store_scratch_value(result, txn)))
            dag.nodes = sorted({result_ref, *dag.nodes})
            dag.result = result_ref
        dag_ref = txn.put(dag)
        tree_ref = txn.put(Tree(dags={}))
        txn.put(Commit(message="", dag=dag_ref, parents=[], tree=tree_ref, author="DaggerML User"))
        return dag_ref
