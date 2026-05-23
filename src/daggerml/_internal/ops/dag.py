"""DAG operations for managing directed acyclic graphs.

Public API:
    DagOps - Class for DAG-related operations
"""

from dataclasses import dataclass

from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.ops.cache import CacheOps
from daggerml._internal.types import Dag, DmlRepoError, KwargvNode


@dataclass
class DagOps(BaseOps):
    """Operations for listing and describing DAGs stored in the repository."""

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

    def describe(self, dag_ref: Ref) -> dict:
        """Get DAG attributes, topology, and id as a dict."""
        if dag_ref.ns() != "dag":
            raise DmlRepoError(f"Expected dag ref, got: {dag_ref}")
        with self._tx(readonly=True) as txn:
            dag = txn.get(dag_ref)
            kwargv_ref = self._kwargv_ref_from_nodes(dag, txn)
            cache_key = CacheOps.get_cache_key(dag.argv, txn) if dag.argv is not None else None
        return {
            "nodes": dag.nodes,
            "names": dag.names,
            "result": dag.result,
            "argv": dag.argv,
            "kwargv": kwargv_ref,
            "cache_key": cache_key,
        }

    def get_node(self, dag_ref: Ref, name: str) -> Ref:
        if dag_ref.ns() != "dag":
            raise DmlRepoError(f"Expected dag ref, got: {dag_ref}")
        with self._tx(readonly=True) as txn:
            dag = txn.get(dag_ref)
            if dag is None:
                raise DmlRepoError(f"Object not found: {dag_ref}")
            # Ensure DAG is finished before allowing named node lookup
            if not dag.is_finished():
                raise DmlRepoError("Cannot get node from unfinished DAG")
            if name not in dag.names:
                raise DmlRepoError(f"Node '{name}' not found in DAG")
            return dag.names[name]

    def get_argv(self, dag_ref: Ref) -> Ref:
        if dag_ref.ns() != "dag":
            raise DmlRepoError(f"Expected dag ref, got: {dag_ref}")
        with self._tx(readonly=True) as txn:
            dag = txn.get(dag_ref)
            if dag is None:
                raise DmlRepoError(f"Object not found: {dag_ref}")
            if dag.argv is None:
                raise DmlRepoError("DAG has no argv node")
            return dag.argv

    def get_kwargv(self, dag_ref: Ref) -> Ref:
        """Return the kwargv Ref for a DAG."""
        if dag_ref.ns() != "dag":
            raise DmlRepoError(f"Expected dag ref, got: {dag_ref}")
        with self._tx(readonly=True) as txn:
            dag = txn.get(dag_ref)
            if dag is None:
                raise DmlRepoError(f"Object not found: {dag_ref}")
            kwargv_ref = self._kwargv_ref_from_nodes(dag, txn)
            if kwargv_ref is None:
                raise DmlRepoError("DAG has no kwargv node")
            return kwargv_ref
