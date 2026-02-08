"""DAG operations for managing directed acyclic graphs.

Public API:
    DagOps - Class for DAG-related operations
"""

from dataclasses import dataclass
from typing import Any

from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
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

    def list(self) -> list[dict[str, Any]]:
        """List all DAGs

        Returns a list of dicts describing each DAG (id, names, result, argv).
        """
        with self._tx(readonly=True) as txn:
            out = []
            for dag_ref in txn.iter("dag"):
                dag = txn.get(dag_ref)
                argv_ref = dag.argv if dag is not None else None
                kwargv_ref = self._kwargv_ref_from_nodes(dag, txn) if dag is not None else None
                out.append(
                    {
                        "name": dag_ref.id(),
                        "id": dag_ref.id(),
                        "nodes": dag.nodes,
                        "names": dag.names,
                        "result": dag.result,
                        "argv": argv_ref,
                        "kwargv": kwargv_ref,
                    }
                )
        return out

    def describe(self, dag_ref: Ref) -> dict:
        """Get DAG attributes, topology, and id as a dict."""
        if dag_ref.ns() != "dag":
            raise DmlRepoError(f"Expected dag ref, got: {dag_ref}")
        with self._tx(readonly=True) as txn:
            dag = txn.get(dag_ref)
            kwargv_ref = self._kwargv_ref_from_nodes(dag, txn)
        return {
            "id": dag_ref.id(),
            "nodes": dag.nodes,
            "names": dag.names,
            "result": dag.result,
            "argv": dag.argv,
            "kwargv": kwargv_ref,
        }

    def get_node(self, dag_ref: Ref, name: str) -> Ref:
        """Return the Ref of a named node in a finished DAG.

        Parameters
        ----------
        dag_ref : Ref
            Reference to the DAG to query (must be namespace 'dag').
        name : str
            The name of the node to look up in the DAG's `names` mapping.

        Returns
        -------
        Ref
            Reference to the node associated with `name`.

        Raises
        ------
        DmlRepoError
            If `dag_ref` is not a dag Ref, the DAG is not present or not
            finished (has no result), or the named node does not exist.
        """
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
        """Return the argv Ref for a DAG.

        Parameters
        ----------
        dag_ref : Ref
            Reference to the DAG to query (must be namespace 'dag').

        Returns
        -------
        Ref
            Reference to the argv node for the DAG.

        Raises
        ------
        DmlRepoError
            If `dag_ref` is not a dag Ref, the DAG is not present, or the DAG
            has no argv node.
        """
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
