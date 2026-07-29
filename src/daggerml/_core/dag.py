"""Read-only queries on DAGs."""

from typing import TypedDict, cast

try:
    from typing import NotRequired
except ImportError:
    from typing_extensions import NotRequired

from daggerml._core.db import Ref
from daggerml._core.types import Dag, DmlDB, DmlRepoError, FnNode, ImportNode, TxnWithValid


class DagDescription(TypedDict):
    id: Ref
    nodes: list[Ref]
    names: dict[str, Ref]
    error: Ref | None
    result: Ref | None
    argv: Ref | None
    cache_key: str | None


class NodeDescriptionPayload(TypedDict):
    id: Ref
    type: str
    dag: NotRequired[Ref]
    argv: NotRequired[list[Ref]]
    node: NotRequired[Ref]


class DagOps:
    def describe(self, dag_ref: Ref, *, db: DmlDB) -> DagDescription:
        with db.tx(readonly=True) as txn:
            dag: Dag = txn.get(txn.require(dag_ref, "dag"))
            return cast(
                DagDescription,
                {
                    "id": dag_ref,
                    "nodes": dag.nodes,
                    "names": dag.names,
                    "error": dag.error,
                    "result": dag.result,
                    "argv": dag.argv,
                    "cache_key": dag.cache_key(txn) if dag.argv is not None else None,
                },
            )

    def describe_node(self, node_ref: Ref, *, db: DmlDB) -> NodeDescriptionPayload:
        with db.tx(readonly=True) as txn:
            node = txn.get(txn.require(node_ref, "node"))
        resp = {"id": node_ref, "type": type(node).__name__}
        if isinstance(node, ImportNode):
            resp.update({"dag": node.dag, "node": node.node})
        elif isinstance(node, FnNode):
            resp.update({"dag": node.dag, "argv": node.argv})
        return cast(NodeDescriptionPayload, resp)

    def get_node(self, dag_ref: Ref, name: str, *, db: DmlDB) -> Ref:
        with db.tx(readonly=True) as txn:
            dag: Dag = txn.get(TxnWithValid.require(dag_ref, "dag"))
            if not dag.is_finished():
                raise DmlRepoError("Cannot get node from unfinished DAG")
            if name not in dag.names:
                raise DmlRepoError(f"Node '{name}' not found in DAG")
            return dag.names[name]

    def get_argv(self, dag_ref: Ref, *, db: DmlDB) -> Ref:
        with db.tx(readonly=True) as txn:
            dag: Dag = txn.get(TxnWithValid.require(dag_ref, "dag"))
            if dag.argv is None:
                raise DmlRepoError("DAG is not a function application and has no argv")
            return dag.argv
