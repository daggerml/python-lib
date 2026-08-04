"""Read-only queries on DAGs."""

from typing import Literal, TypedDict, cast

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


class _NodeDescription(TypedDict):
    id: Ref


class LiteralNodeDescription(_NodeDescription):
    type: Literal["LiteralNode", "ArgvNode"]


class ImportNodeDescription(_NodeDescription):
    type: Literal["ImportNode"]
    dag: Ref
    node: Ref


class FnNodeDescription(_NodeDescription):
    type: Literal["FnNode"]
    dag: Ref
    argv: list[Ref]


NodeDescriptionPayload = LiteralNodeDescription | ImportNodeDescription | FnNodeDescription


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
        if isinstance(node, ImportNode):
            return {"id": node_ref, "type": "ImportNode", "dag": node.dag, "node": node.node}
        if isinstance(node, FnNode):
            return {"id": node_ref, "type": "FnNode", "dag": node.dag, "argv": node.argv}
        return cast(LiteralNodeDescription, {"id": node_ref, "type": type(node).__name__})

    def get_node(self, dag_ref: Ref, name: str, *, db: DmlDB) -> Ref:
        with db.tx(readonly=True) as txn:
            dag: Dag = txn.get(TxnWithValid.require(dag_ref, "dag"))
            if name not in dag.names:
                raise DmlRepoError(f"Node '{name}' not found in DAG")
            return dag.names[name]

    def get_argv(self, dag_ref: Ref, *, db: DmlDB) -> Ref:
        with db.tx(readonly=True) as txn:
            dag: Dag = txn.get(TxnWithValid.require(dag_ref, "dag"))
            if dag.argv is None:
                raise DmlRepoError("DAG is not a function application and has no argv")
            return dag.argv
