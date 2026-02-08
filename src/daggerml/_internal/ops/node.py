"""Node operations for retrieving and inspecting DAG nodes.

This module provides NodeOps, a small helper subsystem for working with node
objects in the repository. It can retrieve a node's value one-layer deep, or
fully unroll nested Datum references into plain Python values.

Public API:
    NodeOps - Class for node inspection operations
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.types import (
    Datum,
    DictDatum,
    DmlRepoError,
    FnNode,
    ImportNode,
    ListDatum,
    Node,
    Runnable,
    RunnableDatum,
    ScalarDatum,
    Uri,
)


@dataclass
class NodeOps(BaseOps):
    """Operations for retrieving and inspecting node values."""

    def _require_node_ref(self, node_ref: Ref) -> Ref:
        if not isinstance(node_ref, Ref):
            raise DmlRepoError(f"Expected Ref, got: {type(node_ref).__name__}")
        if node_ref.nss()[0] != "node":
            raise DmlRepoError(f"Expected node ref, got: {node_ref}")
        return node_ref

    def _unroll_datum_ref(self, ref: Ref, txn, *, _stack: set[Ref] | None = None) -> Any:
        if ref.ns() == "error":
            raise DmlRepoError("Cannot unroll error value.")
        if ref.nss()[0] != "datum":
            raise DmlRepoError(f"Expected datum ref, got: {ref}")

        stack = _stack if _stack is not None else set()
        if ref in stack:
            raise DmlRepoError(f"Cycle detected while unrolling datum: {ref}")

        stack.add(ref)
        try:
            datum: Datum = txn.get(ref)
            if isinstance(datum, ScalarDatum):
                return datum.data
            if isinstance(datum, ListDatum):
                return [self._unroll_datum_ref(x, txn, _stack=stack) for x in datum.data]
            if isinstance(datum, DictDatum):
                return {k: self._unroll_datum_ref(v, txn, _stack=stack) for k, v in datum.data.items()}
            if isinstance(datum, Uri):
                return datum
            if isinstance(datum, RunnableDatum):
                target = self._unroll_datum_ref(datum.target, txn, _stack=stack)
                kwargs_datum: DictDatum = txn.get(datum.kwargs)
                kwargs = {k: self._unroll_datum_ref(v, txn, _stack=stack) for k, v in kwargs_datum.data.items()}
                sub = None
                if datum.sub is not None:
                    sub_obj = self._unroll_datum_ref(datum.sub, txn, _stack=stack)
                    if not isinstance(sub_obj, Runnable):
                        raise DmlRepoError(f"Runnable sub must unroll to Runnable, got {type(sub_obj).__name__}")
                    sub = sub_obj
                if not isinstance(target, Uri):
                    raise DmlRepoError(f"Runnable target must unroll to Uri, got {type(target).__name__}")
                return Runnable(target=target, sub=sub, kwargs=kwargs, adapter=datum.adapter)
            raise DmlRepoError(f"Unsupported datum type: {type(datum).__name__}")
        finally:
            stack.remove(ref)

    def get(self, node_ref: Ref) -> Any:
        """Retrieve node value/content one layer deep (refs preserved in collections)."""
        try:
            node_ref = self._require_node_ref(node_ref)
            with self._tx(readonly=True) as txn:
                node: Node = txn.get(node_ref)
                value_ref = node.datum_ref(txn)
                datum: Datum = txn.get(value_ref)
                if isinstance(datum, ScalarDatum):
                    return datum.data
                if isinstance(datum, ListDatum):
                    return list(datum.data)
                if isinstance(datum, DictDatum):
                    return dict(datum.data)
                if isinstance(datum, Uri):
                    return datum
                if isinstance(datum, RunnableDatum):
                    return self._unroll_datum_ref(value_ref, txn)
                raise DmlRepoError(f"Unsupported datum type: {type(datum).__name__}")
        except Exception as e:
            raise DmlRepoError(f"Failed to get node value: {e}") from e

    def unroll(self, node_ref: Ref) -> Any:
        """Fully realize Python object without any datum refs."""
        try:
            node_ref = self._require_node_ref(node_ref)
            with self._tx(readonly=True) as txn:
                node: Node = txn.get(node_ref)
                value_ref = node.datum_ref(txn)
                return self._unroll_datum_ref(value_ref, txn)
        except Exception as e:
            raise DmlRepoError(f"Failed to unroll node value: {e}") from e

    def describe(self, node_ref: Ref) -> dict[str, Any]:
        """Describe a node with stable metadata fields."""
        try:
            node_ref = self._require_node_ref(node_ref)
            with self._tx(readonly=True) as txn:
                node: Node = txn.get(node_ref)
                payload: dict[str, Any] = {
                    "id": node_ref.id(),
                    "ref": node_ref,
                    "type": type(node).__name__,
                    "value_ref": node.datum_ref(txn),
                }
                if isinstance(node, FnNode):
                    payload["dag"] = node.dag
                    payload["argv"] = list(node.argv)
                if isinstance(node, ImportNode):
                    payload["dag"] = node.dag
                    payload["node"] = node.node
                return payload
        except Exception as e:
            raise DmlRepoError(f"Failed to describe node: {e}") from e
