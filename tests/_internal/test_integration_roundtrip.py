"""Integration tests for round-tripping Python values through IndexOps/NodeOps."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from hypothesis import given, settings
from hypothesis import strategies as st

from daggerml._internal._db import Ref
from daggerml._internal.ops.head import HeadOps
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.ops.node import NodeOps
from daggerml._internal.types import DictDatum, ListDatum, Runnable, RunnableDatum, ScalarDatum, Uri

TEST_FN_ADAPTER = str(Path(__file__).resolve().parent / "fn" / "python-fork-adapter.py")


def _remote_root_from_env() -> str:
    return os.environ["DML_REMOTE_ROOT"]


@dataclass(frozen=True)
class RunnableSpec:
    target: object  # str | RunnableSpec
    kwargs: dict[str, object]
    adapter: str


def _scalar_strategy():
    return st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-(2**31), max_value=2**31 - 1),
        st.floats(allow_nan=False, allow_infinity=False, width=32),
        st.text(max_size=16),
    )


@st.composite
def _value_spec(draw, depth: int = 0):
    if depth >= 3:
        return draw(_scalar_strategy())
    kind = draw(st.sampled_from(["scalar", "list", "dict", "runnable"]))
    if kind == "scalar":
        return draw(_scalar_strategy())
    if kind == "list":
        return draw(st.lists(_value_spec(depth=depth + 1), max_size=4))
    if kind == "dict":
        return draw(st.dictionaries(st.text(min_size=1, max_size=8), _value_spec(depth=depth + 1), max_size=4))

    # runnable
    nested_target = False
    target = (
        draw(_value_spec(depth=depth + 1).filter(lambda x: isinstance(x, RunnableSpec)))
        if nested_target
        else draw(
            st.one_of(
                st.sampled_from(["daggerml:list", "daggerml:dict", "daggerml:get", "file:///tmp/fn.py"]),
                st.text(min_size=1, max_size=24),
            )
        )
    )
    kwargs = draw(st.dictionaries(st.text(min_size=1, max_size=6), _value_spec(depth=depth + 1), max_size=3))
    return RunnableSpec(
        target=target,
        kwargs=kwargs,
        adapter=draw(st.sampled_from(["", TEST_FN_ADAPTER, "custom-adapter"])),
    )


def _build_target_ref(spec_target: object, ops: IndexOps, index_ref: Ref) -> Ref:
    if isinstance(spec_target, RunnableSpec):
        nested = _materialize(spec_target, ops, index_ref)
        nested_node = ops.put_literal(index_ref, nested)
        with ops._tx(readonly=True) as txn:
            return txn.get(nested_node).datum_ref(txn)

    uri_node = ops.put_literal(index_ref, Uri(str(spec_target)))
    with ops._tx(readonly=True) as txn:
        return txn.get(uri_node).datum_ref(txn)


def _materialize(value: object, ops: IndexOps, index_ref: Ref):
    if isinstance(value, RunnableSpec):
        target_ref = _build_target_ref(value.target, ops, index_ref)
        kwargs: dict[str, Ref] = {}
        for k, v in value.kwargs.items():
            vv = _materialize(v, ops, index_ref)
            node_ref = ops.put_literal(index_ref, vv)
            with ops._tx(readonly=True) as txn:
                kwargs[k] = txn.get(node_ref).datum_ref(txn)
        kwargs_node_ref = ops.put_literal(index_ref, kwargs)
        with ops._tx(readonly=True) as txn:
            kwargs_ref = txn.get(kwargs_node_ref).datum_ref(txn)
        return RunnableDatum(target=target_ref, sub=None, kwargs=kwargs_ref, adapter=value.adapter)
    if isinstance(value, list):
        return [_materialize(v, ops, index_ref) for v in value]
    if isinstance(value, dict):
        return {k: _materialize(v, ops, index_ref) for k, v in value.items()}
    return value


def _canonical_from_ref(txn, ref: Ref):
    if ref.nss()[0] == "node":
        node = txn.get(ref)
        return _canonical_from_ref(txn, node.datum_ref(txn))

    datum = txn.get(ref)
    if isinstance(datum, ScalarDatum):
        return datum.data
    if isinstance(datum, ListDatum):
        return [_canonical_from_ref(txn, x) for x in datum.data]
    if isinstance(datum, DictDatum):
        return {k: _canonical_from_ref(txn, v) for k, v in datum.data.items()}
    if isinstance(datum, Uri):
        return {"__uri__": datum.uri}
    if isinstance(datum, RunnableDatum):
        kwargs_datum: DictDatum = txn.get(datum.kwargs)
        return {
            "__runnable__": {
                "adapter": datum.adapter,
                "target": _canonical_from_ref(txn, datum.target),
                "kwargs": {k: _canonical_from_ref(txn, v) for k, v in kwargs_datum.data.items()},
            }
        }
    raise AssertionError(f"Unsupported datum type: {type(datum).__name__}")


def _canonical_value(txn, value):
    if isinstance(value, Ref):
        return _canonical_from_ref(txn, value)
    if isinstance(value, RunnableDatum):
        kwargs_datum: DictDatum = txn.get(value.kwargs)
        return {
            "__runnable__": {
                "adapter": value.adapter,
                "target": _canonical_from_ref(txn, value.target),
                "kwargs": {k: _canonical_from_ref(txn, v) for k, v in kwargs_datum.data.items()},
            }
        }
    if isinstance(value, Runnable):
        return {
            "__runnable__": {
                "adapter": value.adapter,
                "target": _canonical_value(txn, value.target),
                "kwargs": {k: _canonical_value(txn, v) for k, v in value.kwargs.items()},
            }
        }
    if isinstance(value, Uri):
        return {"__uri__": value.uri}
    if isinstance(value, list):
        return [_canonical_value(txn, x) for x in value]
    if isinstance(value, dict):
        return {k: _canonical_value(txn, v) for k, v in value.items()}
    return value


@given(payload=_value_spec())
@settings(max_examples=30, deadline=None)
def test_put_literal_unroll_roundtrip_with_nested_runnables(temp_bo, payload):
    head_ops = HeadOps(_db=temp_bo._db)
    index_ops = IndexOps(_db=temp_bo._db, remote_root=_remote_root_from_env())
    node_ops = NodeOps(_db=temp_bo._db)

    head_ref = head_ops.create(f"rt-{uuid4().hex}")
    index_ref = index_ops.create(head=head_ref)
    try:
        materialized = _materialize(payload, index_ops, index_ref)
        root_ref = index_ops.put_literal(index_ref, materialized, name="root")
        result = node_ops.unroll(root_ref)
        with index_ops._tx(readonly=True) as txn:
            assert _canonical_value(txn, result) == _canonical_value(txn, materialized)
    finally:
        index_ops.delete(index_ref)
        head_ops.delete(head_ref)
