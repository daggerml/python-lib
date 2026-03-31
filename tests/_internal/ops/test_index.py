import json
import os
from unittest.mock import patch
from uuid import uuid4

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import daggerml._internal.codec as literal_codec
from daggerml._internal._db import Ref
from daggerml._internal.ops.index import IndexOps
from daggerml._internal.ops.node import NodeOps
from daggerml._internal.types import (
    ArgvNode,
    Commit,
    Dag,
    DictDatum,
    DmlRepoError,
    Error,
    FnNode,
    Head,
    ImportNode,
    Index,
    KwargvNode,
    ListDatum,
    LiteralNode,
    Runnable,
    RunnableDatum,
    ScalarDatum,
    Tree,
    Uri,
)
from tests._internal.conftest import remote_bucket_and_prefix_from_env
from tests._internal.test__db import REF_ALPHABET, _gen_ref, float_strategy, int_strategy, scalar_strategy
from tests._internal.test_types import _index_strategy
from tests._internal.util import TEST_DIR

_NAME_STRAT = st.text(alphabet=REF_ALPHABET, min_size=1, max_size=12)
DELAYED_FN_URI = str(TEST_DIR / "fn/delayed-sum.py")
PREPOP_FN_URI = str(TEST_DIR / "fn/prepop.py")
ERROR_FN_URI = str(TEST_DIR / "fn/adapter-error.py")
RAND_FN_URI = str(TEST_DIR / "fn/rand.py")
SUM_FN_URI = str(TEST_DIR / "fn/sum.py")
FN_ADAPTER = str(TEST_DIR / "fn/python-fork-adapter.py")


def _remote_root_from_env() -> str:
    return os.environ["DML_REMOTE_ROOT"]


def _remote_protocol_prefix_from_env() -> str:
    _bucket, prefix = remote_bucket_and_prefix_from_env()
    return f"{prefix}/dml" if prefix else "dml"


def _mk_repo_state(temp_bo, *, with_argv: bool = False) -> tuple[IndexOps, Ref, Ref]:
    """Create a minimal head + working index context for IndexOps tests."""
    head_ref = _gen_ref("head")
    tree_ref = _gen_ref("tree")
    base_commit_ref = _gen_ref("commit")
    index_dag_ref = _gen_ref("dag")
    index_commit_ref = _gen_ref("commit")
    index_ref = _gen_ref("index")
    with temp_bo._tx(readonly=False) as txn:
        txn.put(Tree(dags={}), to=tree_ref)
        txn.put(Commit(parents=[], tree=tree_ref, author="test", message="base"), to=base_commit_ref)
        txn.put(Head(commit=base_commit_ref), to=head_ref)
        nodes: list[Ref] = []
        argv_node_ref: Ref | None = None
        if with_argv:
            argv_datum_ref = txn.put(ListDatum(data=[]), to=_gen_ref("datum-list"))
            argv_node_ref = txn.put(ArgvNode(value=argv_datum_ref), to=_gen_ref("node", "argv"))
            kwargv_datum_ref = txn.put(DictDatum(data={}), to=_gen_ref("datum-dict"))
            kwargv_node_ref = txn.put(KwargvNode(value=kwargv_datum_ref), to=_gen_ref("node", "kwargv"))
            nodes = [argv_node_ref, kwargv_node_ref]
        txn.put(Dag(nodes=nodes, names={}, result=None, argv=(argv_node_ref if with_argv else None)), to=index_dag_ref)
        txn.put(
            Commit(
                parents=[base_commit_ref],
                tree=tree_ref,
                author="test",
                message="working",
                dag=index_dag_ref,
            ),
            to=index_commit_ref,
        )
        txn.put(Index(commit=index_commit_ref), to=index_ref)
    return IndexOps(_db=temp_bo._db), head_ref, index_ref


def _mk_remote_index_ops(temp_bo, *, cache: str | None = None) -> IndexOps:
    return IndexOps(
        _db=temp_bo._db,
        remote_root=_remote_root_from_env(),
        remote_cache=cache or f"cache-{uuid4().hex}",
    )


def _unroll_datum(txn, ref: Ref):
    datum = txn.get(ref)
    if isinstance(datum, ListDatum):
        return [_unroll_datum(txn, x) if isinstance(x, Ref) else x for x in datum.data]
    if isinstance(datum, DictDatum):
        return {k: _unroll_datum(txn, v) if isinstance(v, Ref) else v for k, v in datum.data.items()}
    if isinstance(datum, ScalarDatum):
        return datum.data
    raise AssertionError(f"Unexpected datum type: {type(datum).__name__}")


def _put_runnable_literal(ops: IndexOps, index_ref: Ref, *, uri: str, adapter: str) -> Ref:
    uri_node = ops.put_literal(index_ref, Uri(uri))
    defaults_node = ops.put_literal(index_ref, {})
    with ops._tx(readonly=True) as txn:
        uri_ref = txn.get(uri_node).datum_ref(txn)
        defaults_ref = txn.get(defaults_node).datum_ref(txn)
    return ops.put_literal(index_ref, RunnableDatum(target=uri_ref, sub=None, kwargs=defaults_ref, adapter=adapter))


class TestIndexOps:
    @given(_index_strategy())
    @settings(max_examples=10)
    def test_list(self, temp_bo, idx):
        """List returns existing refs; delete removes them."""
        ops = IndexOps(_db=temp_bo._db)
        with temp_bo._tx(readonly=False) as txn:
            ref = txn.put(idx)
        try:
            assert ref in list(ops.list())
        finally:
            ops.delete(ref)
        assert ref not in list(ops.list())

    @pytest.mark.parametrize(
        "builtin,args,expected",
        [
            ("list", [1, 2], [1, 2]),
            ("dict", ["a", 1, "b", 2], {"a": 1, "b": 2}),
            ("get", [{"a": 1}, "a"], 1),
            ("get", [{"a": 1}, "b", 9], 9),
            ("contains", [{"a": 1, "b": 2}, "a"], True),
            ("contains", [{"a": 1, "b": 2}, "c"], False),
            ("contains", [[{"a": 1}, {"b": 2}], {"a": 1}], True),
            ("contains", [[{"a": 1}, {"b": 2}], {"a": 2}], False),
            ("assoc", [{"a": 1, "b": 2}, "c", 3], {"a": 1, "b": 2, "c": 3}),
            ("assoc", [{"a": 1, "b": 2}, "a", 9], {"a": 9, "b": 2}),
            ("conj", [[1, 2], 3], [1, 2, 3]),
            ("unnest", [[[1], [2, 3], [4, [5]]]], [1, 2, 3, 4, [5]]),
        ],
    )
    def test_start_fn_builtins(self, temp_bo, builtin, args, expected):
        ops, _head_ref, index = _mk_repo_state(temp_bo)
        try:
            fn_node = _put_runnable_literal(ops, index, uri=f"daggerml:{builtin}", adapter="")
            arg_nodes = [ops.put_literal(index, arg) for arg in args]
            result = ops.start_fn(index, [fn_node, *arg_nodes])
            nv = NodeOps(_db=temp_bo._db).unroll(result)
            assert nv == expected
        finally:
            ops.delete(index)

    @given(args=st.lists(st.one_of(int_strategy(), float_strategy()), min_size=1, max_size=5))
    @settings(max_examples=10, deadline=2000, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_start_fn_sum(self, temp_bo, args, s3):
        temp_bo._db.clear_all()
        _ops, _head_ref, index = _mk_repo_state(temp_bo)
        ops = _mk_remote_index_ops(temp_bo)
        try:
            fn_node = _put_runnable_literal(ops, index, uri=SUM_FN_URI, adapter=FN_ADAPTER)
            node_args = [fn_node, *[ops.put_literal(index, arg) for arg in args]]
            result = ops.start_fn(index, node_args)
            nv = NodeOps(_db=temp_bo._db).unroll(result)
            assert nv == pytest.approx(sum(args))
        finally:
            ops.delete(index)

    def test_put_literal_dict_fn(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        n0 = ops.put_literal(index_ref, 0, name="v0")
        ops.put_literal(index_ref, {"a": n0}, name="v1")
        nops = NodeOps(_db=temp_bo._db)
        with ops._tx(readonly=True) as txn:
            dag: Dag = txn.get_ctx(index_ref).dag
        vals = [nops.unroll(v) for v in dag.nodes]
        vals = [str(v) if isinstance(v, dict) else v for v in vals]
        vals = [x for x in vals if not isinstance(x, (Uri, Runnable))]
        assert {0, "a", "{'a': 0}"} == set(vals)

    def test_put_literal_list_fn(self, temp_bo):
        temp_bo._db.clear_all()
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        n0 = ops.put_literal(index_ref, 0, name="v0")
        ops.put_literal(index_ref, [1, n0], name="v1")
        nops = NodeOps(_db=temp_bo._db)
        with ops._tx(readonly=True) as txn:
            dag: Dag = txn.get_ctx(index_ref).dag
        vals = [nops.unroll(v) for v in dag.nodes]
        vals = [tuple(v) if isinstance(v, list) else v for v in vals]
        vals = [x for x in vals if not isinstance(x, (Uri, Runnable))]
        assert {0, 1, (1, 0)} == set(vals)

    def test_put_literal_runnable_fn_when_attrs_are_nodes(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        default_value_node = ops.put_literal(index_ref, 10)
        runnable_node = ops.put_literal(
            index_ref,
            Runnable(target=Uri("daggerml:get"), kwargs={"x": default_value_node}, adapter=""),
            name="rf",
        )
        with ops._tx(readonly=True) as txn:
            node = txn.get(runnable_node)
            datum = txn.get(node.datum_ref(txn))
            assert isinstance(datum, RunnableDatum)
            assert datum.target.ns() == "datum-uri"
            assert datum.kwargs.ns() == "datum-dict"

    def test_start_fn_sum_err(self, temp_bo, s3):
        _ops, _head_ref, index = _mk_repo_state(temp_bo)
        ops = _mk_remote_index_ops(temp_bo)
        args = [1, 2, 3, "BOGUS", 5]
        try:
            fn_node = _put_runnable_literal(ops, index, uri=SUM_FN_URI, adapter=FN_ADAPTER)
            node_args = [fn_node, *[ops.put_literal(index, arg) for arg in args]]
            with pytest.raises(Error, match="Argument at index 3 is str, expected int or float"):
                ops.start_fn(index, node_args)
        finally:
            ops.delete(index)

    @given(
        args=st.lists(
            st.one_of(
                st.integers(min_value=-(2**63), max_value=2**63 - 1),
                st.floats(allow_nan=False, allow_infinity=False),
            ),
            max_size=6,
        )
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=2, deadline=None)
    def test_start_fn_sum_adapter(self, temp_bo, args, s3):
        total = float(sum(args))
        _ops, _head_ref, index = _mk_repo_state(temp_bo)
        ops = _mk_remote_index_ops(temp_bo)
        try:
            fn_node = _put_runnable_literal(ops, index, uri=SUM_FN_URI, adapter=FN_ADAPTER)
            arg_nodes = [ops.put_literal(index, arg) for arg in args]
            result = ops.start_fn(index, [fn_node, *arg_nodes], name="result")
            nv = NodeOps(_db=temp_bo._db).unroll(result)
            assert nv == pytest.approx(total)
        finally:
            ops.delete(index)

    @given(
        args=st.lists(
            st.one_of(
                st.integers(min_value=-(2**63), max_value=2**63 - 1),
                st.floats(allow_nan=False, allow_infinity=False),
            ),
            max_size=6,
        ),
        prepop=st.floats(allow_nan=False, allow_infinity=False),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=2, deadline=None)
    def test_start_fn_prepop(self, temp_bo, args, prepop, s3):
        temp_bo._db.clear_all()
        total = float(sum(args) * prepop)
        _ops, _head_ref, index = _mk_repo_state(temp_bo)
        ops = _mk_remote_index_ops(temp_bo)
        try:
            x_default = ops.put_literal(index, 1.0)
            fn_node = ops.put_literal(
                index,
                Runnable(target=Uri(PREPOP_FN_URI), kwargs={"x": x_default}, adapter=FN_ADAPTER),
            )
            arg_nodes = [ops.put_literal(index, arg) for arg in args]
            prepop_node = ops.put_literal(index, prepop)
            result = ops.start_fn(index, [fn_node, *arg_nodes], kwargv={"x": prepop_node}, name="result")
            nv = NodeOps(_db=temp_bo._db).unroll(result)
            assert nv == pytest.approx(total)
        finally:
            ops.delete(index)

    @given(
        args=st.lists(
            st.one_of(
                st.integers(min_value=-(2**63), max_value=2**63 - 1),
                st.floats(allow_nan=False, allow_infinity=False),
            ),
            max_size=6,
        )
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=2, deadline=None)
    def test_start_fn_delayed_sum_adapter(self, temp_bo, args, s3):
        total = float(sum(args))
        _ops, _head_ref, index = _mk_repo_state(temp_bo)
        ops = _mk_remote_index_ops(temp_bo)
        try:
            with patch.dict(__import__("os").environ, DML_TMP_DIR=ops._db.path):
                fn_node = _put_runnable_literal(ops, index, uri=DELAYED_FN_URI, adapter=FN_ADAPTER)
                arg_nodes = [ops.put_literal(index, arg) for arg in args]
                # First call returns None (job not done yet)
                result = ops.start_fn(index, [fn_node, *arg_nodes], name="result")
                assert result is None
                # Second call returns the result
                result = ops.start_fn(index, [fn_node, *arg_nodes], name="result")
                assert result is not None
                nv = NodeOps(_db=temp_bo._db).unroll(result)
                assert nv == pytest.approx(total)
        finally:
            ops.delete(index)

    def test_start_fn_adapter_nonzero_exit_raises(self, temp_bo, tmp_path, s3):
        _ops, _head_ref, index = _mk_repo_state(temp_bo)
        ops = _mk_remote_index_ops(temp_bo)
        failing_adapter = tmp_path / "adapter-fail.py"
        failing_adapter.write_text(
            ("import sys\nsys.stderr.write('boom from adapter\\n')\nraise SystemExit(1)\n"),
            encoding="utf-8",
        )
        os.chmod(failing_adapter, 0o755)
        try:
            fn_node = _put_runnable_literal(ops, index, uri="noop://fn", adapter=str(failing_adapter))
            with pytest.raises(DmlRepoError, match=r"Adapter call failed: .*boom from adapter"):
                ops.start_fn(index, [fn_node], name="result")
        finally:
            ops.delete(index)

    def test_start_fn_adapter_invalid_json_raises(self, temp_bo, tmp_path, s3):
        _ops, _head_ref, index = _mk_repo_state(temp_bo)
        ops = _mk_remote_index_ops(temp_bo)
        bad_json_adapter = tmp_path / "adapter-bad-json.py"
        bad_json_adapter.write_text(
            ("print('not-json')\nraise SystemExit(0)\n"),
            encoding="utf-8",
        )
        os.chmod(bad_json_adapter, 0o755)
        try:
            fn_node = _put_runnable_literal(ops, index, uri="noop://fn", adapter=str(bad_json_adapter))
            with pytest.raises(DmlRepoError, match="Adapter output must be JSON"):
                ops.start_fn(index, [fn_node], name="result")
        finally:
            ops.delete(index)

    def test_start_fn_runnable_sub_cycle_raises(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            with ops._tx(readonly=False) as txn:
                uri_ref = txn.put(Uri("noop://fn"), to=_gen_ref("datum-uri"))
                kwargs_ref = txn.put(DictDatum(data={}), to=_gen_ref("datum-dict"))
                runnable_ref = _gen_ref("datum-runnable")
                txn.put(
                    RunnableDatum(target=uri_ref, sub=runnable_ref, kwargs=kwargs_ref, adapter="nonempty"),
                    to=runnable_ref,
                )
                fn_node_ref = ops._put_node(LiteralNode(value=runnable_ref), txn=txn, index_ref=index_ref)
            with pytest.raises(DmlRepoError, match="Runnable sub cycle detected"):
                ops.start_fn(index_ref, [fn_node_ref], name="result")
        finally:
            ops.delete(index_ref)

    def test_start_fn_caching(self, temp_bo, s3):
        # ensure clean DB for this test to prevent map growth from prior tests
        temp_bo._db.clear_all()
        _ops, _head_ref, index = _mk_repo_state(temp_bo)
        ops = IndexOps(_db=temp_bo._db, remote_root=_remote_root_from_env(), remote_cache=f"cache-{uuid4().hex}")
        try:
            fn_node = _put_runnable_literal(ops, index, uri=RAND_FN_URI, adapter=FN_ADAPTER)
            # First call generates a random UUID
            result1 = ops.start_fn(index, [fn_node], name="result1")
            nv1 = NodeOps(_db=temp_bo._db).unroll(result1)
            # Second call with same args should return cached result
            result2 = ops.start_fn(index, [fn_node], name="result2")
            nv2 = NodeOps(_db=temp_bo._db).unroll(result2)
            assert nv1 == nv2
            assert isinstance(nv1, str)
            assert len(nv1) == 36
            assert nv1.count("-") == 4
        finally:
            ops.delete(index)

    def test_start_fn_is_always_cached(self, temp_bo, s3):
        # ensure clean DB to avoid map growth between runs
        temp_bo._db.clear_all()
        _ops, _head_ref, index = _mk_repo_state(temp_bo)
        ops = _mk_remote_index_ops(temp_bo)
        try:
            fn_node = _put_runnable_literal(ops, index, uri=RAND_FN_URI, adapter=FN_ADAPTER)
            # First call generates a random UUID
            result1 = ops.start_fn(index, [fn_node], name="result1")
            nv1 = NodeOps(_db=temp_bo._db).unroll(result1)
            # Second call with same args should return cached UUID
            result2 = ops.start_fn(index, [fn_node], name="result2")
            nv2 = NodeOps(_db=temp_bo._db).unroll(result2)
            assert nv1 == nv2
            assert isinstance(nv1, str)
            assert len(nv1) == 36
            assert nv1.count("-") == 4
        finally:
            ops.delete(index)

    def test_start_fn_cache_key_includes_adapter(self, temp_bo, tmp_path, s3):
        # This should fail on the current bug: cache key does not include adapter identity.
        temp_bo._db.clear_all()
        _ops, _head_ref, index = _mk_repo_state(temp_bo)
        ops = _mk_remote_index_ops(temp_bo)
        try:
            alt_adapter = tmp_path / "python-fork-adapter-alt.sh"
            alt_adapter.write_text(
                "#!/usr/bin/env bash\n"
                "set -eu\n"
                'if [ -n "${PYTHON:-}" ]; then\n'
                f'    exec "${{PYTHON}}" "{FN_ADAPTER}"\n'
                "fi\n"
                "if command -v python3 >/dev/null 2>&1; then\n"
                f'    exec python3 "{FN_ADAPTER}"\n'
                "fi\n"
                f'exec python "{FN_ADAPTER}"\n',
                encoding="utf-8",
            )
            os.chmod(alt_adapter, 0o755)

            fn_node_path_adapter = _put_runnable_literal(ops, index, uri=RAND_FN_URI, adapter=str(alt_adapter))
            fn_node_default_adapter = _put_runnable_literal(ops, index, uri=RAND_FN_URI, adapter=FN_ADAPTER)

            result1 = ops.start_fn(index, [fn_node_path_adapter], name="result_path_adapter")
            result2 = ops.start_fn(index, [fn_node_default_adapter], name="result_default_adapter")
            nv1 = NodeOps(_db=temp_bo._db).unroll(result1)
            nv2 = NodeOps(_db=temp_bo._db).unroll(result2)

            assert nv1 != nv2
            assert isinstance(nv1, str)
            assert isinstance(nv2, str)
            assert len(nv1) == 36
            assert nv1.count("-") == 4
            assert len(nv2) == 36
            assert nv2.count("-") == 4
        finally:
            ops.delete(index)

    @given(value=scalar_strategy(), name=_NAME_STRAT)
    @settings(max_examples=10)
    def test_put_literal(self, temp_bo, value, name):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            node_ref = ops.put_literal(index_ref, value, name=name)
            with ops._tx(readonly=True) as txn:
                ctx = txn.get_ctx(index_ref)
                assert node_ref in ctx.dag.nodes
                assert ctx.dag.names[name] == node_ref
                node = txn.get(node_ref)
                assert isinstance(node, LiteralNode)
                assert _unroll_datum(txn, node.value) == value
        finally:
            ops.delete(index_ref)

    def test_literal_codecs_apply_highest_priority_match(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        calls: list[str] = []

        class LowCodec:
            def can_encode(self, value):
                calls.append("low:can")
                return isinstance(value, str)

            def encode(self, value, ctx):
                calls.append("low:encode")
                return "low"

        class HighCodec:
            def can_encode(self, value):
                calls.append("high:can")
                return isinstance(value, str)

            def encode(self, value, ctx):
                calls.append("high:encode")
                return "high"

        old_codecs = literal_codec._literal_codecs.copy()
        old_seq = literal_codec._literal_codec_seq
        old_plugins_loaded = literal_codec._plugins_loaded
        try:
            literal_codec._literal_codecs = []
            literal_codec._literal_codec_seq = 0
            literal_codec._plugins_loaded = True
            literal_codec.register_codec(LowCodec(), priority=1)
            literal_codec.register_codec(HighCodec(), priority=10)
            node_ref = ops.put_literal(index_ref, "input")
            assert NodeOps(_db=temp_bo._db).unroll(node_ref) == "high"
            assert calls == ["high:can", "high:encode", "high:can", "high:encode"]
        finally:
            literal_codec._literal_codecs = old_codecs
            literal_codec._literal_codec_seq = old_seq
            literal_codec._plugins_loaded = old_plugins_loaded
            ops.delete(index_ref)

    def test_literal_codecs_short_circuit_first_match(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        calls: list[str] = []

        class FirstCodec:
            def can_encode(self, value):
                calls.append("first:can")
                return isinstance(value, str)

            def encode(self, value, ctx):
                calls.append("first:encode")
                return "first"

        class SecondCodec:
            def can_encode(self, value):
                calls.append("second:can")
                return isinstance(value, str)

            def encode(self, value, ctx):
                calls.append("second:encode")
                return "second"

        old_codecs = literal_codec._literal_codecs.copy()
        old_seq = literal_codec._literal_codec_seq
        old_plugins_loaded = literal_codec._plugins_loaded
        try:
            literal_codec._literal_codecs = []
            literal_codec._literal_codec_seq = 0
            literal_codec._plugins_loaded = True
            literal_codec.register_codec(FirstCodec(), priority=0)
            literal_codec.register_codec(SecondCodec(), priority=0)
            node_ref = ops.put_literal(index_ref, "input")
            assert NodeOps(_db=temp_bo._db).unroll(node_ref) == "first"
            assert calls == ["first:can", "first:encode", "first:can", "first:encode"]
        finally:
            literal_codec._literal_codecs = old_codecs
            literal_codec._literal_codec_seq = old_seq
            literal_codec._plugins_loaded = old_plugins_loaded
            ops.delete(index_ref)

    def test_literal_codecs_reencode_with_new_match(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        calls: list[str] = []

        class IntCodec:
            def can_encode(self, value):
                calls.append("int:can")
                return isinstance(value, int)

            def encode(self, value, ctx):
                calls.append("int:encode")
                return {"wrapped": f"v{value}"}

        class DictCodec:
            def can_encode(self, value):
                calls.append("dict:can")
                return isinstance(value, dict) and "wrapped" in value

            def encode(self, value, ctx):
                calls.append("dict:encode")
                return f"wrapped={value['wrapped']}"

        old_codecs = literal_codec._literal_codecs.copy()
        old_seq = literal_codec._literal_codec_seq
        old_plugins_loaded = literal_codec._plugins_loaded
        try:
            literal_codec._literal_codecs = []
            literal_codec._literal_codec_seq = 0
            literal_codec._plugins_loaded = True
            literal_codec.register_codec(IntCodec(), priority=10)
            literal_codec.register_codec(DictCodec(), priority=0)
            node_ref = ops.put_literal(index_ref, 7)
            assert NodeOps(_db=temp_bo._db).unroll(node_ref) == "wrapped=v7"
            assert calls.count("int:encode") == 1
            assert calls.count("dict:encode") == 1
        finally:
            literal_codec._literal_codecs = old_codecs
            literal_codec._literal_codec_seq = old_seq
            literal_codec._plugins_loaded = old_plugins_loaded
            ops.delete(index_ref)

    def test_literal_codecs_non_convergent_recursion_fails(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)

        class FlappingCodec:
            def can_encode(self, value):
                return isinstance(value, str) and value in {"left", "right"}

            def encode(self, value, ctx):
                return "right" if value == "left" else "left"

        old_codecs = literal_codec._literal_codecs.copy()
        old_seq = literal_codec._literal_codec_seq
        old_plugins_loaded = literal_codec._plugins_loaded
        old_max_reencodes = literal_codec._literal_codec_max_reencodes
        try:
            literal_codec._literal_codecs = []
            literal_codec._literal_codec_seq = 0
            literal_codec._plugins_loaded = True
            literal_codec._literal_codec_max_reencodes = 4
            literal_codec.register_codec(FlappingCodec(), priority=0)
            with pytest.raises(DmlRepoError, match=r"Literal codec recursion failed to converge"):
                ops.put_literal(index_ref, "left")
        finally:
            literal_codec._literal_codecs = old_codecs
            literal_codec._literal_codec_seq = old_seq
            literal_codec._plugins_loaded = old_plugins_loaded
            literal_codec._literal_codec_max_reencodes = old_max_reencodes
            ops.delete(index_ref)

    def test_literal_codec_failure_is_wrapped(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)

        class FailingCodec:
            def can_encode(self, value):
                return isinstance(value, str)

            def encode(self, value, ctx):
                raise ValueError("boom")

        old_codecs = literal_codec._literal_codecs.copy()
        old_seq = literal_codec._literal_codec_seq
        old_plugins_loaded = literal_codec._plugins_loaded
        try:
            literal_codec._literal_codecs = []
            literal_codec._literal_codec_seq = 0
            literal_codec._plugins_loaded = True
            literal_codec.register_codec(FailingCodec(), priority=0)
            with pytest.raises(DmlRepoError, match=r"Literal codec FailingCodec failed: boom"):
                ops.put_literal(index_ref, "input")
        finally:
            literal_codec._literal_codecs = old_codecs
            literal_codec._literal_codec_seq = old_seq
            literal_codec._plugins_loaded = old_plugins_loaded
            ops.delete(index_ref)

    def test_literal_codecs_traverse_codec_returned_collection(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)

        class SeedCodec:
            def can_encode(self, value):
                return value == "seed"

            def encode(self, value, ctx):
                return [1, 2]

        class IntCodec:
            def can_encode(self, value):
                return isinstance(value, int) and value < 10

            def encode(self, value, ctx):
                return value + 10

        old_codecs = literal_codec._literal_codecs.copy()
        old_seq = literal_codec._literal_codec_seq
        old_plugins_loaded = literal_codec._plugins_loaded
        try:
            literal_codec._literal_codecs = []
            literal_codec._literal_codec_seq = 0
            literal_codec._plugins_loaded = True
            literal_codec.register_codec(SeedCodec(), priority=10)
            literal_codec.register_codec(IntCodec(), priority=0)
            node_ref = ops.put_literal(index_ref, "seed")
            assert NodeOps(_db=temp_bo._db).unroll(node_ref) == [11, 12]
        finally:
            literal_codec._literal_codecs = old_codecs
            literal_codec._literal_codec_seq = old_seq
            literal_codec._plugins_loaded = old_plugins_loaded
            ops.delete(index_ref)

    def test_literal_codecs_traverse_codec_returned_runnable(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)

        class SeedCodec:
            def can_encode(self, value):
                return value == "seed"

            def encode(self, value, ctx):
                return Runnable(target=Uri("daggerml:list"), adapter="", kwargs={"x": 1}, sub=None)

        class IntCodec:
            def can_encode(self, value):
                return isinstance(value, int) and value == 1

            def encode(self, value, ctx):
                return value + 1

        old_codecs = literal_codec._literal_codecs.copy()
        old_seq = literal_codec._literal_codec_seq
        old_plugins_loaded = literal_codec._plugins_loaded
        try:
            literal_codec._literal_codecs = []
            literal_codec._literal_codec_seq = 0
            literal_codec._plugins_loaded = True
            literal_codec.register_codec(SeedCodec(), priority=10)
            literal_codec.register_codec(IntCodec(), priority=0)
            node_ref = ops.put_literal(index_ref, "seed")
            with ops._tx(readonly=True) as txn:
                lit: LiteralNode = txn.get(node_ref)
                encoded: RunnableDatum = txn.get(lit.value)
                target: Uri = txn.get(encoded.target)
                kwargs: DictDatum = txn.get(encoded.kwargs)
                x = txn.get(kwargs.data["x"])
                assert isinstance(target, Uri)
                assert target.uri == "daggerml:list"
                assert isinstance(x, ScalarDatum)
                assert x.data == 2
        finally:
            literal_codec._literal_codecs = old_codecs
            literal_codec._literal_codec_seq = old_seq
            literal_codec._plugins_loaded = old_plugins_loaded
            ops.delete(index_ref)

    def test_start_fn_applies_codec_to_argv_and_kwargv(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        fn_node = _put_runnable_literal(ops, index_ref, uri="daggerml:list", adapter="")
        arg_node = ops.put_literal(index_ref, 1)
        kw_node = ops.put_literal(index_ref, 2)

        with ops._tx(readonly=False) as txn:
            fn_lit = txn.get(fn_node)
            fn_runnable_ref = fn_lit.datum_ref(txn)
            runnable: RunnableDatum = txn.get(fn_runnable_ref)
            kwargs = txn.get(runnable.kwargs)
            kwargs.data["x"] = txn.put(ScalarDatum(0))
            runnable.kwargs = txn.put(kwargs)
            fn_lit.value = txn.put(runnable)
            txn.put(fn_lit, to=fn_node)

        seen: list[Ref] = []

        def _spy_apply(value, *, ctx):
            assert ctx.index_ref == index_ref
            assert ctx.index_ops is ops
            seen.append(value)
            return value

        with patch("daggerml._internal.ops.index.apply_codec", side_effect=_spy_apply):
            with pytest.raises(DmlRepoError, match="Keyword arguments are not supported"):
                ops.start_fn(index_ref, [fn_node, arg_node], kwargv={"x": kw_node})

        assert fn_node in seen
        assert arg_node in seen
        assert kw_node in seen

    def test_get_argv_raises_when_missing(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo, with_argv=False)
        try:
            with pytest.raises(DmlRepoError, match="DAG has no argv node"):
                ops.get_argv(index_ref)
        finally:
            ops.delete(index_ref)

    def test_get_argv_returns_node_when_present(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo, with_argv=True)
        try:
            argv_node_ref = ops.get_argv(index_ref)
            with ops._tx(readonly=True) as txn:
                node = txn.get(argv_node_ref)
                assert isinstance(node, ArgvNode)
        finally:
            ops.delete(index_ref)

    def test_get_kwargv_returns_node_when_present(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo, with_argv=True)
        try:
            kwargv_node_ref = ops.get_kwargv(index_ref)
            with ops._tx(readonly=True) as txn:
                node = txn.get(kwargv_node_ref)
                assert isinstance(node, KwargvNode)
        finally:
            ops.delete(index_ref)

    def test_start_fn_sub_runnable_forwards_and_resolves_kwargs(self, temp_bo, tmp_path, s3):
        temp_bo._db.clear_all()
        _ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        ops = _mk_remote_index_ops(temp_bo)
        outer_log = tmp_path / "outer-runnable.json"
        inner_log = tmp_path / "inner-runnable.json"
        inner_adapter = tmp_path / "inner-adapter.py"
        outer_adapter = tmp_path / "outer-adapter.py"

        inner_adapter.write_text(
            (
                "import json\n"
                "import subprocess\n"
                "import sys\n"
                "from urllib.parse import urlparse\n"
                f"LOG_PATH = {str(inner_log)!r}\n"
                "raw = sys.stdin.read()\n"
                "payload = json.loads(raw)\n"
                "with open(LOG_PATH, 'w', encoding='utf-8') as fh:\n"
                "    json.dump(payload.get('runnable', {}), fh, sort_keys=True)\n"
                "target = payload.get('runnable', {}).get('target', '')\n"
                "script = urlparse(target).path\n"
                "completed = subprocess.run(\n"
                "    [sys.executable, script],\n"
                "    input=raw,\n"
                "    text=True,\n"
                "    capture_output=True,\n"
                "    check=False,\n"
                ")\n"
                "sys.stdout.write(completed.stdout)\n"
                "sys.stderr.write(completed.stderr)\n"
                "raise SystemExit(completed.returncode)\n"
            ),
            encoding="utf-8",
        )
        outer_adapter.write_text(
            (
                "import json\n"
                "import shutil\n"
                "import subprocess\n"
                "import sys\n"
                f"LOG_PATH = {str(outer_log)!r}\n"
                "raw = sys.stdin.read()\n"
                "payload = json.loads(raw)\n"
                "runnable = payload.get('runnable', {})\n"
                "with open(LOG_PATH, 'w', encoding='utf-8') as fh:\n"
                "    json.dump(runnable, fh, sort_keys=True)\n"
                "sub = runnable.get('sub')\n"
                "if sub is None:\n"
                "    sys.stderr.write('missing sub runnable\\n')\n"
                "    raise SystemExit(1)\n"
                "adapter = sub.get('adapter', '')\n"
                "adapter_path = shutil.which(adapter) if '/' not in adapter else adapter\n"
                "if adapter_path is None:\n"
                "    sys.stderr.write(f'No such adapter: {adapter}\\n')\n"
                "    raise SystemExit(1)\n"
                "cmd = [adapter_path]\n"
                "if adapter_path.endswith('.py'):\n"
                "    cmd = [sys.executable, adapter_path]\n"
                "forwarded = {\n"
                "    'argv_ptr': payload.get('argv_ptr'),\n"
                "    'cache_key': payload.get('cache_key'),\n"
                "    'remote': payload.get('remote'),\n"
                "    'runnable': sub,\n"
                "}\n"
                "completed = subprocess.run(\n"
                "    cmd,\n"
                "    input=json.dumps(forwarded),\n"
                "    text=True,\n"
                "    capture_output=True,\n"
                "    check=False,\n"
                ")\n"
                "sys.stdout.write(completed.stdout)\n"
                "sys.stderr.write(completed.stderr)\n"
                "raise SystemExit(completed.returncode)\n"
            ),
            encoding="utf-8",
        )
        os.chmod(inner_adapter, 0o755)
        os.chmod(outer_adapter, 0o755)

        try:
            fn_node = ops.put_literal(
                index_ref,
                Runnable(
                    target=Uri("wrapper://outer"),
                    kwargs={"y": 1.0, "shared": 40.0},
                    adapter=str(outer_adapter),
                    sub=Runnable(
                        target=Uri(PREPOP_FN_URI),
                        kwargs={"x": 2.0, "shared": 20.0},
                        adapter=str(inner_adapter),
                    ),
                ),
            )
            arg_nodes = [ops.put_literal(index_ref, x) for x in [1.0, 2.0, 3.0]]
            result_ref = ops.start_fn(
                index_ref,
                [fn_node, *arg_nodes],
                kwargv={
                    "x": ops.put_literal(index_ref, 10.0),
                    "y": ops.put_literal(index_ref, 30.0),
                    "shared": ops.put_literal(index_ref, 99.0),
                },
                name="result",
            )
            assert result_ref is not None
            assert NodeOps(_db=temp_bo._db).unroll(result_ref) == pytest.approx(60.0)

            outer_runnable = json.loads(outer_log.read_text(encoding="utf-8"))
            inner_runnable = json.loads(inner_log.read_text(encoding="utf-8"))

            assert outer_runnable["target"] == "wrapper://outer"
            assert outer_runnable["kwargs"]["y"] == 30.0
            assert outer_runnable["kwargs"]["shared"] == 40.0
            assert outer_runnable["sub"]["target"] == PREPOP_FN_URI
            assert outer_runnable["sub"]["kwargs"]["x"] == 10.0
            assert outer_runnable["sub"]["kwargs"]["shared"] == 99.0

            assert inner_runnable["target"] == PREPOP_FN_URI
            assert inner_runnable["kwargs"]["x"] == 10.0
            assert inner_runnable["kwargs"]["shared"] == 99.0
        finally:
            ops.delete(index_ref)

    def test_start_fn_adapter_envelope_includes_remote_fields(self, temp_bo, tmp_path, s3):
        temp_bo._db.clear_all()
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        capture_adapter = tmp_path / "capture-adapter.py"
        payload_log = tmp_path / "payload.json"
        capture_adapter.write_text(
            (
                "import json\n"
                "import sys\n"
                f"LOG_PATH = {str(payload_log)!r}\n"
                "payload = json.loads(sys.stdin.read())\n"
                "with open(LOG_PATH, 'w', encoding='utf-8') as fh:\n"
                "    json.dump(payload, fh, sort_keys=True)\n"
                'print(\'{"status":"pending","error":null}\')\n'
            ),
            encoding="utf-8",
        )
        os.chmod(capture_adapter, 0o755)
        remote_ops = IndexOps(_db=temp_bo._db, remote_root=_remote_root_from_env(), remote_cache="cachetest")
        try:
            fn_node = remote_ops.put_literal(
                index_ref,
                Runnable(target=Uri("wrapper://capture"), kwargs={}, adapter=str(capture_adapter)),
            )
            result_ref = remote_ops.start_fn(index_ref, [fn_node])
            assert result_ref is None
            payload = json.loads(payload_log.read_text(encoding="utf-8"))
            assert isinstance(payload.get("cache_key"), str)
            assert len(payload["cache_key"]) == 64
            assert ":" not in payload["cache_key"]
            assert isinstance(payload.get("argv_ptr"), str)
            assert len(payload["argv_ptr"]) == 64
            assert payload["remote"] == {"root": _remote_root_from_env(), "cache": "cachetest"}
        finally:
            ops.delete(index_ref)

    def test_get_node_returns_named_node(self, temp_bo):
        """get_node returns the ref of a node that was stored with a name."""
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            node_ref = ops.put_literal(index_ref, 42, name="my_node")
            retrieved_ref = ops.get_node(index_ref, "my_node")
            assert retrieved_ref == node_ref
        finally:
            ops.delete(index_ref)

    def test_describe_returns_current_index_state(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            node_ref = ops.put_literal(index_ref, 42, name="answer")
            info = ops.describe(index_ref)
            assert info["dag"].ns() == "dag"
            assert node_ref in info["nodes"]
            assert info["names"]["answer"] == node_ref
        finally:
            ops.delete(index_ref)

    def test_set_node_name_updates_name_map(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            node_ref = ops.put_literal(index_ref, 42)
            ops.set_node_name(index_ref, "answer", node_ref)
            assert ops.get_node(index_ref, "answer") == node_ref
        finally:
            ops.delete(index_ref)

    def test_get_node_raises_when_name_not_found(self, temp_bo):
        """get_node raises DmlRepoError when the name doesn't exist in the DAG."""
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            with pytest.raises(DmlRepoError, match="Node 'nonexistent' not found in DAG"):
                ops.get_node(index_ref, "nonexistent")
        finally:
            ops.delete(index_ref)

    @given(value=scalar_strategy(), name=_NAME_STRAT)
    @settings(max_examples=10)
    def test_get_node_roundtrip(self, temp_bo, value, name):
        """get_node returns the same ref that was stored via put_literal with a name."""
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            node_ref = ops.put_literal(index_ref, value, name=name)
            retrieved_ref = ops.get_node(index_ref, name)
            assert retrieved_ref == node_ref
            # Verify the node contains the expected value
            with ops._tx(readonly=True) as txn:
                node = txn.get(retrieved_ref)
                assert isinstance(node, LiteralNode)
                assert _unroll_datum(txn, node.value) == value
        finally:
            ops.delete(index_ref)

    def test_get_node_with_multiple_named_nodes(self, temp_bo):
        """get_node correctly retrieves each named node when multiple exist."""
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            ref_a = ops.put_literal(index_ref, "value_a", name="node_a")
            ref_b = ops.put_literal(index_ref, "value_b", name="node_b")
            ref_c = ops.put_literal(index_ref, "value_c", name="node_c")

            assert ops.get_node(index_ref, "node_a") == ref_a
            assert ops.get_node(index_ref, "node_b") == ref_b
            assert ops.get_node(index_ref, "node_c") == ref_c
        finally:
            ops.delete(index_ref)

    def test_put_import_incomplete_dag_errors(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            with ops._tx(readonly=True) as txn:
                ctx = txn.get_ctx(index_ref)
                with pytest.raises(DmlRepoError, match="Cannot import from a DAG with no result node"):
                    ops.put_import(index_ref, ctx.commit.dag)
        finally:
            ops.delete(index_ref)

    def test_put_import_imports_other_dag_result(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            other_dag_ref = _gen_ref("dag")
            other_node_ref = _gen_ref("node", "literal")
            other_datum_ref = _gen_ref("datum-scalar")
            with ops._tx(readonly=False) as txn:
                txn.put(ScalarDatum(data=123), to=other_datum_ref)
                txn.put(LiteralNode(value=other_datum_ref), to=other_node_ref)
                txn.put(Dag(nodes=[other_node_ref], names={}, result=other_node_ref, argv=None), to=other_dag_ref)

            imported_ref = ops.put_import(index_ref, other_dag_ref, name="imported")
            with ops._tx(readonly=True) as txn:
                node = txn.get(imported_ref)
                assert isinstance(node, ImportNode)
                assert node.dag == other_dag_ref
                assert node.node == other_node_ref
                ctx = txn.get_ctx(index_ref)
                assert imported_ref in ctx.dag.nodes
        finally:
            ops.delete(index_ref)

    @given(a=scalar_strategy(), b=scalar_strategy())
    @settings(max_examples=10)
    def test_start_fn_builtin_list(self, temp_bo, a, b):
        # use a clean DB per Hypothesis example to avoid map growth
        temp_bo._db.clear_all()
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            fn_ref = _put_runnable_literal(ops, index_ref, uri="daggerml:list", adapter="")
            a_ref = ops.put_literal(index_ref, a)
            b_ref = ops.put_literal(index_ref, b)
            argv = [fn_ref, a_ref, b_ref]
            result_ref = ops.start_fn(index_ref, argv, name="result")
            assert result_ref is not None
            with ops._tx(readonly=True) as txn:
                node = txn.get(result_ref)
                assert isinstance(node, FnNode)
                assert node.argv == argv
                value_ref = txn.get(result_ref).datum_ref(txn)
                assert _unroll_datum(txn, value_ref) == [a, b]
        finally:
            ops.delete(index_ref)

    def test_start_fn_requires_runnable_first_arg(self, temp_bo):
        ops, _head_ref, index_ref = _mk_repo_state(temp_bo)
        non_runnable = ops.put_literal(index_ref, 123)
        with pytest.raises(DmlRepoError, match="First arg must resolve to a Runnable datum"):
            ops.start_fn(index_ref, [non_runnable])

    def test_create_argv_ptr_rejects_non_argv_root(self, temp_bo, s3):
        from daggerml._internal.ops.remote import RemoteOps

        ops, head_ref, index_ref = _mk_repo_state(temp_bo)
        remote_index_ops = IndexOps(_db=temp_bo._db, remote_root=_remote_root_from_env())
        try:
            literal_node = ops.put_literal(index_ref, 123)
            bucket, _prefix = remote_bucket_and_prefix_from_env()
            prefix = _remote_protocol_prefix_from_env()
            remote_ops = RemoteOps(_db=temp_bo._db, client=s3, bucket=bucket, prefix=prefix)
            bad_ptr = remote_ops.put_ref_manifest(literal_node)
            with pytest.raises(DmlRepoError, match="Manifest root namespace mismatch"):
                remote_index_ops.create(argv_ptr=bad_ptr)
        finally:
            ops.delete(index_ref)
            with ops._tx(readonly=False) as txn:
                txn.delete(head_ref)

    def test_create_validates_input_mode(self, temp_bo):
        ops, head_ref, index_ref = _mk_repo_state(temp_bo)
        try:
            with pytest.raises(DmlRepoError, match="Provide exactly one of head or argv_ptr."):
                ops.create()
            with pytest.raises(DmlRepoError, match="Provide exactly one of head or argv_ptr."):
                ops.create(head=head_ref, argv_ptr="a" * 64)
        finally:
            ops.delete(index_ref)
            with ops._tx(readonly=False) as txn:
                txn.delete(head_ref)

    def test_create_argv_ptr_requires_remote_context(self, temp_bo):
        ops = IndexOps(_db=temp_bo._db)
        with pytest.raises(DmlRepoError, match="Remote context required for argv_ptr"):
            ops.create(argv_ptr="a" * 64)

    def test_create_argv_ptr_loads_remote_argv(self, temp_bo, s3):
        from daggerml._internal.ops.remote import RemoteOps

        ops, head_ref, index_ref = _mk_repo_state(temp_bo)
        created_index = None
        try:
            fn_node = _put_runnable_literal(ops, index_ref, uri="daggerml:list", adapter="")
            arg_node = ops.put_literal(index_ref, 42)
            with ops._tx(readonly=False) as txn:
                argv_ref = ops._prepare_fn(index_ref, [fn_node, arg_node], {}, txn)

            bucket, _prefix = remote_bucket_and_prefix_from_env()
            prefix = _remote_protocol_prefix_from_env()
            remote_ops = RemoteOps(_db=temp_bo._db, client=s3, bucket=bucket, prefix=prefix)
            argv_ptr = remote_ops.put_ref_manifest(argv_ref)

            remote_index_ops = IndexOps(_db=temp_bo._db, remote_root=_remote_root_from_env())
            created_index = remote_index_ops.create(argv_ptr=argv_ptr)

            with remote_index_ops._tx(readonly=True) as txn:
                ctx = txn.get_ctx(created_index)
                assert ctx.dag is not None
                assert ctx.dag.argv == argv_ref
                kwargv_ref = remote_index_ops._kwargv_ref_from_nodes(ctx.dag, txn)
                assert kwargv_ref is not None
                assert kwargv_ref.ns() == "node-kwargv"
        finally:
            if created_index is not None:
                ops.delete(created_index)
            ops.delete(index_ref)
            with ops._tx(readonly=False) as txn:
                txn.delete(head_ref)

    @given(value=scalar_strategy(), dag_name=_NAME_STRAT)
    @settings(max_examples=10)
    def test_commit_deletes_index_and_updates_head(self, temp_bo, value, dag_name):
        ops, head_ref, index_ref = _mk_repo_state(temp_bo)
        with ops._tx(readonly=True) as txn:
            before = txn.get_ctx(head_ref).head.commit
        node_ref = ops.put_literal(index_ref, value, name="result")
        commit_ref = ops.commit(index_ref, node_ref, message="done", dag_name=dag_name, head=head_ref)

        with ops._tx(readonly=True) as txn:
            assert not txn.exists(index_ref)
            assert txn.get_ctx(head_ref).head.commit == commit_ref
            assert txn.get_ctx(head_ref).head.commit != before

            commit_obj = txn.get(commit_ref)
            assert isinstance(commit_obj, Commit)
            assert commit_obj.message == "done"
            tree_obj = txn.get(commit_obj.tree)
            assert isinstance(tree_obj, Tree)
            assert dag_name in tree_obj.dags
