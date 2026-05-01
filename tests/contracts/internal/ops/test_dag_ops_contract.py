import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from daggerml._internal._db import Ref
from daggerml._internal.ops.dag import DagOps
from daggerml._internal.types import (
    ArgvNode,
    Commit,
    Dag,
    DictDatum,
    DmlRepoError,
    Head,
    KwargvNode,
    ListDatum,
    LiteralNode,
    ScalarDatum,
    Tree,
)
from tests.contracts.internal.support.test_db_support import REF_ALPHABET, _gen_ref
from tests.contracts.internal.test_types_contract import _dag_strategy, _refs


def _put_dag(temp_bo, dag, data) -> Ref:
    ref = data.draw(_refs("dag"))
    with temp_bo._tx(readonly=False) as txn:
        txn.put(dag, to=ref)
    return ref


def setup(temp_bo, dag, data):
    dag_ref = dag and _put_dag(temp_bo, dag, data)
    # Create a fake commit/tree context
    tree_ref = data.draw(_refs("tree"))
    commit_ref = data.draw(_refs("commit"))
    head_ref = data.draw(_refs("head"))
    with temp_bo._tx(readonly=False) as txn:
        txn.put(Tree(dags={"main": dag_ref} if dag_ref else {}), to=tree_ref)
        txn.put(Commit(parents=[], tree=tree_ref, author="test", message="test commit"), to=commit_ref)
        txn.put(Head(commit=commit_ref), to=head_ref)
    return dag_ref, tree_ref, commit_ref, head_ref


class TestDagOps:
    def test_list_and_describe(self, temp_bo):
        with temp_bo._tx(readonly=False) as txn:
            datum_ref = txn.put(ScalarDatum(data=1))
            node_ref = txn.put(LiteralNode(value=datum_ref))
            argv_datum_ref = txn.put(ListDatum(data=[]))
            argv_node_ref = txn.put(ArgvNode(value=argv_datum_ref))
            kwargv_datum_ref = txn.put(DictDatum(data={}))
            kwargv_node_ref = txn.put(KwargvNode(value=kwargv_datum_ref))
            dag = Dag(
                nodes=[node_ref, argv_node_ref, kwargv_node_ref],
                names={"result": node_ref},
                result=node_ref,
                argv=argv_node_ref,
            )
            dag_ref = txn.put(dag)
            tree_ref = txn.put(Tree(dags={"main": dag_ref}))
            commit_ref = txn.put(Commit(parents=[], tree=tree_ref, author="test", message="test commit"))
            head_ref = txn.put(Head(commit=commit_ref))
        refs = dag_ref, tree_ref, commit_ref, head_ref
        ops = DagOps(temp_bo._db)
        # List DAGs in this commit
        dags = ops.list()
        assert isinstance(dags, list)
        assert any(d["id"] == dag_ref.id() for d in dags)
        # Describe the DAG
        desc = ops.describe(dag_ref)
        assert desc["id"] == dag_ref.id()
        assert desc["nodes"] == dag.nodes
        assert desc["names"] == dag.names
        assert desc["result"] == dag.result
        assert desc["argv"] == dag.argv
        # Clean up
        with temp_bo._tx(readonly=False) as txn:
            for ref in set(refs):
                if ref:
                    txn.delete(ref)

    def test_list_empty(self, temp_bo):
        ops = DagOps(temp_bo._db)
        # No heads/commits
        assert ops.list() == []

    @pytest.mark.parametrize(
        "arg,msg",
        [
            (_gen_ref("node"), "Expected dag ref"),
            (_gen_ref("dag"), r"Object not found: Ref\(dag"),  # non-existent dag ref
        ],
    )
    def test_describe_invalid_ref_raises(self, temp_bo, arg, msg):
        """describe() should raise ValueError when given a non-dag Ref."""
        ops = DagOps(temp_bo._db)
        with pytest.raises(DmlRepoError, match=msg):
            ops.describe(arg)

    def test_describe_missing_dag_raises(self, temp_bo):
        """describe() should raise DmlRepoError when the dag ref does not exist."""
        ops = DagOps(temp_bo._db)
        missing = _gen_ref("dag")
        with pytest.raises(DmlRepoError):
            ops.describe(missing)

    @given(_dag_strategy().filter(lambda d: bool(d.nodes) and bool(d.names) and d.result is not None), st.data())
    @settings(max_examples=10)
    def test_get_node_happy_path(self, temp_bo, dag, data):
        """get_node should return the named node for a finished DAG."""
        dag_ref, tree_ref, commit_ref, head_ref = setup(temp_bo, dag, data)
        try:
            ops = DagOps(temp_bo._db)
            name = next(iter(dag.names))
            node_ref = ops.get_node(dag_ref, name)
            assert node_ref == dag.names[name]
        finally:
            with temp_bo._tx(readonly=False) as txn:
                for ref in {dag_ref, tree_ref, commit_ref, head_ref}:
                    if ref:
                        txn.delete(ref)

    @given(_dag_strategy().filter(lambda d: d.is_finished()), st.text(alphabet=REF_ALPHABET, min_size=1, max_size=16))
    def test_get_node_not_found_raises(self, temp_bo, dag, name):
        """get_node should raise if the named node is not present."""
        assume(name not in dag.names)
        with temp_bo._tx(readonly=False) as txn:
            dag_ref = txn.put(dag)
        try:
            ops = DagOps(temp_bo._db)
            with pytest.raises(DmlRepoError):
                ops.get_node(dag_ref, name)
        finally:
            with temp_bo._tx(readonly=False) as txn:
                txn.delete(dag_ref)

    @given(_dag_strategy().filter(lambda d: (not d.is_finished()) and bool(d.nodes)))
    def test_get_node_unfinished_raises(self, temp_bo, dag):
        """get_node should raise if the DAG is not finished."""
        ops = DagOps(temp_bo._db)
        # insert the unfinished DAG directly; we don't need a commit/head context
        with temp_bo._tx(readonly=False) as txn:
            dag_ref = txn.put(dag)
        try:
            with pytest.raises(DmlRepoError):
                ops.get_node(dag_ref, "missing_name")
        finally:
            with temp_bo._tx(readonly=False) as txn:
                txn.delete(dag_ref)

    def test_get_argv_happy_path(self, temp_bo):
        """get_argv should return the argv node when present."""
        # create a dag with argv present and insert it directly
        with temp_bo._tx(readonly=False) as txn:
            argv_datum_ref = txn.put(ListDatum(data=[]))
            argv_node_ref = txn.put(ArgvNode(value=argv_datum_ref))
            kwargv_datum_ref = txn.put(DictDatum(data={}))
            kwargv_node_ref = txn.put(KwargvNode(value=kwargv_datum_ref))
            dag_ref = txn.put(Dag(nodes=[argv_node_ref, kwargv_node_ref], names={}, result=None, argv=argv_node_ref))
        try:
            ops = DagOps(temp_bo._db)
            assert ops.get_argv(dag_ref) == argv_node_ref
        finally:
            with temp_bo._tx(readonly=False) as txn:
                for r in (dag_ref, argv_node_ref, kwargv_node_ref, argv_datum_ref, kwargv_datum_ref):
                    if r:
                        txn.delete(r)

    def test_get_argv_missing_raises(self, temp_bo):
        """get_argv should raise when the DAG has no argv node."""
        # insert a dag without argv
        with temp_bo._tx(readonly=False) as txn:
            dag_ref = txn.put(Dag(nodes=[], names={}, result=None, argv=None))
        try:
            ops = DagOps(temp_bo._db)
            with pytest.raises(DmlRepoError, match="DAG has no argv node"):
                ops.get_argv(dag_ref)
        finally:
            with temp_bo._tx(readonly=False) as txn:
                txn.delete(dag_ref)
