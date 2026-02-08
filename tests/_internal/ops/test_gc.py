import pytest
from hypothesis import given, settings

from daggerml._internal.ops.gc import GcOps
from daggerml._internal.types import Commit, Dag, DmlRepoError, Head, LiteralNode, ScalarDatum, Tree
from tests._internal.test__db import _gen_ref
from tests._internal.test_types import _head_strategy


class TestGcOps:
    def test_gc_and_list_orphans(self, temp_bo):
        try:
            with temp_bo._tx(readonly=False) as txn:
                datum_ref = txn.put(ScalarDatum(data=42))
                node_ref = txn.put(LiteralNode(value=datum_ref))
                dag_ref = txn.put(Dag([node_ref], {}, node_ref))
            # datum_ref = _put_datum(temp_bo, 42)
            tree = Tree(dags={"main": dag_ref})
            tree_ref = _gen_ref("tree")
            commit_ref = _gen_ref("commit")
            head_ref = _gen_ref("head")
            with temp_bo._tx(readonly=False) as txn:
                txn.put(tree, to=tree_ref)
                txn.put(Commit(parents=[], tree=tree_ref, author="test", message="test commit"), to=commit_ref)
                txn.put(Head(commit=commit_ref), to=head_ref)
            ops = GcOps(temp_bo._db)
            assert ops.list_orphans() == []
            with temp_bo._tx(readonly=False) as txn:
                new_datum_ref = txn.put(ScalarDatum(data="orphan datum"))
            assert ops.list_orphans() == [new_datum_ref]
            stats = ops.gc()
            assert "datum-scalar" in stats and stats["datum-scalar"] == 1
            assert ops.list_orphans() == []
        finally:
            temp_bo._db.clear_all()

    @given(_head_strategy())
    @settings(max_examples=1)
    def test_gc_error(self, temp_bo, head):
        try:
            with temp_bo._tx(readonly=False) as txn:
                txn.put(head)
            ops = GcOps(temp_bo._db)
            with pytest.raises(DmlRepoError, match="^GC failed: Failed to list orphans: .*"):
                ops.gc()  # should raise because head points to non-existent commit
        finally:
            temp_bo._db.clear_all()

    def test_gc_empty(self, temp_bo):
        ops = GcOps(temp_bo._db)
        assert ops.list_orphans() == []
        assert ops.gc() == {}
