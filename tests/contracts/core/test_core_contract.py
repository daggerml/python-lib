import os
from pathlib import Path
from typing import cast
from unittest import TestCase

import pytest

from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.api import Dag, DictNode, Dml, Error, ListNode, Node

SUM_URI = "./tests/assets/fns/sum.py"
ASYNC_URI = "./tests/assets/fns/async.py"
TIMEOUT_URI = "./tests/assets/fns/timeout.py"
TEST_FN_DIR = Path(__file__).resolve().parents[2] / "assets" / "internal_fn"
FN_ADAPTER = str(TEST_FN_DIR / "python-fork-adapter.py")


class TestSetAttrs:
    def _mk_runnable(self, dml, uri: str, adapter: str, defaults: dict | None = None) -> Runnable:
        return Runnable(target=Uri(uri), kwargs=defaults or {}, adapter=adapter)

    @pytest.mark.parametrize("x", [[0], (0,), [], ["asdf", None]])  # none contain 1
    def test_list_attrs(self, x, dml):
        dag = dml.new("d0", "d0")
        n0 = dag.put(x)
        assert n0.contains(1).value() is False
        assert 1 not in n0
        assert len(n0) == len(x)
        for index, item_node in enumerate(n0):
            item = x[index]
            assert item_node.value() == item
            assert n0.contains(item).value() is True
            assert item in n0
            assert n0[index].value() == item
        assert n0.append(1).value() == [*x, 1]
        assert n0.conj(1).value() == [*x, 1]

    @pytest.mark.parametrize("x", [{}, {"a": 1}, {"x": 42, "y": {"k0": None}}])  # none contain 'z'
    def test_dict_attrs(self, x, dml):
        dag = dml.new("d0", "d0")
        n0 = dag.put(x)
        assert n0.contains("z").value() is False
        assert "z" not in n0
        assert len(n0) == len(x)
        assert n0.get("z", default=123).value() == 123
        for key in n0:
            item = x[key]
            assert n0[key].value() == item
            assert n0.contains(key).value() is True
            assert key in n0
            assert n0.get(key).value() == item
        assert [(k, v.value()) for k, v in n0.items()] == list(x.items())
        assert n0.keys() == list(x.keys())
        assert [x.value() for x in n0.values()] == list(x.values())
        assert n0.assoc("y", 3).value() == {**x, "y": 3}
        assert n0.update({"z": 1, "a": 2}).value() == {**x, "z": 1, "a": 2}

    def test_load_reboot(self, dml):
        with dml.new("d0", "d0") as dag:
            dag.put(42, name="n0")
            dag.commit("foo")
        with dml.new("d1", "d1") as dag:
            node = dag.load("d0", name="n1")
            assert node.dag == dag
            assert node.value() == "foo"
            assert node.load()["n0"].value() == 42
            assert dag.load("d0", key="n0").value() == 42

    def test_put_node_from_other_dag_auto_imports(self, dml):
        with dml.new("src", "src") as src:
            src.put(99, name="n0")
            src.commit(src.n0)

        foreign_node = dml.load("src")["n0"]
        with dml.new("dst", "dst") as dst:
            imported = dst.put(foreign_node, name="imported")
            assert imported.value() == 99
            dst.commit(imported)

    def test_node_call_w_literal_deps(self, dml):
        nums = [1, 2, 3]
        dag = dml.new("d0", "d0")
        fn = self._mk_runnable(dml, SUM_URI, FN_ADAPTER, defaults={"x": 10})
        result = dag.call(fn, *nums)
        assert result.value() == sum(nums)
        assert "x" in result.load().keys()
        assert result.load()["x"].value() == 10

    def test_node_call_w_node_deps(self, dml):
        nums = [1, 2, 3]
        dag = dml.new("d0", "d0")
        fn = self._mk_runnable(dml, SUM_URI, FN_ADAPTER, defaults={"x": dag.put(10)})
        result = dag.call(fn, *nums)
        assert result.value() == sum(nums)
        assert "x" in result.load().keys()
        assert result.load()["x"].value() == 10

    def test_node_call_w_kwarg(self, dml):
        nums = [1, 2, 3]
        dag = dml.new("d0", "d0")
        fn = self._mk_runnable(dml, SUM_URI, FN_ADAPTER, defaults={"x": 10})
        result = dag.call(fn, *nums, x=100)
        assert result.value() == sum(nums)
        assert "x" in result.load().keys()
        assert result.load()["x"].value() == 100

    def test_bad_kwarg(self, dml):
        nums = [1, 2, 3]
        dag = dml.new("d0", "d0")
        fn = self._mk_runnable(dml, SUM_URI, FN_ADAPTER, defaults={"x": 10})
        with pytest.raises(DmlRepoError, match=r"Unknown kwarg: y"):
            dag.call(fn, *nums, y=100)

    def test_node_call(self, dml):
        nums = [1, 2, 3]
        dag = dml.new("d0", "d0")
        fn = dag.put(self._mk_runnable(dml, SUM_URI, FN_ADAPTER))
        result = fn(*nums)
        assert result.value() == sum(nums)

    def test_node_call_runnable(self, dml):
        nums = [1, 2, 3]
        dag = dml.new("d0", "d0")
        fn = self._mk_runnable(dml, SUM_URI, FN_ADAPTER)
        result = dag.call(fn, *nums)
        assert result.value() == sum(nums)

    def test_load_recursing(self, dml):
        nums = [1, 2, 3]
        with dml.new("d0", "d0") as dag:
            dag.commit(dag.call(self._mk_runnable(dml, SUM_URI, FN_ADAPTER), *nums, name="n1"))
        d1 = dml.new("d1", "d1")
        n1 = d1.load(dml.load("d0")["n1"], name="n1_1")
        assert n1.dag == d1
        n2 = n1.load()["n1"].load()["num_args"]
        assert n2.value() == len(nums)
        assert n1.value() == sum(nums)

    def test_no_caching(self):
        nums = [1, 2, 3]
        with Dml.temporary() as dml:
            with dml.new("d0", "d0") as d1:
                n1 = d1.call(self._mk_runnable(dml, SUM_URI, FN_ADAPTER), *nums)
                uid = n1.load()["uuid"].value()
        with Dml.temporary() as dml:
            with dml.new("d1", "d0") as d1:
                n1 = d1.call(self._mk_runnable(dml, SUM_URI, FN_ADAPTER), *nums)
                uid1 = n1.load()["uuid"].value()
        assert uid == uid1, "Cached dag should have the same UUID"

    def test_nodemap(self, dml):
        dag = dml.new("d0", "d0")
        dag.a = 23
        node = dag.put(42, name="b")
        other = dag.put(420)
        assert dag["a"].value() == 23
        assert list(dag) == ["a", "b"]
        dag.commit([node, other])

    def test_set_attrs(self, dml):
        dag = dml.new("d0", "d0")
        with pytest.raises(DmlRepoError, match="Set literals are not supported"):
            dag.put({0})

    def test_load_constructors(self, dml):
        dag = dml.new("d0", "d0")
        l0 = dag.put(42)
        c0 = dag.put({"a": 1, "b": [l0, "23"]})
        assert c0["b"][0] != l0
        with pytest.raises(NotImplementedError, match="temporarily disabled"):
            c0.backtrack("b", 0)

    def test_fn_ok_cache(self, dml):
        with dml.new("d0", "d0") as dag:
            nodes = [dag.call(self._mk_runnable(dml, SUM_URI, FN_ADAPTER), i, 1, 2) for i in range(2)]
            # Add a repeat outside so `nodes` remains unique.
            dag.call(self._mk_runnable(dml, SUM_URI, FN_ADAPTER), 0, 1, 2)
            dag.commit(nodes[0])
        assert dag.result.value() == 3

    def test_dag_cache_requires_commit(self, dml):
        dag = dml.new("d0", "d0")
        with pytest.raises(DmlRepoError, match="committed"):
            dag.cache()

    def test_dag_cache_publishes_function_dag(self, dml):
        with dml.new("d0", "d0") as dag:
            result = dag.call(self._mk_runnable(dml, SUM_URI, FN_ADAPTER), 1, 2, 3)
            dag.commit(result)
        fn_dag = result.load()
        assert isinstance(fn_dag.cache(), str)

    def test_async_fn_ok(self, dml):
        debug_file = os.path.join(dml.repo, "debug")
        with dml.new("d0", "d0") as dag:
            n1 = dag.call(self._mk_runnable(dml, ASYNC_URI, FN_ADAPTER), 1, 2, 3)
            dag.commit(n1)
        assert n1.value() == 6
        with open(debug_file, "r") as f:
            assert len([1 for _ in f]) == 2

    def test_async_fn_error(self, dml):
        with pytest.raises(Error, match=r".*unsupported operand type.*"):
            with dml.new("d0", "d0") as dag:
                dag.call(self._mk_runnable(dml, ASYNC_URI, FN_ADAPTER), 1, 2, "asdf")
        commit_ref = dml.head.get_branch_commit(cast(str, dml.branch))
        assert dml.commit.get_dag(commit_ref, "d0") is not None

    def test_async_fn_timeout(self, dml):
        with pytest.raises(TimeoutError):
            with dml.new("d0", "d0") as dag:
                dag.call(self._mk_runnable(dml, TIMEOUT_URI, FN_ADAPTER), 1, 2, 3, timeout=1000)

    def test_load(self, dml):
        with dml.new("d0", "d0") as dag:
            dag.put(42, name="n0")
            dag.commit("foo")
        dl = dml.load("d0")
        assert isinstance(dl, Dag)
        assert dl["n0"].value() == 42
        assert dl.result.value() == "foo"

    def test_put_node_uses_node_codec(self, dml):
        dag = dml.new("d0", "d0")
        original = dag.put(42, name="n0")
        alias = dag.put(original, name="n1")
        assert alias.ref == original.ref
        assert dag["n1"].value() == 42


class TestBasic(TestCase):
    def test_dag_named_node_access_roundtrip(self):
        with Dml.temporary() as dml:
            d0 = dml.new("d0", "d0")
            self.assertIsInstance(d0, Dag)
            n0 = d0.put([42], name="n0")
            self.assertIsInstance(n0, Node)
            self.assertEqual(n0.value(), [42])
            assert len(d0) == 1
            self.assertEqual(len(n0), 1)
            self.assertEqual(n0.type, "list")
            d0["x0"] = n0
            self.assertEqual(d0["x0"], n0)
            self.assertEqual(d0.x0, n0)
            d0.x1 = 42
            self.assertEqual(d0["x1"].value(), 42)
            self.assertEqual(d0.x1.value(), 42)

    def test_dag_collection_materialization_roundtrip(self):
        with Dml.temporary() as dml:
            d0 = dml.new("d0", "d0")
            n0 = d0.put([42], name="n0")
            d0.x2 = 99
            self.assertEqual(d0.x2.value(), 99)
            d0.x3 = 100
            self.assertEqual(d0.x3.value(), 100)
            d0.n1 = n0[0]
            self.assertIsInstance(n0[0], Node)
            self.assertEqual([x.value() for x in n0], [d0.n1.value()])
            self.assertEqual(d0.n1.value(), 42)
            d0.n2 = {"x": n0, "y": "z"}
            n2 = cast(DictNode, d0.load("d0", "n2"))
            self.assertNotEqual(n2["x"], n0)
            self.assertEqual(n2["x"].value(), n0.value())
            d0.n3 = list(n2.items())
            self.assertIsInstance([x for x in d0.n3], list)
            self.assertDictEqual(
                {k: v.value() for k, v in n2.items()},
                {"x": n0.value(), "y": "z"},
            )
            d0.n4 = [1, 2, 3, 4, 5]
            d0.n5 = cast(ListNode, d0.load("d0", "n4"))[1:]
            self.assertListEqual([x.value() for x in d0.n5], [2, 3, 4, 5])

    def test_dag_commit_result_and_delete_then_gc(self):
        with Dml.temporary() as dml:
            d0 = dml.new("d0", "d0")
            n0 = d0.put([42], name="n0")
            d0.commit(n0)
            commit_ref = dml.head.get_branch_commit(cast(str, dml.branch))
            dag_ref = dml.commit.get_dag(commit_ref, "d0")
            assert dag_ref is not None
            self.assertEqual(dml.dag.describe(dag_ref)["result"], n0.ref)
            dml.commit.delete_dag("d0", cast(str, dml.branch), dml.user or "dml")
            dml.ops.gc().gc()
