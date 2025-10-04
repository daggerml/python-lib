import os
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

import pytest

from daggerml.core import Dag, Dml, Error, Executable, Node, from_data

SUM = Executable("./tests/assets/fns/sum.py", adapter="dml-python-fork-adapter")
ASYNC = Executable("./tests/assets/fns/async.py", adapter="dml-python-fork-adapter")
ENVVARS = Executable("./tests/assets/fns/envvars.py", adapter="dml-python-fork-adapter")
TIMEOUT = Executable("./tests/assets/fns/timeout.py", adapter="dml-python-fork-adapter")


class TestSetAttrs:
    @pytest.mark.parametrize("x", [[0], (0,), [], ["asdf", None]])  # none contain 1
    def test_list_attrs(self, x):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with dml.new("d0", "d0") as dag:
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
    def test_dict_attrs(self, x):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with dml.new("d0", "d0") as dag:
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

    def test_load_reboot(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with dml.new("d0", "d0") as dag:
                    dag.put(42, name="n0")
                    dag.commit("foo")
                with dml.new("d1", "d1") as dag:
                    node = dag.load("d0", name="n1")
                    assert node.dag == dag
                    assert node.value() == "foo"
                    assert node.load().n0.value() == 42
                    assert dag.load("d0", key="n0").value() == 42

    def test_node_call_w_literal_deps(self):
        nums = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=dml.kwargs["config_dir"]):
                    with dml.new("d0", "d0") as dag:
                        fn = Executable(
                            "./tests/assets/fns/sum.py",
                            adapter="dml-python-fork-adapter",
                            prepop={"x": 10},
                        )
                        result = dag.call(fn, *nums)
                        assert result.value() == sum(nums)
                        assert "x" in result.load().keys()
                        assert result.load().x.value() == 10

    def test_node_call_w_node_deps(self):
        nums = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=dml.kwargs["config_dir"]):
                    with dml.new("d0", "d0") as dag:
                        fn = Executable(
                            "./tests/assets/fns/sum.py",
                            adapter="dml-python-fork-adapter",
                            prepop={"x": dag.put(10)},
                        )
                        result = dag.call(fn, *nums)
                        assert result.value() == sum(nums)
                        assert "x" in result.load().keys()
                        assert result.load().x.value() == 10

    def test_node_call(self):
        nums = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=dml.kwargs["config_dir"]):
                    with dml.new("d0", "d0") as dag:
                        fn = dag.put(SUM)
                        result = fn(*nums)
                        assert result.value() == sum(nums)

    def test_load_recursing(self):
        nums = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=dml.kwargs["config_dir"]):
                    with dml.new("d0", "d0") as dag:
                        dag.commit(dag.call(SUM, *nums, name="n1"))
                d1 = dml.new("d1", "d1")
                n1 = d1.put(dml.load("d0").n1, name="n1_1")
                assert n1.dag == d1
                n2 = n1.load().n1.load().num_args
                assert n2.value() == len(nums)
                assert n1.value() == sum(nums)

    def test_caching(self):
        nums = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                config_dir = dml.config_dir
                with dml.new("d0", "d0") as d1:
                    n1 = d1.call(SUM, *nums)
                    assert n1.value() == sum(nums)
                    assert isinstance(n1.load(), Dag)
                    uid = n1.load().uuid.value()
            with Dml.temporary(cache_path=cache_path) as dml:
                assert dml.config_dir != config_dir, "Config dir should not be the same"
                with dml.new("d1", "d0") as d1:
                    n1 = d1.call(SUM, *nums)
                    uid1 = n1.load().uuid.value()
        assert uid == uid1, "Cached dag should have the same UUID"

    def test_no_caching(self):
        nums = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                config_dir = dml.config_dir
                with dml.new("d0", "d0") as d1:
                    n1 = d1.call(SUM, *nums)
                    uid = n1.load().uuid.value()
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                assert dml.config_dir != config_dir, "Config dir should not be the same"
                with dml.new("d1", "d0") as d1:
                    n1 = d1.call(SUM, *nums)
                    uid1 = n1.load().uuid.value()
        assert uid != uid1, "Cached dag should have the same UUID"

    def test_nodemap(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with dml.new("d0", "d0") as d0:
                    d0.a = 23
                    node = d0.put(42, name="b")
                    other = d0.put(420)
                    assert d0.a.value() == 23
                    assert list(d0) == ["a", "b"]
                    d0.commit([node, other])


class TestBasic(TestCase):
    def test_init(self):
        with Dml.temporary() as dml:
            status = dml("status")
            self.assertDictEqual(
                {k: v for k, v in status.items() if k != "cache_path"},
                {
                    "repo": dml.kwargs.get("repo"),
                    "branch": dml.kwargs.get("branch"),
                    "user": dml.kwargs.get("user"),
                    "config_dir": dml.kwargs.get("config_dir"),
                    "project_dir": dml.kwargs.get("project_dir"),
                },
            )
            assert status["cache_path"].startswith(os.path.expanduser("~"))
            self.assertEqual(dml.envvars["DML_CONFIG_DIR"], dml.kwargs.get("config_dir"))
            self.assertEqual(
                {k: v for k, v in dml.envvars.items() if k != "DML_CACHE_PATH"},
                {
                    "DML_REPO": dml.kwargs.get("repo"),
                    "DML_BRANCH": dml.kwargs.get("branch"),
                    "DML_USER": dml.kwargs.get("user"),
                    "DML_CONFIG_DIR": dml.kwargs.get("config_dir"),
                    "DML_PROJECT_DIR": dml.kwargs.get("project_dir"),
                },
            )

    def test_init_kwargs(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(repo="does-not-exist", branch="unique-name", cache_path=cache_path) as dml:
                self.assertDictEqual(
                    dml("status"),
                    {
                        "repo": "does-not-exist",
                        "branch": "unique-name",
                        "user": dml.kwargs.get("user"),
                        "config_dir": dml.kwargs.get("config_dir"),
                        "project_dir": dml.kwargs.get("project_dir"),
                        "cache_path": dml.kwargs.get("cache_path"),
                    },
                )
                self.assertEqual(dml.envvars["DML_CONFIG_DIR"], dml.kwargs.get("config_dir"))
                self.assertEqual(
                    dml.envvars,
                    {
                        "DML_REPO": "does-not-exist",
                        "DML_BRANCH": "unique-name",
                        "DML_USER": dml.kwargs.get("user"),
                        "DML_CONFIG_DIR": dml.kwargs.get("config_dir"),
                        "DML_PROJECT_DIR": dml.kwargs.get("project_dir"),
                        "DML_CACHE_PATH": cache_path,
                    },
                )

    def test_message_handler_load(self):
        local_value = None

        def message_handler(dump):
            nonlocal local_value
            local_value = dump

        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                d0 = dml.new("d0", "d0", message_handler=message_handler)
                data = {"key": "value", "list": [1, 2, 3], "dict": {"a": 1, "b": 2}, "resource": SUM}
                n0 = d0.put(data, name="n0")
                d0.commit(n0)
        assert isinstance(local_value, str)
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                ref = from_data(dml("ref", "load", local_value))
                assert len(dml("dag", "describe", ref.to)["nodes"]) == 1

    def test_dag(self):
        local_value = None

        def message_handler(dump):
            nonlocal local_value
            local_value = dump

        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                d0 = dml.new("d0", "d0", message_handler=message_handler)
                self.assertIsInstance(d0, Dag)
                n0 = d0.put([42], name="n0")
                assert isinstance(n0, Node)
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
                d0.n1 = n0[0]
                self.assertIsInstance(n0[0], Node)
                self.assertEqual([x.value() for x in n0], [d0.n1.value()])
                self.assertEqual(d0.n1.value(), 42)
                d0.n2 = {"x": n0, "y": "z"}
                self.assertNotEqual(d0.n2["x"], n0)
                self.assertEqual(d0.n2["x"].value(), n0.value())
                d0.n3 = list(d0.n2.items())
                self.assertIsInstance([x for x in d0.n3], list)
                self.assertDictEqual(
                    {k: v.value() for k, v in d0.n2.items()},
                    {"x": n0.value(), "y": "z"},
                )
                d0.n4 = [1, 2, 3, 4, 5]
                d0.n5 = d0.n4[1:]
                self.assertListEqual([x.value() for x in d0.n5], [2, 3, 4, 5])
                d0.commit(n0)
                self.assertIsInstance(local_value, str)
                dag = dml("dag", "list")[0]
                self.assertEqual(dag["result"], n0.ref.to)
                assert len(dml("dag", "list", "--all")) > 1
                dml("dag", "delete", dag["name"], "Deleting dag")
                dml("repo", "gc", as_text=True)

    def test_set_attrs(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with dml.new("d0", "d0") as dag:
                    n0 = dag.put({0})
                    assert n0.contains(1).value() is False
                    assert n0.contains(0).value() is True
                    assert 0 in n0
                    n1 = n0.append(1)
                    assert n1.value() == {0, 1}

    def test_load_constructors(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                dag = dml.new("d0", "d0")
                l0 = dag.put(42)
                c0 = dag.put({"a": 1, "b": [l0, "23"]})
                assert c0.backtrack("b", 0) == l0
                assert c0.backtrack("b", 1).value() == "23"
                assert c0.backtrack("b").backtrack(0) == l0
                assert c0["b"][0] != l0
                c1 = c0["b"]
                assert c1.backtrack() == c0
                assert c1.backtrack().backtrack("b", 0) == l0

    def test_fn_ok_cache(self):
        with TemporaryDirectory(prefix="dml-test-") as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                with TemporaryDirectory(prefix="dml-cache-") as cache_path:
                    with Dml.temporary(cache_path=cache_path) as dml:
                        with dml.new("d0", "d0") as dag:
                            nodes = [dag.call(SUM, i, 1, 2) for i in range(2)]  # unique function applications
                            dag.call(SUM, 0, 1, 2)  # add a repeat outside so `nodes` is still unique
                            dag.commit(nodes[0])
                        self.assertEqual(dag.result.value(), 3)
                        cache_list = dml("cache", "list", as_text=True)  # response is jsonlines format
                        assert len([x for x in cache_list if x.rstrip() == "{"]) == 2  # this gets us unique maps

    def test_async_fn_ok(self):
        with TemporaryDirectory(prefix="dml-test-") as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                debug_file = os.path.join(fn_cache_dir, "debug")
                with TemporaryDirectory(prefix="dml-cache-") as cache_path:
                    with Dml.temporary(cache_path=cache_path) as dml:
                        with dml.new("d0", "d0") as dag:
                            n1 = dag.call(ASYNC, 1, 2, 3)
                            dag.commit(n1)
                        self.assertEqual(n1.value(), 6)
                        with open(debug_file, "r") as f:
                            self.assertEqual(len([1 for _ in f]), 2)

    def test_async_fn_error(self):
        with TemporaryDirectory(prefix="dml-test-") as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                with TemporaryDirectory(prefix="dml-cache-") as cache_path:
                    with Dml.temporary(cache_path=cache_path) as dml:
                        with self.assertRaisesRegex(Error, r".*unsupported operand type.*"):
                            with dml.new("d0", "d0") as dag:
                                dag.call(ASYNC, 1, 2, "asdf")
                        info = [x for x in dml("dag", "list") if x["name"] == "d0"]
                        self.assertEqual(len(info), 1)

    def test_async_fn_timeout(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with self.assertRaises(TimeoutError):
                    with dml.new("d0", "d0") as dag:
                        dag.call(TIMEOUT, 1, 2, 3, timeout=1000)

    def test_load(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with dml.new("d0", "d0") as dag:
                    dag.put(42, name="n0")
                    dag.commit("foo")
                dl = dml.load("d0")
                assert isinstance(dl, Dag)
                self.assertEqual(dl.n0.value(), 42)
                self.assertEqual(dl.result.value(), "foo")
