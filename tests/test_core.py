import os
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from daggerml.core import Dag, Dml, Error, Node, Resource

SUM = Resource("./tests/assets/fns/sum.py", adapter="dml-python-fork-adapter")
ASYNC = Resource("./tests/assets/fns/async.py", adapter="dml-python-fork-adapter")
ENVVARS = Resource("./tests/assets/fns/envvars.py", adapter="dml-python-fork-adapter")
TIMEOUT = Resource("./tests/assets/fns/timeout.py", adapter="dml-python-fork-adapter")


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

    def test_dag(self):
        local_value = None

        def message_handler(dump):
            nonlocal local_value
            local_value = dump

        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                d0 = dml.new("d0", "d0", message_handler=message_handler)
                self.assertIsInstance(d0, Dag)
                # d0.n0 = [42]
                n0 = d0._put([42], name="n0")
                assert isinstance(n0, Node)
                self.assertIsInstance(n0, Node)
                self.assertEqual(n0.value(), [42])
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
                d0.result = result = n0
                self.assertIsInstance(local_value, str)
                dag = dml("dag", "list")[0]
                self.assertEqual(dag["result"], result.ref.to)
                assert len(dml("dag", "list", "--all")) > 1
                dml("dag", "delete", dag["name"], "Deleting dag")
                dml("repo", "gc", as_text=True)

    def test_list_attrs(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with dml.new("d0", "d0") as d0:
                    d0.n0 = [0]
                    assert d0.n0.contains(1).value() is False
                    assert d0.n0.contains(0).value() is True
                    assert 0 in d0.n0
                    d0.n1 = d0.n0.append(1)
                    assert d0.n1.value() == [0, 1]

    def test_set_attrs(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with dml.new("d0", "d0") as d0:
                    d0.n0 = {0}
                    assert d0.n0.contains(1).value() is False
                    assert d0.n0.contains(0).value() is True
                    assert 0 in d0.n0
                    d0.n1 = d0.n0.append(1)
                    assert d0.n1.value() == {0, 1}

    def test_dict_attrs(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with dml.new("d0", "d0") as d0:
                    d0.n0 = {"x": 42}
                    assert d0.n0.contains("y").value() is False
                    assert d0.n0.contains("x").value() is True
                    assert "y" not in d0.n0
                    assert "x" in d0.n0
                    d0.n1 = d0.n0.assoc("y", 3)
                    assert d0.n1.value() == {"x": 42, "y": 3}
                    d0.n2 = d0.n1.update({"z": 1, "a": 2})
                    assert d0.n2.value() == {"a": 2, "x": 42, "y": 3, "z": 1}

    def test_load_constructors(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                d0 = dml.new("d0", "d0")
                l0 = d0._put(42)
                c0 = d0._put({"a": 1, "b": [l0, "23"]})
                assert c0.load("b", 0) == l0
                assert c0.load("b", 1).value() == "23"
                assert c0.load("b").load(0) == l0
                assert c0["b"][0] != l0

    def test_fn_ok_cache(self):
        with TemporaryDirectory(prefix="dml-test-") as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                with TemporaryDirectory(prefix="dml-cache-") as cache_path:
                    with Dml.temporary(cache_path=cache_path) as dml:
                        with dml.new("d0", "d0") as d0:
                            d0.n0 = SUM
                            nodes = [d0.n0(i, 1, 2) for i in range(2)]  # unique function applications
                            d0.n0(0, 1, 2)  # add a repeat outside so `nodes` is still unique
                            d0.result = nodes[0]
                        self.assertEqual(d0.result.value(), 3)
                        cache_list = dml("cache", "list", as_text=True)  # response is jsonlines format
                        assert len([x for x in cache_list if x.rstrip() == "{"]) == 2  # this gets us unique maps

    def test_async_fn_ok(self):
        with TemporaryDirectory(prefix="dml-test-") as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                debug_file = os.path.join(fn_cache_dir, "debug")
                with TemporaryDirectory(prefix="dml-cache-") as cache_path:
                    with Dml.temporary(cache_path=cache_path) as dml:
                        with dml.new("d0", "d0") as d0:
                            d0.n0 = ASYNC
                            d0.n1 = d0.n0(1, 2, 3)
                            d0.result = result = d0.n1
                        self.assertEqual(result.value(), 6)
                        with open(debug_file, "r") as f:
                            self.assertEqual(len([1 for _ in f]), 2)

    def test_async_fn_error(self):
        with TemporaryDirectory(prefix="dml-test-") as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                with TemporaryDirectory(prefix="dml-cache-") as cache_path:
                    with Dml.temporary(cache_path=cache_path) as dml:
                        with self.assertRaisesRegex(Error, r".*unsupported operand type.*"):
                            with dml.new("d0", "d0") as d0:
                                d0.n0 = ASYNC
                                d0.n1 = d0.n0(1, 2, "asdf")
                        info = [x for x in dml("dag", "list") if x["name"] == "d0"]
                        self.assertEqual(len(info), 1)

    def test_async_fn_timeout(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with self.assertRaises(TimeoutError):
                    with dml.new("d0", "d0") as d0:
                        d0.n0 = TIMEOUT
                        d0.n0(1, 2, 3, timeout=1000)

    def test_load(self):
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with dml.new("d0", "d0") as d0:
                    d0.n0 = 42
                    d0.result = "foo"
                dl = dml.load("d0")
                assert isinstance(dl, Dag)
                self.assertEqual(type(dl.n0), Node)
                self.assertEqual(dl.n0.value(), 42)
                self.assertEqual(type(dl.result), Node)
                self.assertEqual(dl.result.value(), "foo")

    def test_load_recursing(self):
        nums = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=dml.kwargs["config_dir"]):
                    with dml.new("d0", "d0") as d0:
                        d0.n0 = SUM
                        d0.n1 = d0.n0(*nums)
                        assert d0.n1.dag == d0
                        d0.result = d0.n1
                d1 = dml.new("d1", "d1")
                d1.n1 = dml.load("d0").n1
                assert d1.n1.dag == d1
                d1.n2 = dml.load(d1.n1, recurse=True).num_args
                assert d1.n2.value() == len(nums)
                assert d1.n1.value() == sum(nums)
                assert isinstance(d1.n1.load(), Dag)

    def test_caching(self):
        nums = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                config_dir = dml.config_dir
                with dml.new("d0", "d0") as d1:
                    d1.sum_fn = SUM
                    n1 = d1.sum_fn(*nums, name="n1")
                    assert n1.value() == sum(nums)
                    assert isinstance(n1.load(), Dag)
                    uid = d1.n1.load().uuid.value()
            with Dml.temporary(cache_path=cache_path) as dml:
                assert dml.config_dir != config_dir, "Config dir should not be the same"
                with dml.new("d1", "d0") as d1:
                    d1.sum_fn = SUM
                    d1.n1 = d1.sum_fn(*nums)
                    uid1 = d1.n1.load().uuid.value()
        assert uid == uid1, "Cached dag should have the same UUID"

    def test_no_caching(self):
        nums = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                config_dir = dml.config_dir
                with dml.new("d0", "d0") as d1:
                    d1.n0 = SUM
                    d1.n1 = d1.n0(*nums)
                    assert isinstance(d1.n1, Node)
                    uid = d1.n1.load().uuid.value()
        with TemporaryDirectory(prefix="dml-cache-") as cache_path:
            with Dml.temporary(cache_path=cache_path) as dml:
                assert dml.config_dir != config_dir, "Config dir should not be the same"
                with dml.new("d1", "d0") as d1:
                    d1.n0 = SUM
                    d1.n1 = d1.n0(*nums)
                    uid1 = d1.n1.load().uuid.value()
        assert uid != uid1, "Cached dag should have the same UUID"
