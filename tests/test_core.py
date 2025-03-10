import os
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from daggerml.core import Dag, Dml, Error, Node, Resource

SUM = Resource("./tests/assets/fns/sum.py", adapter="dml-python-fork-adapter")
ASYNC = Resource("./tests/assets/fns/async.py", adapter="dml-python-fork-adapter")
ERROR = Resource("./tests/assets/fns/error.py", adapter="dml-python-fork-adapter")
TIMEOUT = Resource("./tests/assets/fns/timeout.py", adapter="dml-python-fork-adapter")


class TestBasic(TestCase):
    def test_init(self):
        with Dml() as dml:
            self.assertDictEqual(
                dml("status"),
                {
                    "repo": dml.kwargs.get("repo"),
                    "branch": dml.kwargs.get("branch"),
                    "user": dml.kwargs.get("user"),
                    "config_dir": dml.kwargs.get("config_dir"),
                    "project_dir": dml.kwargs.get("project_dir"),
                },
            )
            self.assertEqual(dml.envvars["DML_CONFIG_DIR"], dml.kwargs.get("config_dir"))
            self.assertEqual(
                dml.envvars,
                {
                    "DML_REPO": dml.kwargs.get("repo"),
                    "DML_BRANCH": dml.kwargs.get("branch"),
                    "DML_USER": dml.kwargs.get("user"),
                    "DML_CONFIG_DIR": dml.kwargs.get("config_dir"),
                    "DML_PROJECT_DIR": dml.kwargs.get("project_dir"),
                },
            )

    def test_dag(self):
        local_value = None

        def message_handler(dump):
            nonlocal local_value
            local_value = dump

        with Dml() as dml:
            d0 = dml.new("d0", "d0", message_handler=message_handler)
            d0.n0 = [42]
            self.assertIsInstance(d0.n0, Node)
            self.assertEqual(d0.n0.value(), [42])
            self.assertEqual(d0.n0.len().value(), 1)
            self.assertEqual(d0.n0.type().value(), "list")
            d0["x0"] = d0.n0
            self.assertEqual(d0["x0"], d0.n0)
            self.assertEqual(d0.x0, d0.n0)
            d0.x1 = 42
            self.assertEqual(d0["x1"].value(), 42)
            self.assertEqual(d0.x1.value(), 42)
            d0.n1 = d0.n0[0]
            self.assertIsInstance(d0.n1, Node)
            self.assertEqual([x for x in d0.n0], [d0.n1])
            self.assertEqual(d0.n1.value(), 42)
            d0.n2 = {"x": d0.n0, "y": "z"}
            self.assertNotEqual(d0.n2["x"], d0.n0)
            self.assertEqual(d0.n2["x"].value(), d0.n0.value())
            d0.n3 = list(d0.n2.items())
            self.assertIsInstance([x for x in d0.n3], list)
            self.assertDictEqual(
                {k.value(): v.value() for k, v in d0.n2.items()},
                {"x": d0.n0.value(), "y": "z"},
            )
            d0.n4 = [1, 2, 3, 4, 5]
            d0.n5 = d0.n4[1:]
            self.assertListEqual([x.value() for x in d0.n5], [2, 3, 4, 5])
            d0.result = result = d0.n0
            self.assertIsInstance(local_value, str)
            dag = dml("dag", "list")[0]
            self.assertEqual(dag["result"], result.ref.to.split("/", 1)[1])
            dml("dag", "delete", dag["name"], "Deleting dag")
            dml("repo", "gc", as_text=True)

    def test_list_attrs(self):
        with Dml() as dml:
            with dml.new("d0", "d0") as d0:
                d0.n0 = [0]
                assert d0.n0.contains(1).value() is False
                assert d0.n0.contains(0).value() is True
                assert 0 in d0.n0
                d0.n1 = d0.n0.append(1)
                assert d0.n1.value() == [0, 1]

    def test_set_attrs(self):
        with Dml() as dml:
            with dml.new("d0", "d0") as d0:
                d0.n0 = {0}
                assert d0.n0.contains(1).value() is False
                assert d0.n0.contains(0).value() is True
                assert 0 in d0.n0
                d0.n1 = d0.n0.append(1)
                assert d0.n1.value() == {0, 1}

    def test_dict_attrs(self):
        with Dml() as dml:
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

    def test_async_fn_ok(self):
        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                debug_file = os.path.join(fn_cache_dir, "debug")
                with Dml() as dml:
                    with dml.new("d0", "d0") as d0:
                        d0.n0 = ASYNC
                        d0.n1 = d0.n0(1, 2, 3)
                        d0.result = result = d0.n1
                    self.assertEqual(result.value(), 6)
                    with open(debug_file, "r") as f:
                        self.assertEqual(len([1 for _ in f]), 2)

    def test_async_fn_error(self):
        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                with Dml() as dml:
                    with self.assertRaises(Error):
                        with dml.new("d0", "d0") as d0:
                            d0.n0 = ERROR
                            d0.n1 = d0.n0(1, 2, 3)
                    info = [x for x in dml("dag", "list") if x["name"] == "d0"]
                    self.assertEqual(len(info), 1)

    def test_async_fn_timeout(self):
        with Dml() as dml:
            with self.assertRaises(TimeoutError):
                with dml.new("d0", "d0") as d0:
                    d0.n0 = TIMEOUT
                    d0.n0(1, 2, 3, timeout=1000)

    def test_load(self):
        with Dml() as dml:
            with dml.new("d0", "d0") as d0:
                # only fn dags have an argv attribute, expect AttributeError
                with self.assertRaises(Error):
                    d0.argv  # noqa: B018
                # d0.result hasn't been assigned yet but it can't raise an
                # AttributeError because we also have __getitem__ implemented
                # which would then be called, so an AssertionError is raised.
                with self.assertRaises(AssertionError):
                    d0.result  # noqa: B018
                d0.n0 = 42
                self.assertEqual(type(d0.n0), Node)
                d0.n1 = 420
                d0.result = d0.n0
            dl = dml.load("d0")
            self.assertEqual(type(dl), Dag)
            self.assertEqual(type(dl.n0), Node)
            self.assertEqual(dl.n0.value(), 42)
            self.assertEqual(type(dl.result), Node)
            self.assertEqual(dl.result.value(), 42)
            self.assertEqual(len(dl), 2)
            self.assertEqual(set(dl.keys()), {"n0", "n1"})
            self.assertEqual(set(dl.values()), {dl.n0, dl.n1})
            for x in dl.values():
                self.assertIsInstance(x, Node)
            with dml.new("d1", "d1") as d1:
                d0 = dml.load("d0")
                self.assertEqual(d0.result.value(), 42)
                self.assertEqual(d0.n0.value(), 42)
                self.assertEqual(d0["n0"].value(), 42)

                self.assertEqual(len(d0), 2)
                self.assertEqual(set(d0.keys()), {"n0", "n1"})
                self.assertEqual(set(d0.values()), {d0.n0, d0.n1})
                # d0 has been committed: its nodes are now imports
                for x in d0.values():
                    self.assertIsInstance(x, Node)

                d1.n0 = 42
                d1.n1 = 420
                self.assertEqual(set(d1.keys()), {"n0", "n1"})
                # d1 has not yet been committed: its nodes are of type Node
                for x in d1.values():
                    self.assertEqual(type(x), Node)

                d1.result = d0.result

    def test_load_recursing(self):
        nums = [1, 2, 3]
        with Dml() as dml:
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
