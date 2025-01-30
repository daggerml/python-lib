import os
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from daggerml import Dag, Dml, Error, Node, Resource
from daggerml.core import Import

SUM = Resource('./tests/assets/fns/minimal_viable_fn.py', adapter='dml-python-fork-adapter')
ASYNC = Resource('./tests/assets/fns/async.py', adapter='dml-python-fork-adapter')
ERROR = Resource('./tests/assets/fns/error.py', adapter='dml-python-fork-adapter')
TIMEOUT = Resource('./tests/assets/fns/timeout.py', adapter='dml-python-fork-adapter')


class TestBasic(TestCase):

    def test_init(self):
        with Dml() as dml:
            self.assertDictEqual(dml('status'), {
                'repo': dml.kwargs.get('repo'),
                'branch': dml.kwargs.get('branch'),
                'user': dml.kwargs.get('user'),
                'config_dir': dml.kwargs.get('config_dir'),
                'project_dir': dml.kwargs.get('project_dir'),
            })

    def test_dag(self):
        with Dml() as dml:
            with dml.new('d0', 'd0') as d0:
                d0.n0 = [42]
                self.assertIsInstance(d0.n0, Node)
                self.assertEqual(d0.n0.value(), [42])
                self.assertEqual(d0.n0.len().value(), 1)
                self.assertEqual(d0.n0.type().value(), 'list')
                d0['x0'] = d0.n0
                self.assertEqual(d0['x0'], d0.n0)
                self.assertEqual(d0.x0, d0.n0)
                d0.x1 = 42
                self.assertEqual(d0['x1'].value(), 42)
                self.assertEqual(d0.x1.value(), 42)
                d0.n1 = d0.n0[0]
                self.assertIsInstance(d0.n1, Node)
                self.assertEqual([x for x in d0.n0], [d0.n1])
                self.assertEqual(d0.n1.value(), 42)
                d0.n2 = {'x': d0.n0, 'y': 'z'}
                self.assertNotEqual(d0.n2['x'], d0.n0)
                self.assertEqual(d0.n2['x'].value(), d0.n0.value())
                d0.n3 = list(d0.n2.items())
                self.assertIsInstance([x for x in d0.n3], list)
                self.assertDictEqual({k.value(): v.value() for k, v in d0.n2.items()}, {'x': d0.n0.value(), 'y': 'z'})
                d0.n4 = [1, 2, 3, 4, 5]
                d0.n5 = d0.n4[1:]
                self.assertListEqual([x.value() for x in d0.n5], [2, 3, 4, 5])
                d0.result = result = d0.n0
                self.assertIsInstance(d0._dump, str)
            dag = dml('dag', 'list')[0]
            self.assertEqual(dag['result'], result.ref.to.split('/', 1)[1])

    def test_async_fn_ok(self):
        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                debug_file = os.path.join(fn_cache_dir, 'debug')
                with Dml() as dml:
                    with dml.new('d0', 'd0') as d0:
                        d0.n0 = ASYNC
                        d0.n1 = d0.n0(1, 2, 3)
                        d0.result = result = d0.n1
                    self.assertEqual(result.value(), 6)
                    with open(debug_file, 'r') as f:
                        self.assertEqual(len([1 for _ in f]), 2)

    def test_async_fn_error(self):
        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                with Dml() as dml:
                    with self.assertRaises(Error):
                        with dml.new('d0', 'd0') as d0:
                            d0.n0 = ERROR
                            d0.n1 = d0.n0(1, 2, 3)
                    info = [x for x in dml('dag', 'list') if x['name'] == 'd0']
                    self.assertEqual(len(info), 1)

    def test_async_fn_timeout(self):
        with Dml() as dml:
            with self.assertRaises(TimeoutError):
                with dml.new('d0', 'd0') as d0:
                    d0.n0 = TIMEOUT
                    d0.n0(1, 2, 3, timeout=1000)

    def test_load(self):
        with Dml() as dml:
            with dml.new('d0', 'd0') as d0:
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
            dl = dml.load('d0')
            self.assertEqual(type(dl), Dag)
            self.assertEqual(type(dl.n0), Import)
            self.assertEqual(dl.n0.value(), 42)
            self.assertEqual(type(dl.result), Import)
            self.assertEqual(dl.result.value(), 42)
            self.assertEqual(len(dl), 2)
            self.assertEqual(set(dl.keys()), {'n0', 'n1'})
            self.assertEqual(set(dl.values()), {dl.n0, dl.n1})
            for x in dl.values():
                self.assertIsInstance(x, Import)
            with dml.new('d1', 'd1') as d1:
                d0 = dml.load('d0')
                self.assertEqual(d0.result.value(), 42)
                self.assertEqual(d0.n0.value(), 42)
                self.assertEqual(d0['n0'].value(), 42)

                self.assertEqual(len(d0), 2)
                self.assertEqual(set(d0.keys()), {'n0', 'n1'})
                self.assertEqual(set(d0.values()), {d0.n0, d0.n1})
                # d0 has been committed: its nodes are now of type Import
                for x in d0.values():
                    self.assertIsInstance(x, Import)

                d1.n0 = 42
                d1.n1 = 420
                self.assertEqual(set(d1.keys()), {'n0', 'n1'})
                # d1 has not yet been committed: its nodes are of type Node
                for x in d1.values():
                    self.assertEqual(type(x), Node)

                d1.result = d0.result
