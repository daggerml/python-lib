import os
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from daggerml import Error, Node, Resource
from daggerml.helper import Dml

ASYNC = Resource('./tests/assets/fns/async.py', adapter='dml-python-fork-adapter')
ERROR = Resource('./tests/assets/fns/error.py', adapter='dml-python-fork-adapter')


class TestBasic(TestCase):

    def test_init(self):
        with Dml.init() as dml:
            self.assertDictEqual(dml('status'), {
                'repo': dml.kwargs.get('repo'),
                'branch': dml.kwargs.get('branch'),
                'user': dml.kwargs.get('user'),
                'config_dir': dml.kwargs.get('config_dir'),
                'project_dir': dml.kwargs.get('project_dir'),
                'repo_path': f'{os.path.join(dml.kwargs.get("config_dir"), "repo", dml.kwargs.get("repo"))}',
            })

    def test_dag(self):
        with Dml.init() as dml:
            with dml.new('d0', 'd0') as d0:
                n0 = d0.put([42])
                self.assertIsInstance(n0, Node)
                self.assertEqual(n0.value(), [42])
                self.assertEqual(n0.len().value(), 1)
                self.assertEqual(n0.type().value(), 'list')
                n1 = n0[0]
                self.assertIsInstance(n1, Node)
                self.assertEqual([x for x in n0], [n1])
                self.assertEqual(n1.value(), 42)
                n2 = d0.put({'x': n0, 'y': 'z'})
                self.assertNotEqual(n2['x'], n0)
                self.assertEqual(n2['x'].value(), n0.value())
                n3 = n2.items()
                self.assertIsInstance([x for x in n3], list)
                self.assertDictEqual({k.value(): v.value() for k, v in n2.items()}, {'x': n0.value(), 'y': 'z'})
                d0.commit(n0)
                self.assertIsInstance(d0.dump, str)
                dag = dml('dag', 'list')[0]
                self.assertEqual(dag['result'], n0.ref.to.split('/', 1)[1])

    def test_fn(self):
        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                debug_file = os.path.join(fn_cache_dir, 'debug')
                with Dml.init() as dml:
                    with dml.new('d0', 'd0') as d0:
                        n0 = d0.put(ASYNC)
                        n1 = n0()
                        self.assertEqual(n1.value(), 42)
                        with open(debug_file, 'r') as f:
                            self.assertEqual(len([1 for _ in f]), 2)

    def test_fn_error(self):
        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                debug_file = os.path.join(fn_cache_dir, 'debug')
                with Dml.init() as dml:
                    with self.assertRaises(Error):
                        with dml.new('d0', 'd0') as d0:
                            n0 = d0.put(ERROR)
                            n0()
                    with open(debug_file, 'r') as f:
                        self.assertEqual(len([1 for _ in f]), 2)
                    # TODO: Verify fndag was correctly committed.

    def test_load(self):
        with Dml.init() as dml:
            with dml.new('d0', 'd0') as d0:
                d0.commit(42)
            with dml.new('d1', 'd1') as d1:
                n0 = d1.load('d0')
                self.assertEqual(n0.value(), 42)
