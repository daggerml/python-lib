import os
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from daggerml import Dml, Error, Node, Resource

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
                'repo_path': f'{os.path.join(dml.kwargs.get("config_dir"), "repo", dml.kwargs.get("repo"))}',
            })

    def test_dag(self):
        with Dml() as dml:
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
                n4 = d0.put([1, 2, 3, 4, 5])
                n5 = n4[1:]
                self.assertListEqual([x.value() for x in n5], [2, 3, 4, 5])
                d0.commit(n0)
                self.assertIsInstance(d0.dump, str)
                dag = dml('dag', 'list')[0]
                self.assertEqual(dag['result'], n0.ref.to.split('/', 1)[1])

    def test_async_fn_ok(self):
        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                debug_file = os.path.join(fn_cache_dir, 'debug')
                with Dml() as dml:
                    with dml.new('d0', 'd0') as d0:
                        n0 = d0.put(ASYNC)
                        n1 = n0(1, 2, 3, timeout=1000)
                        d0.commit(n1)
                        self.assertEqual(n1.value(), 6)
                        with open(debug_file, 'r') as f:
                            self.assertEqual(len([1 for _ in f]), 2)

    def test_async_fn_error(self):
        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                with Dml() as dml:
                    with self.assertRaises(Error):
                        with dml.new('d0', 'd0') as d0:
                            n0 = d0.put(ERROR)
                            n0(1, 2, 3, timeout=1000)
                    info = [x for x in dml('dag', 'list') if x['name'] == 'd0']
                    self.assertEqual(len(info), 1)

    def test_async_fn_timeout(self):
        with Dml() as dml:
            with self.assertRaises(TimeoutError):
                with dml.new('d0', 'd0') as d0:
                    n0 = d0.put(TIMEOUT)
                    n0(1, 2, 3, timeout=1000)

    def test_load(self):
        with Dml() as dml:
            with dml.new('d0', 'd0') as d0:
                d0.commit(42)
            with dml.new('d1', 'd1') as d1:
                n0 = d1.load('d0')
                self.assertEqual(n0.value(), 42)
