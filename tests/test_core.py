import os
from unittest import TestCase, mock

from daggerml import Node, Resource
from daggerml.helper import Dml

ASYNC = Resource('./tests/assets/fns/async.py', adapter='dml-python-in-process-adapter')


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
                self.assertNotEqual(n2['x'], n0)  # NOTE: Should these be equal?
                self.assertEqual(n2['x'].value(), n0.value())
                n3 = n2.items()
                self.assertIsInstance([x for x in n3], list)
                self.assertDictEqual({k.value(): v.value() for k, v in n2.items()}, {'x': n0.value(), 'y': 'z'})
                n4 = d0.commit(n0)
                self.assertIsInstance(n4, Node)
                dag = dml('dag', 'list')[0]
                self.assertEqual(dag['result'], n0.ref.to.split('/', 1)[1])

    def test_fn(self):
        with mock.patch.dict(os.environ, PYTHONPATH='.'):
            with Dml.init() as dml:
                with dml.new('d0', 'd0') as d0:
                    n0 = d0.put(ASYNC)
                    try:
                        n1 = n0()
                        print(f'zzz: {str(n1)}')
                        print(f'zzz: {n1=}')
                        print(f'zzz: {n1.value()=}')
                    except Exception:
                        print(f"{dml('dag', 'list')=}")
