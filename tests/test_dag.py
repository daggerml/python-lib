from typing import Any, Dict

import daggerml as dml
from tests.util import DmlTestBase


class TestApi(DmlTestBase):

    def test_basic(self):
        dag = self.new('test-dag0', 'this is the test dag')
        assert isinstance(dag, dml.Dag)
        l0 = dag.put({'asdf': 12})
        assert isinstance(l0, dml.Node)
        assert dag.get_value(l0) == {'asdf': 12}
        rsrc = dml.Resource('a')
        r0 = dag.put(rsrc)
        f0 = dag.start_fn(r0, l0, l0)
        assert isinstance(f0, dml.Dag)
        assert f0.expr[0] == rsrc
        l1 = f0.put({'qwer': 23})
        assert isinstance(l1, dml.Node)
        n1 = f0.commit(l1)
        assert isinstance(n1, dml.Node)
        assert dag.get_value(n1) == {'qwer': 23}
        assert dag.commit(n1) is None
        dag = self.new('test-dag1', 'this is the second test dag')
        n0 = dag.load('test-dag0')
        assert isinstance(n0, dml.Node)
        assert dag.get_value(n0) == {'qwer': 23}

    def test_literal(self):
        data = {
            'int': 23,
            'float': 12.43,
            'bool': True,
            'null': None,
            'string': 'qwer',
            'list': [3, 4, 5],
            'map': {'a': 2, 'b': 'asdf'},
            'set': {12, 13, 'a', 3.4},
            'resource': dml.Resource('a'),
            'compound': {'a': 23, 'b': {5, dml.Resource('b')}}
        }
        dag = self.new('test-dag0', 'this is the test dag')
        for k, v in data.items():
            node = dag.put(v)
            assert isinstance(node, dml.Node), f'{k = }'
            assert dag.get_value(node) == v, f'{k = }'

    def test_in_process_sdk(self):
        # from aaron.dml import run
        dag = self.new('test-dag0', 'this is the test dag')
        l0 = dag.put({'asdf': 12})
        def f(x: Dict[Any, int]) -> Dict[Any, int]:
            return {k: v**2 for k, v in x.items()}
        # ====== start
        n1 = dag.call(f, l0)
        # ====== end
        assert isinstance(n1, dml.Node)
        assert dag.get_value(n1) == {'asdf': 144}

    def test_cache_basic(self):
        stash = [0]
        def f(*_):
            return stash[0]
        # rsrc = dml.Resource('b')
        dag = self.new('test-dag0', 'this is the test dag')
        args = dag.put(12), dag.put(13)
        n0 = dag.call(f, *args, cache=True)
        assert dag.get_value(n0) == 0
        stash[0] = 1
        n1 = dag.call(f, *args, cache=True)
        assert dag.get_value(n1) == 0

    def test_contextmanager(self):
        with self.assertRaises(ZeroDivisionError):
            with self.new('dag0', 'this is the test dag') as dag:
                dag.put(1 / 0)
        dag = self.new('dag1', 'this is the test dag')
        n0 = dag.load('dag0')
        assert isinstance(n0, dml.Node)
        with self.assertRaises(dml.Error):
            dag.get_value(n0)
