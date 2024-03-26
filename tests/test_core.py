from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import sleep

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
        def f(x):
            return {k: v**2 for k, v in x.items()}
        # ====== start
        n1 = dag.call(f, l0)
        # ====== end
        assert isinstance(n1, dml.Node)
        assert dag.get_value(n1) == {'asdf': 144}

    def test_dag_threads(self):
        def doit(dag, i):
            return dag.put(i)
        dag = self.new('test-dag0', 'this is the test dag')
        # ====== start
        with ThreadPoolExecutor(2) as pool:
            resp = pool.map(partial(doit, dag), range(2))
        for i, x in enumerate(resp):
            assert dag.get_value(x) == i

    def test_update_loop(self):
        pool_size = 5
        extra = 3
        def f(x):
            sleep(0.5)
            return x**2
        dag = self.new('test-dag0', 'this is the test dag')
        l0 = dag.put(12)
        # ====== start
        with self.assertLogs('daggerml', level='DEBUG') as cm:
            with ThreadPoolExecutor(pool_size) as pool:
                resp = pool.map(partial(dag.call, cache=True, update_freq=0.1),
                                *zip(*[(f, l0) for _ in range(pool_size + extra)]))
        assert len([x for x in cm.output if 'checking dag' in x]) == pool_size
        assert len([x for x in cm.output if 'returning cached call' in x]) == extra
        assert len([x for x in cm.output if f'running function: {f.__qualname__!r}' in x]) == 1
        assert len([x for x in cm.output if 'no lock. waiting for result' in x]) == pool_size - 1
        assert {dag.get_value(n) for n in resp} == {144}

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

    def test_fn_meta(self):
        dag = self.new('test-dag0', 'this is the test dag')
        args = dag.put(dml.Resource('a')), dag.put(12), dag.put(13)
        fn = dag.start_fn(*args)
        assert isinstance(fn, dml.Dag)
        assert fn.meta == ''
        fn.update_meta('', 'testing')
        assert fn.meta == 'testing'

    def test_contextmanager(self):
        with self.assertRaises(ZeroDivisionError):
            with self.new('dag0', 'this is the test dag') as dag:
                dag.put(1 / 0)
        dag = self.new('dag1', 'this is the test dag')
        n0 = dag.load('dag0')
        assert isinstance(n0, dml.Node)
        with self.assertRaises(dml.Error):
            dag.get_value(n0)
