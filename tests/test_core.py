from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
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

    def test_composite(self):
        dag = self.new('test-dag0', 'this is the test dag')
        n0 = dag.put(3)
        n1 = dag.put('x')
        n2 = dag.put([n0, n1])
        assert dag.get_value(n2) == [3, 'x']
        n3 = dag.put({'y': n2})
        assert dag.get_value(n3) == {'y': [3, 'x']}

    def test_in_process_sdk(self):
        # from aaron.dml import run
        dag = self.new('test-dag0', 'this is the test dag')
        l0 = dag.put({'asdf': 12})
        def f(fndag):
            _, x = fndag.expr
            return fndag.commit({k: v**2 for k, v in x.items()})
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
        def f(fndag):
            _, x = fndag.expr
            sleep(0.5)
            return fndag.commit(x**2)
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
        some_global_variable = [5]
        def fn(fndag):
            return fndag.commit(some_global_variable[0])
        # rsrc = dml.Resource('b')
        dag = self.new('test-dag0', 'this is the test dag')
        n0 = dag.call(fn, 12, 13, cache=True)
        assert dag.get_value(n0) == 5
        some_global_variable[0] = 1
        n1 = dag.call(fn, 12, 13, cache=True)
        assert dag.get_value(n1) == 5
        n1 = dag.call(fn, 12, 14, cache=True)
        assert dag.get_value(n1) == 1

    def test_realish(self):
        from itertools import product
        dag = self.new('test', 'foo')
        def f0(fndag):
            _, x, y = fndag.expr
            return fndag.commit(y * (x + 1))
        def f1(fndag):
            _, x, y = fndag.expr
            return fndag.commit(x + y)
        xs = [1, 2, 3, 4, 5]
        grid = list(product(range(5), repeat=2))
        results = []
        for a, b in grid:
            tmp = [dag.call(f0, x, a, cache=True) for x in xs]
            tmp = [dag.call(f1, x, b, cache=True) for x in tmp]
            results.append({'a': a, 'b': b, 'ys': tmp})

    def test_namespaces(self):
        @dataclass
        class Foo:
            dag: dml.Dag|dml.Node
            name = 'foo'
            def x(self, y):
                return y ** 2
        dml.Dag.register_ns(Foo)
        dag = self.new('test', 'this is a test')
        assert isinstance(dag.foo, Foo)
        assert dag.foo.x(3) == 9
        n = dag.put(12)
        with self.assertRaises(AttributeError, msg="'Node' object has no attribute 'foo'"):
            _ = n.foo
        dml.Node.register_ns(Foo)
        assert n.foo.x(3) == 9

    def test_fn_meta(self):
        dag = self.new('test-dag0', 'this is the test dag')
        args = dag.put(dml.Resource('a')), dag.put(12), dag.put(13)
        fn = dag.start_fn(*args)
        assert isinstance(fn, dml.Dag)
        assert isinstance(fn, dml.FnDag)
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
