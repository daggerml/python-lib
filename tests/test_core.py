from dataclasses import dataclass

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
        waiter = dag.start_fn(r0, l0, l0)
        assert waiter.get_result() is None
        f0 = dml.Dag.new('foo', 'message', dump=waiter.dump, api_flags=dag.api_flags)
        assert f0.expr[0] == rsrc
        l1 = f0.put({'qwer': 23})
        assert isinstance(l1, dml.Node)
        f0.commit(l1)
        n1 = waiter.get_result()
        assert isinstance(n1, dml.Node)
        assert dag.get_value(n1) == {'qwer': 23}
        dag.commit(n1)
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

    def test_cache_basic(self):
        dag = self.new('test-dag0', 'this is the test dag')
        r0 = dag.put(dml.Resource('a'))
        l0 = dag.put({'asdf': 12})
        waiter = dag.start_fn(r0, l0, use_cache=True)
        assert waiter.get_result() is None
        f0 = dml.Dag.new('foo', 'message', dump=waiter.dump, api_flags=dag.api_flags)
        f0.commit(f0.put(23))
        n1 = waiter.get_result()
        assert n1.value() == 23
        waiter.cache()
        # should be cached now
        waiter = dag.start_fn(r0, l0, use_cache=True)
        n1 = waiter.get_result()
        assert n1.value() == 23

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

    def test_contextmanager(self):
        with self.assertRaises(ZeroDivisionError):
            with self.new('dag0', 'this is the test dag') as dag:
                dag.put(1 / 0)
        dag = self.new('dag1', 'this is the test dag')
        n0 = dag.load('dag0')
        assert isinstance(n0, dml.Node)
        with self.assertRaises(dml.Error):
            dag.get_value(n0)
