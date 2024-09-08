import pytest

import daggerml as dml
from tests.util import DmlTestBase

data = {
    'int': 23,
    'float': 12.43,
    'bool': True,
    'null': None,
    'string': 'qwer',
    'list': [3, 4, 5],
    'map': {'a': 2, 'b': 'asdf'},
    'set': {12, 13, 'a', 3.4},
    'resource': dml.Resource('a:b', data='{"x":23}'),
    'compound': {'a': 23, 'b': {5, dml.Resource('b:b')}}
}

@pytest.mark.parametrize("x", list(data.values()), ids=list(data))
def test_literal(x):
    dag = dml.Api(initialize=True).new_dag('test-dag0', 'this is the test dag')
    node = dag.put(x)
    assert isinstance(node, dml.Node)
    assert node.value() == x

class TestApi(DmlTestBase):

    def test_basic(self):
        dag = self.new('test-dag0', 'this is the test dag')
        assert isinstance(dag, dml.Dag)
        l0 = dag.put({'asdf': 12})
        assert isinstance(l0, dml.Node)
        assert l0.value() == {'asdf': 12}
        dag.commit(l0)

    def test_db_funcs_dict(self):
        dag = self.new('test-dag0', 'this is the test dag')
        l0 = dag.put({'a': 3, 'b': 5})
        assert l0.len().value() == 2
        assert l0.keys().value() == ['a', 'b']
        assert l0['a'].value() == l0[dag.put('a')].value() == 3
        assert l0['b'].value() == l0[dag.put('b')].value() == 5

    def test_db_funcs_list(self):
        dag = self.new('test-dag0', 'this is the test dag')
        l0 = dag.put(['a', 3, 'b', 5])
        assert l0.len().value() == 4
        with self.assertRaises(dml.Error):
            l0.keys()
        with self.assertRaises(dml.Error):
            l0[-1]
        with self.assertRaises(dml.Error):
            l0[4]
        assert [x.value() for x in l0[1:]] == [3, 'b', 5]
        assert l0[0].value() == l0[dag.put(0)].value() == 'a'
        assert [x.value() for x in l0] == ['a', 3, 'b', 5]
        node = dag.put({'a': 3, 'b': 5})
        for k, v in node.items():
            assert node[k] == v

    def test_composite(self):
        dag = self.new('test-dag0', 'this is the test dag')
        n0 = dag.put(3)
        n1 = dag.put('x')
        n2 = dag.put([n0, n1])
        assert n2.value() == [3, 'x']
        n3 = dag.put({'y': n2})
        assert n3.value() == {'y': [3, 'x']}

    def test_cache_basic(self):
        dag = self.new('test-dag0', 'this is the test dag')
        r0 = dag.put(dml.Resource('a:a'))
        l0 = dag.put({'asdf': 12})
        waiter = dag.start_fn(r0, l0)
        assert waiter.get_result() is None
        f0 = dml.Dag.new('foo', 'message', dump=waiter.dump, api_flags=dag.api.flags)
        f0.commit(f0.put(23))
        n1 = waiter.get_result()
        assert n1.value() == 23
        # should be cached now
        waiter = dag.start_fn(r0, l0)
        n1 = waiter.get_result()
        assert n1.value() == 23

    def test_cache_cloud(self):
        expr = [dml.Resource('a:b'), {'asdf': 12}]
        # self is cloud api
        with dml.Api(initialize=True) as user0_api:
            dag = user0_api.new_dag('test-dag0', 'this is the test dag')
            r0, l0 = (dag.put(x) for x in expr)
            waiter = dag.start_fn(r0, l0)
            assert waiter.get_result() is None
            # runs in the "cloud..."
            f0 = self.new('foo', 'message', dump=waiter.dump)
            f0_dump = f0.dump(f0.commit(f0.put(23)))
            # back on user0's machine
            dag.load_ref(f0_dump)
            n1 = waiter.get_result()
            assert n1.value() == 23
        with dml.Api(initialize=True) as user1_api:
            dag = user1_api.new_dag('test-dag0', 'this is the test dag')
            r0, l0 = (dag.put(x) for x in expr)
            waiter = dag.start_fn(r0, l0)
            assert waiter.get_result() is None
            # cloud returns with cached f0_dump
            # back on user0's machine
            user1_api.load(f0_dump)
            n1 = waiter.get_result()
            assert n1.value() == 23

    def test_cannot_commit_finished_dag(self):
        dag = self.new('test-dag0', 'this is the test dag')
        r0 = dag.put(dml.Resource('a:a'))
        l0 = dag.put({'asdf': 12})
        waiter = dag.start_fn(r0, l0)
        assert waiter.get_result() is None
        f0 = dml.Dag.new('foo', 'message', dump=waiter.dump, api_flags=dag.api.flags)
        f0.commit(f0.put(23))
        n1 = waiter.get_result()
        assert n1.value() == 23
        # should be cached now
        waiter = dag.start_fn(r0, l0)
        f0 = dml.Dag.new('foo', 'message', dump=waiter.dump, api_flags=dag.api.flags)
        with self.assertRaisesRegex(dml.Error, 'dag has been committed already'):
            f0.commit(f0.put(5854))

    def test_contextmanager(self):
        with self.assertRaises(ZeroDivisionError):
            with self.new('dag0', 'this is the test dag') as dag:
                dag.put(1 / 0)
        dag = self.new('dag1', 'this is the test dag')
        n0 = dag.load('dag0')
        assert isinstance(n0, dml.Node)
        with self.assertRaises(dml.Error):
            n0.value()

    def test_meta_api_calls(self):
        self.api('branch', 'create', 'foopy')
        self.api('branch', 'use', 'foopy')
        expr = [dml.Resource('a:b'), {'asdf': 12}]
        dag = self.new('test-dag0', 'this is the test dag')
        waiter = dag.start_fn(*(dag.put(x) for x in expr))
        with dml.Api(initialize=True) as cloud_api:
            # runs in the "cloud..."
            f0 = cloud_api.new_dag('foo', 'message', dump=waiter.dump)
            f0_dump = f0.dump(f0.commit(f0.put(23)))
            # back on user's machine
        dag.load_ref(f0_dump)
        n1 = waiter.get_result()
        assert n1.value() == 23
        ref = dag.commit(n1).to
        assert {x['id']: x['name'] for x in self.api('dag', 'list', output='json')} == {ref: 'test-dag0'}
        desc, = self.api('dag', 'describe', ref, output='json')
        result = desc['result']
        assert result in desc['nodes']
        assert list(desc['edges']) == [result]
        self.assertCountEqual(desc['nodes'], [result, *desc['edges'][result]])
        assert sorted(self.api('branch', 'list').split('\n')) == ['foopy', 'main']
        self.api('branch', 'use', 'main')
        assert {x['id']: x['name'] for x in self.api('dag', 'list', output='json')} == {}
        self.api('branch', 'delete', 'foopy')
        assert sorted(self.api('branch', 'list').split('\n')) == ['main']
        assert self.api('repo', 'gc') == f'{expr[0].uri}'
