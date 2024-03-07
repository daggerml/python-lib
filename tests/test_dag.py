import os
import unittest
from contextlib import contextmanager
from tempfile import TemporaryDirectory
from typing import Any, Dict

import pytest

import daggerml as dml
import daggerml.util
from daggerml.core import Error, Resource


@contextmanager
def cd(newdir):
    olddir = os.getcwd()
    newdir = os.path.expanduser(newdir)
    try:
        os.chdir(newdir)
        yield newdir
    finally:
        os.chdir(olddir)

@contextmanager
def use_repo():
    with (
        TemporaryDirectory(prefix='dml-test-wd-') as d0,
        TemporaryDirectory(prefix='dml-test-wd-') as d1
    ):
        daggerml.set_flags({
            'config-dir': d0, 'project-dir': d1
        })
        daggerml.util._api('repo', 'create', 'test')
        daggerml.util._api('project', 'init', 'test')
        yield
        daggerml.revert_flags()


class TestApi(unittest.TestCase):

    def setUp(self):
        self.pylib = use_repo()
        self.pylib.__enter__()

    def tearDown(self):
        self.pylib.__exit__(None, None, None)

    def test_basic(self):
        dag = dml.Dag('test-dag0', 'this is the test dag')
        assert isinstance(dag, dml.Dag)
        l0 = dag.put({'asdf': 12})
        assert isinstance(l0, dml.Node)
        assert list(l0.value.keys()) == ['asdf']
        rsrc = Resource({'a': 1, 'b': 2})
        r0 = dag.put(rsrc)
        f0 = dag.start_fn(r0, l0, l0)
        assert isinstance(f0, dml.Dag)
        assert f0.parent_dag == dag
        assert f0.expr[0].value == rsrc
        l1 = f0.put({'qwer': 23})
        assert isinstance(l1, dml.Node)
        assert l1.dag == f0
        n1 = f0.commit(l1)
        assert isinstance(n1, dml.Node)
        assert n1.dag == dag
        assert list(n1.value.keys()) == ['qwer']
        assert dag.commit(n1) is None
        dag = dml.Dag('test-dag1', 'this is the second test dag')
        n0 = dag.load('test-dag0')
        assert isinstance(n0, dml.Node)
        assert list(n0.value.keys()) == ['qwer']

    def test_dump_n_load(self):
        dag = dml.Dag('a', 'whiodfwshf')
        resource = dml.Resource({'asdf': 42})
        data = {'qwer': 23}
        r0 = dag.put(resource)
        n0 = dag.put(data)
        fndag = dag.start_fn(r0, n0)
        dump = [dml.dump_obj(dag.repo, x) for x in fndag.repo.dag().expr]
        with use_repo():
            dag2 = dml.Dag('b', 'hhwgqrhqwh')
            with self.assertRaises(RuntimeError):
                n0.unroll()
            load = [dml.load_obj(dag2.repo, x) for x in dump]
            load_nodes = [dml.Node(l, dag2) for l in load]
            assert [x.unroll() for x in load_nodes] == [resource, data]

    def test_in_process_sdk(self):
        # from aaron.dml import run_fn
        def run_fn(dag, f, *args):
            f0 = dag.start_fn(dag.put(Resource(f.__name__)), *args)
            assert isinstance(f0, dml.Dag)
            assert f0.parent_dag == dag
            assert [x.value for x in f0.expr] == [Resource(f.__name__), *[x.value for x in args]]
            result = f(*f0.expr[1:])
            result = f0.put(result)
            node = f0.commit(result)
            return node
        dag = dml.Dag('test-dag0', 'this is the test dag')
        l0 = dag.put({'asdf': 12})
        def f(x: dml.Node) -> Dict[Any, int]:
            return {k: v**2 for k, v in x.unroll().items()}
        # ====== start
        n1 = run_fn(dag, f, l0)
        # ====== end
        assert isinstance(n1, dml.Node)
        assert n1.dag == dag
        assert n1.unroll() == {'asdf': 144}
        assert dag.commit(n1) is None
        dag = dml.Dag('test-dag1', 'this is the second test dag')
        n0 = dag.load('test-dag0')
        assert isinstance(n0, dml.Node)
        assert n0.unroll() == n1.unroll()

    def test_literal(self):
        dag = dml.Dag('test-dag0', 'this is the test dag')
        data = {
            'int': 23,
            'float': 12.43,
            'bool': True,
            'null': None,
            'string': 'qwer',
            'list': [3, 4, 5],
            'map': {'a': 2, 'b': 'asdf'},
            'set': {12, 13, 'a', 3.4},
            'resource': Resource({'a': 1, 'b': 2}),
        }
        l0 = dag.put(data)
        assert isinstance(l0, dml.Node)
        assert l0.unroll() == data
        dag.commit(l0)
        dag = dml.Dag('test-dag1', 'this is the test dag')
        n0 = dag.load('test-dag0')
        assert n0.unroll() == data
        with self.assertRaisesRegex(TypeError, "unhashable type: 'dict'"):
            data = {Resource({'a': 8, 'b': 2})}

    def test_cache_basic(self):
        dag = dml.Dag('test-dag0', 'this is the test dag')
        stash = [23]
        def f(fndag):
            return fndag.put(stash[0])
        rsrc = Resource({'a': 1, 'b': 2})
        args = dag.put(rsrc), dag.put(12), dag.put(13)
        f0 = dag.start_fn(*args)
        n0 = f0.commit(f(f0), cache=None)
        assert n0.value == 23
        f1 = dag.start_fn(*args)
        n1 = f1.commit(None)
        assert n1.value == 23

    def test_cache_apply(self):
        dag = dml.Dag('test-dag0', 'this is the test dag')
        stash = [23]
        def f(fndag):
            return fndag.put(stash[0])
        rsrc = Resource({'a': 1, 'b': 2})
        args = dag.put(rsrc), dag.put(12), dag.put(13)
        assert dag.apply(f, *args).value == 23
        # check using cached value
        stash[0] = 40
        assert dag.apply(f, *args).value == 23
        # test ignoring cache
        assert dag.apply(f, *args, cache=False).value == 40
        # test ignoring cache doesn't fuck with cache
        assert dag.apply(f, *args).value == 23
        # test replace cache
        assert dag.apply(f, *args, cache=True).value == 40
        # test using replaced cache
        assert dag.apply(f, *args).value == 40

    def test_cache_datums(self):
        dag = dml.Dag('test-dag0', 'this is the test dag')
        stash = [23]
        def f(fndag):
            return fndag.put(stash[0])
        rsrc = Resource({'a': 1, 'b': 2})
        assert dag.apply(f, dag.put(rsrc), dag.put(12), dag.put(13)).value == 23
        # check using cached value
        stash[0] = 40
        assert dag.apply(f, dag.put(rsrc), dag.put(12), dag.put(13)).value == 23
        dag.commit(dag.put(12))
        # caching persists across dags
        dag0 = dml.Dag('test-dag1', 'this is another test dag')
        assert dag0.apply(f, dag0.put(rsrc), dag0.put(12), dag0.put(13)).value == 23

    def test_cache_errors(self):
        dag = dml.Dag('test-dag0', 'this is the test dag')
        stash = [True]
        def f(fndag):
            if stash[0]:
                raise ValueError('aaahhhh')
            return fndag.put(stash[0])
        rsrc = Resource({'a': 1, 'b': 2})
        with pytest.raises(ValueError):
            dag.apply(f, dag.put(rsrc), dag.put(12), dag.put(13))
        # check using cached value
        stash[0] = False
        assert isinstance(dag.apply(f, dag.put(rsrc), dag.put(12), dag.put(13)).error, Error)
        assert dag.apply(f, dag.put(rsrc), dag.put(12), dag.put(13), cache=False).value is False

    def test_errors(self):
        dag = dml.Dag('test-dag0', 'this is the test dag')
        dag.commit(Error('asdf', code='qwer'))
        dag = dml.Dag('test-dag1', 'this is the test dag')
        n0 = dag.load('test-dag0')
        assert isinstance(n0, dml.Node)
        assert isinstance(n0.error, Error)
        with pytest.raises(Error):
            print(n0.value)

    def test_contextmanager(self):
        with pytest.raises(ZeroDivisionError):
            with dml.Dag('test-dag0', 'this is the test dag') as dag:
                dag.put(1 / 0)

        with dml.Dag('test-dag1', 'this is the test dag') as dag:
            n0 = dag.load('test-dag0')
            assert isinstance(n0, dml.Node)
            assert isinstance(n0.error, Error)
            dag.commit(dag.put(n0.error.code))
