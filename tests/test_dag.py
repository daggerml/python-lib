import json
import os
import unittest
from contextlib import contextmanager
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any, Dict

import pytest

import daggerml as dml
import daggerml.util
from daggerml import core


@contextmanager
def cd(newdir):
    olddir = os.getcwd()
    newdir = os.path.expanduser(newdir)
    try:
        os.chdir(newdir)
        yield newdir
    finally:
        os.chdir(olddir)


class TestCore(unittest.TestCase):

    def test_core(self):
        with TemporaryDirectory(prefix='dml-test-') as tmpd0, TemporaryDirectory(prefix='dml-test-') as tmpd1:
            daggerml.set_flags({
                'config-dir': tmpd0, 'project-dir': tmpd1
            })
            # ['--config-dir', tmpd0, '--project-dir', tmpd1]
            with cd(tmpd0):
                daggerml.util._api('repo', 'create', 'test')
                daggerml.util._api('project', 'init', 'test')
                r0 = core.create_dag('dag0', 'this is the first dag')
                assert isinstance(r0, core.Repo)
                d0 = {'asdf': 12}
                n0 = r0.put_literal(d0)
                assert isinstance(n0, core.Ref)
                assert isinstance(n0(), core.Node)
                assert isinstance(n0().data, core.Literal)
                assert n0().value().value.keys() == d0.keys()
                assert n0().value().value['asdf']().value == d0['asdf']
                resp = r0.commit(n0)
                assert resp is None
                r1 = core.create_dag('dag1', 'second dag')
                n1 = r1.put_load('dag0')
                assert isinstance(n1, core.Ref)
                assert isinstance(n1(), core.Node)
                assert isinstance(n1().data, core.Load)
                assert n1().value is not None
                assert n1().value().value['asdf']().value == d0['asdf']
                r12 = r1.begin([n1])
                assert r12.dag().expr == [n1]
                assert r12.parent_dag() == r1.dag()
                n2 = r12.commit(r12.put_literal(2))
                assert n2().value().value == 2
                assert r1.commit(n2) is None
        daggerml.revert_flags()


class TestApi(unittest.TestCase):

    def setUp(self):
        self.tmpd0 = TemporaryDirectory(prefix='dml-test-wd-')
        self.tmpd1 = TemporaryDirectory(prefix='dml-test-db-')
        self.d0 = self.tmpd0.__enter__()
        self.d1 = self.tmpd1.__enter__()
        self.olddir = os.getcwd()
        os.chdir(self.d0)
        daggerml.set_flags({
            'config-dir': self.d0, 'project-dir': self.d1
        })
        # daggerml.util.CLI_FLAGS[:] = ['--project-dir', self.d1, '--config-dir', self.d0]
        daggerml.util._api('repo', 'create', 'test')
        daggerml.util._api('project', 'init', 'test')

    def tearDown(self):
        os.chdir(self.olddir)
        daggerml.revert_flags()
        self.tmpd0.__exit__(None, None, None)
        self.tmpd1.__exit__(None, None, None)

    def test_basic(self):
        dag = dml.Dag('test-dag0', 'this is the test dag')
        assert isinstance(dag, dml.Dag)
        l0 = dag.put({'asdf': 12})
        assert isinstance(l0, dml.Node)
        assert list(l0.value.keys()) == ['asdf']
        rsrc = dml.Resource({'a': 1, 'b': 2})
        r0 = dag.put(rsrc)
        f0 = dag.start_fn(r0, l0, l0)
        assert isinstance(f0, dml.Dag)
        assert f0.parent_dag == dag
        assert hasattr(f0, 'repo')
        assert isinstance(f0.repo, core.Repo)
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
        dag = dml.Dag('test', 'whiodfwshf')
        js = dag.dump_state()
        d2 = dml.Dag.from_state(js)
        assert d2.repo == dag.repo
        with NamedTemporaryFile() as tmpf:
            with open(tmpf.name, 'w') as f:
                json.dump(js, f)
            assert d2 == dml.load(tmpf.name)

    def test_in_process_sdk(self):
        # from aaron.dml import run_fn
        def run_fn(dag, f, *args):
            f0 = dag.start_fn(dag.put(dml.Resource(f.__name__)), *args)
            assert isinstance(f0, dml.Dag)
            assert f0.parent_dag == dag
            assert [x.value for x in f0.expr] == [dml.Resource(f.__name__), *[x.value for x in args]]
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

    def test_local_process_sdk(self):
        import sys

        from daggerml.executor.local_process import run_local_proc
        dag = dml.Dag('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x: dml.Node) -> Dict[Any, int]:
            return {k: v**2 for k, v in x.unroll().items()}
        n1 = run_local_proc(dag, f, l0, python_interpreter=sys.executable,
                            preamble=['from typing import Any, Dict'])
        assert isinstance(n1, dml.Node)
        assert n1.unroll() == {k: v**2 for k, v in data.items()}

    def test_local_process_sdk_mp(self):
        import asyncio
        import sys

        from daggerml.executor.local_process import async_run_local_proc
        dag = dml.Dag('test-dag0', 'this is the test dag')
        data = [{'asdf': 12}, {'qwer': 48}]
        l0, l1 = [dag.put(d) for d in data]

        def f(x: dml.Node) -> Dict[Any, int]:
            return {k: v**2 for k, v in x.unroll().items()}

        async def submit():
            async def tmp(l, i):
                if i > 0:
                    asyncio.sleep(i)
                return await async_run_local_proc(
                    dag, f, l, python_interpreter=sys.executable, preamble=['from typing import Any, Dict']
                )
            resp = await asyncio.gather(tmp(l0, 0), tmp(l1, 0.1))
            return resp
        
        ns = asyncio.run(submit())
        for n, d in zip(ns, data):
            assert isinstance(n, dml.Node)
            assert n.unroll() == {k: v**2 for k, v in d.items()}

    # def test_local_process_sdk_error(self):
    #     import sys
    #
    #     from daggerml.executor.local_process import run_local_proc
    #     dag = dml.Dag('test-dag0', 'this is the test dag')
    #     data = {'asdf': 12, 'qwer': 0}
    #     l0 = dag.put(data)
    #     def f(x: dml.Node) -> Dict[Any, int]:
    #         return {k: 1 / v for k, v in x.unroll().items()}
    #     n1 = run_local_proc(dag, f, l0, python_interpreter=sys.executable,
    #                         preamble=['from typing import Any, Dict'])
    #     assert isinstance(n1, dml.Node)
    #     assert n1.unroll() == {k: v**2 for k, v in data.items()}

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
            'set': {12, 13},
            'resource': core.Resource({'a': 1, 'b': 2}),
            # FIXME: sets, dicts, and Resources aren't hashible...
            # 'compound': [{'a': 32, 'b': {core.Resource({'a': 1, 'b': 2})}}],
        }
        l0 = dag.put(data)
        assert isinstance(l0, dml.Node)
        assert l0.unroll() == data
        dag.commit(l0)
        dag = dml.Dag('test-dag1', 'this is the test dag')
        n0 = dag.load('test-dag0')
        assert n0.unroll() == data

    def test_cache_basic(self):
        dag = dml.Dag('test-dag0', 'this is the test dag')
        stash = [23]
        def f(fndag):
            return fndag.put(stash[0])
        rsrc = core.Resource({'a': 1, 'b': 2})
        args = dag.put(rsrc), dag.put(12), dag.put(13)
        f0 = dag.start_fn(*args)
        assert f0.repo.parent_dag is not None
        assert f0.repo.cached_dag is None
        n0 = f0.commit(f(f0), cache=None)
        assert n0.value == 23
        f1 = dag.start_fn(*args)
        assert f1.repo.cached_dag is not None
        n1 = f1.commit(None)
        assert n1.value == 23

    def test_cache_apply(self):
        dag = dml.Dag('test-dag0', 'this is the test dag')
        stash = [23]
        def f(fndag):
            return fndag.put(stash[0])
        rsrc = core.Resource({'a': 1, 'b': 2})
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
        rsrc = core.Resource({'a': 1, 'b': 2})
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
        rsrc = core.Resource({'a': 1, 'b': 2})
        with pytest.raises(ValueError):
            dag.apply(f, dag.put(rsrc), dag.put(12), dag.put(13))
        # check using cached value
        stash[0] = False
        assert isinstance(dag.apply(f, dag.put(rsrc), dag.put(12), dag.put(13)).error, core.Error)
        assert dag.apply(f, dag.put(rsrc), dag.put(12), dag.put(13), cache=False).value is False

    def test_errors(self):
        dag = dml.Dag('test-dag0', 'this is the test dag')
        dag.commit(core.Error('asdf', code='qwer'))
        dag = dml.Dag('test-dag1', 'this is the test dag')
        n0 = dag.load('test-dag0')
        assert isinstance(n0, dml.Node)
        assert isinstance(n0.error, core.Error)
        with pytest.raises(core.Error):
            print(n0.value)

    def test_contextmanager(self):
        with pytest.raises(ZeroDivisionError):
            with dml.Dag('test-dag0', 'this is the test dag') as dag:
                dag.put(1 / 0)

        with dml.Dag('test-dag1', 'this is the test dag') as dag:
            n0 = dag.load('test-dag0')
            assert isinstance(n0, dml.Node)
            assert isinstance(n0.error, core.Error)
            dag.commit(dag.put(n0.error.code))
