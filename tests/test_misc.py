from glob import glob
from pathlib import Path
from tempfile import TemporaryDirectory

import daggerml as dml
import daggerml.executor as dx
from tests.util import DmlTestBase


def rel_to(x, rel):
    return str(Path(x).relative_to(rel))


def ls_r(path):
    return [rel_to(x, path) for x in glob(f"{path}/**", recursive=True)]


def get_range_through_torch(dag):
    import torch
    print('getting expr...')
    n = dag.expr[1].value()
    dag.commit(torch.arange(n).tolist())


class TestMisc(DmlTestBase):

    def test_bytes(self):
        with TemporaryDirectory() as tmpd:
            cache = dx.Cache(f'{tmpd}/foo')
            assert cache.get() is None
            assert ls_r(tmpd) == ['.', 'foo.lock']
            data = {'x': 4}
            cache.put(data)
            assert cache.get() == data
            data = {'y': 8}
            cache.put(data)
            assert cache.get() == data

    def test_create_delete(self):
        with TemporaryDirectory() as tmpd:
            cache = dx.Cache(f'{tmpd}/bar')
            data = {'x': 4}
            cache.put(data)
            assert cache.get() == data
            with cache.lock():
                data = {'y': 8}
                cache.put(data)
                assert cache.get() == data
            cache.delete()
            assert cache.get() is None

    def test_conda_exec(self):
        # Note: this will fail unless you have a conda env named torch with pytorch and dml installed
        n = 6
        with dml.Api(initialize=True) as api:
            with api.new_dag('foo', 'bar') as dag:
                lx = dx.Local()
                fn = lx.make_fn(dag, get_range_through_torch, 'conda', 'torch')
                resp = lx.run(dag, fn, n)
                assert resp.get_result().value() == list(range(n))

    def test_fn_indentation(self):
        # Note: this will fail unless you have a conda env named torch with pytorch and dml installed
        def foo(dag):
            import torch
            n = dag.expr[1].value()
            dag.commit(torch.arange(n).tolist())
        n = 6
        with dml.Api(initialize=True) as api:
            with api.new_dag('foo', 'bar') as dag:
                lx = dx.Local()
                fn = lx.make_fn(dag, foo, 'conda', 'torch')
                resp = lx.run(dag, fn, n)
                assert resp.get_result().value() == list(range(n))
