import os
import unittest
from tempfile import TemporaryDirectory
from typing import Any, Dict

import daggerml as dml
import daggerml.util


class TestExecutorLocal(unittest.TestCase):

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

    def test_local_process_sdk(self):
        import sys

        from daggerml.executor.local_process import run_local_proc
        dag = dml.Dag('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x: Dict[Any, int]) -> Dict[Any, int]:
            return {k: v**2 for k, v in x.items()}
        n1 = run_local_proc(dag, f, l0, python_interpreter=sys.executable,
                            preamble=['from typing import Any, Dict'])
        assert isinstance(n1, dml.Node)
        assert n1.unroll() == {k: v**2 for k, v in data.items()}

    def test_local_process_sdk_mp(self):
        import asyncio
        import sys

        from daggerml.executor.local_process import aio_run_local_proc
        dag = dml.Dag('test-dag0', 'this is the test dag')
        data = [{'asdf': 12}, {'qwer': 48}]
        l0, l1 = [dag.put(d) for d in data]

        def f(x: Dict[str, int]) -> Dict[Any, int]:
            return {k: v**2 for k, v in x.items()}

        async def submit():
            async def tmp(l, i):
                if i > 0:
                    asyncio.sleep(i)
                return await aio_run_local_proc(
                    dag, f, l, python_interpreter=sys.executable, preamble=['from typing import Any, Dict']
                )
            resp = await asyncio.gather(tmp(l0, 0), tmp(l1, 0.1))
            return resp
        
        ns = asyncio.run(submit())
        for n, d in zip(ns, data):
            assert isinstance(n, dml.Node)
            assert n.unroll() == {k: v**2 for k, v in d.items()}
