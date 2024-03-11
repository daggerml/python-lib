import os
import sys
import unittest
from tempfile import NamedTemporaryFile, TemporaryDirectory
from uuid import uuid4

import daggerml as dml
import daggerml.executor as ec2
import daggerml.util

REMOTE_INFO = None
if os.getenv('DML_TEST_REMOTE_USER'):
    REMOTE_INFO = {
        'user': os.getenv('DML_TEST_REMOTE_USER'),
        'host': os.getenv('DML_TEST_REMOTE_IP'),
        'port': os.getenv('DML_TEST_REMOTE_PORT'),
        'key': os.getenv('DML_TEST_REMOTE_KEY'),
    }


class TestExecutorLocal(unittest.TestCase):

    def setUp(self):
        self.tmpd0 = TemporaryDirectory(prefix='dml-test-wd-')
        self.tmpd1 = TemporaryDirectory(prefix='dml-test-db-')
        self.d0 = self.tmpd0.__enter__()
        self.d1 = self.tmpd1.__enter__()
        daggerml.set_flags({
            'config-dir': self.d0, 'project-dir': self.d1
        })
        daggerml.util._api('repo', 'create', 'test')
        daggerml.util._api('project', 'init', 'test')

    def tearDown(self):
        daggerml.revert_flags()
        self.tmpd0.__exit__(None, None, None)
        self.tmpd1.__exit__(None, None, None)

    @unittest.skipIf(REMOTE_INFO is None, 'no remote (set REMOTE_{IP,PORT,KEY})')
    def test_remote_exec(self):
        remote = ec2.RemoteSsh(**REMOTE_INFO)
        with remote.ssh() as ssh:
            stdin, stdout, stderr = ssh.exec_command('pwd')
            assert stdout.read() == bf'/home/{REMOTE_INFO["user"]}\n'
            assert stderr.read() == b''

    @unittest.skipIf(REMOTE_INFO is None, 'no remote (set REMOTE_{IP,PORT,KEY})')
    def test_remote_copy(self):
        remote = ec2.RemoteSsh(**REMOTE_INFO)
        fname = f'{uuid4().hex}.txt'
        text = 'random text'
        with NamedTemporaryFile(suffix='.txt') as tmpf:
            with open(tmpf.name, 'w') as f:
                f.write(text)
            remote.put(tmpf.name, fname)
        assert remote.cat(fname) == text
        with remote.ssh() as ssh:
            ssh.exec_command(f'rm "{fname}"')

    def test_local_pyex(self):
        pyex = ec2.PyExecutor(ec2.Local(), sys.executable)
        dag = dml.Dag('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x):
            return {k: v**2 for k, v in x.items()}
        resp = pyex.call(dag, f, l0)
        assert resp.unroll() == {k: v**2 for k, v in data.items()}

    def test_local_hatch(self):
        pyex = ec2.HatchPyExecutor(ec2.Local(), 'test-env')
        dag = dml.Dag('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x):
            import pandas as pd
            series = pd.Series({k: v**2 for k, v in x.items()})
            return series.to_dict()
        resp = pyex.call(dag, f, l0)
        assert resp.unroll() == {k: v**2 for k, v in data.items()}

    def test_local_pyex_error(self):
        pyex = ec2.PyExecutor(ec2.Local(), sys.executable)
        dag = dml.Dag('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x):
            import pandas as pd
            series = pd.Series({k: v**2 for k, v in x.items()})
            return series.to_dict()
        with self.assertRaisesRegex(dml.Error, r"No module named 'pandas'"):
            pyex.call(dag, f, l0)


class TestExecutorInProc(unittest.TestCase):

    def setUp(self):
        self.tmpd0 = TemporaryDirectory(prefix='dml-test-wd-')
        self.tmpd1 = TemporaryDirectory(prefix='dml-test-db-')
        self.d0 = self.tmpd0.__enter__()
        self.d1 = self.tmpd1.__enter__()
        daggerml.set_flags({
            'config-dir': self.d0, 'project-dir': self.d1
        })
        # daggerml.util.CLI_FLAGS[:] = ['--project-dir', self.d1, '--config-dir', self.d0]
        daggerml.util._api('repo', 'create', 'test')
        daggerml.util._api('project', 'init', 'test')

    def tearDown(self):
        daggerml.revert_flags()
        self.tmpd0.__exit__(None, None, None)
        self.tmpd1.__exit__(None, None, None)

    def test_error(self):
        pyex = ec2.InProcExecutor()
        dag = dml.Dag('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x):
            import pandas as pd
            series = pd.Series({k: v**2 for k, v in x.items()})
            return series.to_dict()
        with self.assertRaisesRegex(dml.Error, r"No module named 'pandas'"):
            pyex.call(dag, f, l0)

    def test_runs(self):
        pyex = ec2.InProcExecutor()
        dag = dml.Dag('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x):
            return {k: v**2 for k, v in x.items()}
        resp = pyex.call(dag, f, l0)
        assert resp.unroll() == {k: v**2 for k, v in data.items()}
