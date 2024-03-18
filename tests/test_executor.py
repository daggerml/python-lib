import os
import shutil
import sys
import unittest
from tempfile import NamedTemporaryFile
from uuid import uuid4

import daggerml as dml
import daggerml.executor as ec2
from tests.util import DmlTestBase

REMOTE_INFO = None
if os.getenv('DML_TEST_REMOTE_USER'):
    REMOTE_INFO = {
        'user': os.getenv('DML_TEST_REMOTE_USER'),
        'host': os.getenv('DML_TEST_REMOTE_HOST'),
        'port': os.getenv('DML_TEST_REMOTE_PORT'),
        'key': os.getenv('DML_TEST_REMOTE_KEY'),
    }


class TestExecutorLocal(DmlTestBase):

    @unittest.skipIf(REMOTE_INFO is None, 'no remote (set REMOTE_{IP,PORT,KEY})')
    def test_remote_exec(self):
        remote = ec2.RemoteSsh(**REMOTE_INFO)
        with remote.ssh() as ssh:
            stdin, stdout, stderr = ssh.exec_command('echo foopy')
            assert stdout.read() == b'foopy\n'
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
        dag = self.new('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x):
            return {k: v**2 for k, v in x.items()}
        resp = pyex.call(dag, f, l0)
        assert dag.get_value(resp) == {k: v**2 for k, v in data.items()}

    @unittest.skipIf(shutil.which('hatch') is None, 'Hatch not in path')
    def test_local_hatch(self):
        pyex = ec2.HatchPyExecutor(ec2.Local(), 'test-env')
        dag = self.new('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x):
            import pandas as pd
            series = pd.Series({k: v**2 for k, v in x.items()})
            return series.to_dict()
        resp = pyex.call(dag, f, l0)
        assert dag.get_value(resp) == {k: v**2 for k, v in data.items()}

    @unittest.skipIf(shutil.which('conda') is None, 'Conda not in path')
    def test_local_conda(self):
        pyex = ec2.CondaPyExecutor(ec2.Local(), 'base')
        dag = self.new('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x):
            return {k: v**2 for k, v in x.items()}
        resp = pyex.call(dag, f, l0)
        assert dag.get_value(resp) == {k: v**2 for k, v in data.items()}

    def test_local_pyex_error(self):
        pyex = ec2.PyExecutor(ec2.Local(), sys.executable)
        dag = self.new('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x):
            import pandas as pd
            series = pd.Series({k: v**2 for k, v in x.items()})
            return series.to_dict()
        with self.assertRaisesRegex(dml.Error, r"No module named 'pandas'"):
            pyex.call(dag, f, l0)


class TestExecutorInProc(DmlTestBase):

    def test_error(self):
        pyex = ec2.InProcExecutor()
        dag = self.new('test-dag0', 'this is the test dag')
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
        dag = self.new('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x):
            return {k: v**2 for k, v in x.items()}
        resp = pyex.call(dag, f, l0)
        assert dag.get_value(resp) == {k: v**2 for k, v in data.items()}
