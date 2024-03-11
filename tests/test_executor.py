import sys
import unittest
from tempfile import NamedTemporaryFile, TemporaryDirectory
from uuid import uuid4

import daggerml as dml
import daggerml.executor as ec2
import daggerml.util


class TestExecutorLocal(unittest.TestCase):

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

    def test_remote_exec(self):
        remote = ec2.RemoteSsh('ubuntu', '34.211.226.207', 187, '~/.ssh/misc2-amn-k0.pem')
        with remote.ssh() as ssh:
            stdin, stdout, stderr = ssh.exec_command('pwd')
            assert stdout.read() == b'/home/ubuntu\n'
            assert stderr.read() == b''

    def test_remote_copy(self):
        remote = ec2.RemoteSsh('ubuntu', '34.211.226.207', 187, '~/.ssh/misc2-amn-k0.pem')
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

    def test_local_hatch_pyex(self):
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

    def test_in_proc(self):
        pyex = ec2.InProcExecutor()
        dag = dml.Dag('test-dag0', 'this is the test dag')
        data = {'asdf': 12}
        l0 = dag.put(data)
        def f(x):
            return {k: v**2 for k, v in x.items()}
        resp = pyex.call(dag, f, l0)
        assert resp.unroll() == {k: v**2 for k, v in data.items()}
