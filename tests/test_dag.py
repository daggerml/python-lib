import os
import unittest
from contextlib import contextmanager
from tempfile import TemporaryDirectory

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
            daggerml.util.CLI_FLAGS[:] = ['--config-dir', tmpd0, '--project-dir', tmpd1]
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


class TestDag(unittest.TestCase):

    def setUp(self):
        self.tmpd0 = TemporaryDirectory(prefix='dml-test-wd-')
        self.tmpd1 = TemporaryDirectory(prefix='dml-test-db-')
        self.d0 = self.tmpd0.__enter__()
        self.d1 = self.tmpd1.__enter__()
        self.olddir = os.getcwd()
        os.chdir(self.d0)
        daggerml.util.CLI_FLAGS[:] = ['--project-dir', self.d1, '--config-dir', self.d0]

    def tearDown(self):
        os.chdir(self.olddir)
        self.tmpd0.__exit__(None, None, None)
        self.tmpd1.__exit__(None, None, None)

    def test_base(self):
        print(f'{self.d0 = } -- {self.d1 = }')
        assert 1 == 1
