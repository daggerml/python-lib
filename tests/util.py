import unittest
from contextlib import contextmanager
from tempfile import TemporaryDirectory

import daggerml as dml


@contextmanager
def use_repo():
    with (
        TemporaryDirectory(prefix='dml-test-wd-') as d0,
        TemporaryDirectory(prefix='dml-test-wd-') as d1
    ):
        flags = {'config-dir': d0, 'project-dir': d1}
        dml._api(*dml.Dag._to_flags(flags), 'repo', 'create', 'test')
        dml._api(*dml.Dag._to_flags(flags), 'project', 'init', 'test')
        yield flags


class DmlTestBase(unittest.TestCase):

    def setUp(self):
        self.pylib = use_repo()
        self.flags = self.pylib.__enter__()

    def tearDown(self):
        self.pylib.__exit__(None, None, None)

    def new(self, name, message):
        return dml.new(name, message, flags=self.flags)
