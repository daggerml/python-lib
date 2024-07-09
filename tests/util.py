import unittest
from contextlib import contextmanager
from tempfile import TemporaryDirectory

from click.testing import CliRunner
from daggerml_cli.cli import cli

import daggerml as dml
import daggerml.core


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


def _api(*args):
    runner = CliRunner()
    result = runner.invoke(cli, args)
    return result.output


class DmlTestBase(unittest.TestCase):

    def setUp(self):
        self.pylib = use_repo()
        self.flags = self.pylib.__enter__()
        daggerml.core._api = _api

    def tearDown(self):
        self.pylib.__exit__(None, None, None)

    def new(self, name, message):
        return dml.new(name, message, api_flags=self.flags)
