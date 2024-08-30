import json
import unittest

from click.testing import CliRunner
from daggerml_cli.cli import cli

import daggerml as dml


class Api(dml.Api):
    @staticmethod
    def _api(*args):
        runner = CliRunner()
        result = runner.invoke(cli, args)
        if result.exit_code != 0:
            raise RuntimeError(f'{result.output} ----- {result.return_value}')
        return result.output.strip()

    def jscall(self, *args):
        resp = self(*args)
        return [json.loads(x) for x in resp.split('\n') if len(x) > 0]


class DmlTestBase(unittest.TestCase):

    def setUp(self):
        # daggerml.core._api = _api
        self.api = Api(initialize=True)
        self.ctx = self.api.__enter__()

    def tearDown(self):
        self.ctx.__exit__(None, None, None)

    def new(self, name=None, message='', dump=None):
        return self.api.new_dag(name=name, message=message, dump=dump)
