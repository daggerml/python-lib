import logging
import platform
import unittest
from unittest.mock import patch

from click.testing import CliRunner
from daggerml_cli.cli import cli

import daggerml as dml

SYSTEM = platform.system().lower()
logger = logging.getLogger(__name__)

def _api(*args):
    logger.debug('running patched cli via click')
    runner = CliRunner()
    result = runner.invoke(cli, args)
    if result.exit_code != 0:
        raise RuntimeError(f'{result.output} ----- {result.return_value}')
    return result.output.strip()


class DmlTestBase(unittest.TestCase):

    def setUp(self):
        self.api_patcher = patch('daggerml.core._api', _api)
        self.api_patcher.start()
        self.api = dml.Api(initialize=True)
        self.ctx = self.api.__enter__()
        # logging.config.dictConfig(logging_config)

    def tearDown(self):
        self.api_patcher.stop()
        self.ctx.__exit__(None, None, None)

    def new(self, name=None, message='', dump=None):
        return self.api.new_dag(name=name, message=message, dump=dump)
