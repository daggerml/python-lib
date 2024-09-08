import logging.config
import platform
import unittest
from unittest.mock import patch

from click.testing import CliRunner
from daggerml_cli.cli import cli

import daggerml as dml

SYSTEM = platform.system().lower()

logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'default',
        }
    },
    'root': {
        'level': 'WARNING',
        'handlers': ['console'],
    },
    'loggers': {
        'daggerml': {
            'level': 'DEBUG',  # or whatever level you want for your library
            'handlers': ['console'],
            'propagate': False,
        }
    }
}


def _api(*args):
    print('running patched cli via click')
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
        logging.config.dictConfig(logging_config)

    def tearDown(self):
        self.api_patcher.stop()
        self.ctx.__exit__(None, None, None)

    def new(self, name=None, message='', dump=None):
        return self.api.new_dag(name=name, message=message, dump=dump)
