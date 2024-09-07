import json
import logging
import logging.config
import unittest
from unittest.mock import patch

from click.testing import CliRunner
from daggerml_cli.cli import cli

import daggerml as dml

logging_config = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
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
        self.api_patcher = patch('daggerml.Api', Api)
        self.api_patcher.start()
        self.api = dml.Api(initialize=True)
        self.ctx = self.api.__enter__()
        logging.config.dictConfig(logging_config)

    def tearDown(self):
        self.api_patcher.stop()
        self.ctx.__exit__(None, None, None)

    def new(self, name=None, message='', dump=None):
        return self.api.new_dag(name=name, message=message, dump=dump)
