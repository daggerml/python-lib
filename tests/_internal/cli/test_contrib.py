from __future__ import annotations

import json
import sys
from argparse import ArgumentParser, Namespace
from io import StringIO
from unittest.mock import patch

from daggerml._cli.contrib import execute_contrib_status, setup_contrib_parser


class TestSetupContribParser:
    def test_parser_creation(self):
        parser = ArgumentParser()
        setup_contrib_parser(parser)
        args = parser.parse_args(["status"])
        assert args.subcommand == "status"


class TestExecuteContribStatus:
    @patch("daggerml._cli.contrib.contrib_status.status")
    def test_execute_contrib_status(self, mock_status):
        mock_status.return_value = {"schema_version": 1}
        result = execute_contrib_status(Namespace())
        assert result == {"schema_version": 1}


class TestTopLevelContribCli:
    @patch("daggerml.contrib.status.status")
    def test_dml_contrib_status_outputs_json(self, mock_status):
        from daggerml._cli import cli

        mock_status.return_value = {
            "schema_version": 1,
            "summary": {"has_errors": False},
            "adapters": [],
            "executors": [],
            "codecs": [],
            "diagnostics": [],
        }

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.argv = ["dml", "contrib", "status"]
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        try:
            cli()
            out = sys.stdout.getvalue()
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr

        assert json.loads(out) == mock_status.return_value

    def test_dml_help_lists_contrib(self):
        from daggerml._cli import cli

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.argv = ["dml", "--help"]
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        try:
            try:
                cli()
            except SystemExit:
                pass
            out = sys.stdout.getvalue()
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr

        assert "contrib" in out
