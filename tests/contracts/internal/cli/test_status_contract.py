from __future__ import annotations

import json
import sys
from argparse import ArgumentParser, Namespace
from io import StringIO

from daggerml._cli.status import execute_status, setup_status_parser
from daggerml._internal import Dml


class TestSetupStatusParser:
    def test_parser_creation(self):
        parser = ArgumentParser()
        setup_status_parser(parser)
        args = parser.parse_args([])
        assert args.func is execute_status


class TestExecuteStatus:
    def test_execute_status_returns_repository_summary(self):
        with Dml.temporary() as dml:
            result = execute_status(dml, Namespace())
        assert set(result.keys()) == {"head", "branches", "dags", "indexes"}
        assert result["head"]["mode"] == "attached"
        assert "main" in result["branches"]


class TestTopLevelStatusCli:
    def test_dml_status_outputs_json(self, tmp_path):
        from daggerml._cli import cli

        with Dml.temporary(repo="repo") as dml:
            old_argv = sys.argv
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.argv = ["dml", "--project-home", dml._context.project_home, "status"]
            sys.stdout = StringIO()
            sys.stderr = StringIO()
            try:
                cli()
                out = sys.stdout.getvalue()
            finally:
                sys.argv = old_argv
                sys.stdout = old_stdout
                sys.stderr = old_stderr

        payload = json.loads(out)
        assert set(payload.keys()) == {"head", "branches", "dags", "indexes"}
