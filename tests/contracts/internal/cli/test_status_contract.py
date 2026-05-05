from __future__ import annotations

import json
import sys
from argparse import ArgumentParser, Namespace
from io import StringIO

from daggerml._cli.status import execute_status, setup_status_parser


class TestSetupStatusParser:
    def test_parser_creation(self):
        parser = ArgumentParser()
        setup_status_parser(parser)
        args = parser.parse_args([])
        assert args.func is execute_status


class TestExecuteStatus:
    def test_execute_status_returns_config_dict(self, tmp_path):
        args = Namespace(repo=str(tmp_path), remote_root="s3://bucket/project")
        result = execute_status(args)
        assert set(result.keys()) == {"project", "db", "remote", "user", "default_branch", "hooks", "config_home"}
        assert result["project"]["home"] == str(tmp_path)
        assert set(result["project"].keys()) == {"home", "uri"}
        assert result["remote"]["uri"] == "s3://bucket/project"
        assert result["remote"]["fetch_workers"] == 16


class TestTopLevelStatusCli:
    def test_dml_status_outputs_json(self):
        from daggerml._cli import cli

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.argv = ["dml", "status"]
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
        assert "project" in payload
        assert "remote" in payload
