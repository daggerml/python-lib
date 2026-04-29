from __future__ import annotations

import os
import sys
from argparse import ArgumentParser, Namespace
from io import StringIO

import pytest

from daggerml._cli.config import execute_config, setup_config_parser
from daggerml._internal.types import DmlRepoError


class TestSetupConfigParser:
    def test_parser_creation(self):
        parser = ArgumentParser()
        setup_config_parser(parser)
        args = parser.parse_args(["project.uri"])
        assert args.func is execute_config


class TestExecuteConfig:
    def test_set_and_get_local_value(self, tmp_path):
        args_set = Namespace(repo=str(tmp_path), global_scope=False, key="project.uri", value=["dml://alice/demo#main"])
        assert execute_config(args_set) == ""
        args_get = Namespace(repo=str(tmp_path), global_scope=False, key="project.uri", value=[])
        assert execute_config(args_get) == "dml://alice/demo#main"

    def test_rejects_invalid_global_key(self, tmp_path):
        args = Namespace(repo=str(tmp_path), global_scope=True, key="project.uri", value=["dml://alice/demo#main"])
        with pytest.raises(DmlRepoError, match="not valid in global scope"):
            execute_config(args)


class TestTopLevelConfigCli:
    def test_dml_config_get_prints_plain_text(self, tmp_path):
        dml_dir = tmp_path / ".dml"
        dml_dir.mkdir()
        (dml_dir / "config.toml").write_text('[project]\nuri = "dml://alice/demo#main"\n')

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        old_cwd = os.getcwd()
        sys.argv = ["dml", "config", "project.uri"]
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        os.chdir(str(tmp_path))
        try:
            cli = __import__("daggerml._cli", fromlist=["cli"]).cli
            cli()
            out = sys.stdout.getvalue()
        finally:
            os.chdir(old_cwd)
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr

        assert out == "dml://alice/demo#main\n"

    def test_dml_config_set_prints_empty_line(self, tmp_path):
        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        old_cwd = os.getcwd()
        sys.argv = ["dml", "config", "project.uri", "dml://alice/demo#main"]
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        os.chdir(str(tmp_path))
        try:
            cli = __import__("daggerml._cli", fromlist=["cli"]).cli
            cli()
            out = sys.stdout.getvalue()
        finally:
            os.chdir(old_cwd)
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr

        assert out == "\n"
        payload = (tmp_path / ".dml" / "config.toml").read_text()
        assert "dml://alice/demo#main" in payload
