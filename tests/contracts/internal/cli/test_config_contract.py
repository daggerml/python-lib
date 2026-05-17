from __future__ import annotations

import json
import os
import sys
from argparse import ArgumentParser, Namespace
from io import StringIO
from unittest.mock import patch

import pytest

from daggerml._cli.config import (
    execute_config_get,
    execute_config_set,
    execute_config_show,
    setup_config_parser,
)
from daggerml._internal.types import DmlRepoError
from tests import temporary_dml


class TestSetupConfigParser:
    def test_parser_creation(self):
        parser = ArgumentParser()
        setup_config_parser(parser)
        args = parser.parse_args(["show"])
        assert args.func is execute_config_show

    def test_get_and_set_help_include_examples(self):
        parser = ArgumentParser()
        setup_config_parser(parser)

        with pytest.raises(SystemExit):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                parser.parse_args(["get", "--help"])
        get_help = mock_stdout.getvalue()

        with pytest.raises(SystemExit):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                parser.parse_args(["set", "--help"])
        set_help = mock_stdout.getvalue()

        assert "dml config get remote.project" in get_help
        assert "dml config set remote.project dml://alice/demo" in set_help


class TestExecuteConfig:
    def test_set_and_get_local_value(self):
        with temporary_dml() as dml:
            args_set = Namespace(global_scope=False, key="remote.project", value=["dml://alice/demo"])
            assert execute_config_set(dml, args_set) == ""
            args_get = Namespace(global_scope=False, key="remote.project")
            assert execute_config_get(dml, args_get) == "dml://alice/demo"

    def test_rejects_invalid_global_key(self):
        with temporary_dml() as dml:
            args = Namespace(global_scope=True, key="remote.project", value=["dml://alice/demo"])
            with pytest.raises(DmlRepoError, match="not valid in global scope"):
                execute_config_set(dml, args)

    @patch("daggerml.contrib.status.status")
    def test_show_can_include_contrib_status(self, mock_status):
        mock_status.return_value = {"schema_version": 0}
        with temporary_dml() as dml:
            result = execute_config_show(dml, Namespace(contrib=True))
        assert result["contrib"] == {"schema_version": 0}


class TestTopLevelConfigCli:
    def test_dml_config_get_prints_plain_text(self, tmp_path):
        dml_dir = tmp_path / ".dml"
        dml_dir.mkdir()
        (dml_dir / "config.toml").write_text('[remote]\nproject = "dml://alice/demo"\n')

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        old_cwd = os.getcwd()
        sys.argv = ["dml", "config", "get", "remote.project"]
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

        assert out == "dml://alice/demo\n"

    def test_dml_config_show_outputs_json(self, tmp_path):
        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.argv = ["dml", "--project-home", str(tmp_path), "config", "show"]
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        try:
            cli = __import__("daggerml._cli", fromlist=["cli"]).cli
            cli()
            out = sys.stdout.getvalue()
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr

        payload = json.loads(out)
        assert "project" in payload
