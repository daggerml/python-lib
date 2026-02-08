"""Unit and integration tests for init CLI functionality."""

import json
import tempfile
from argparse import ArgumentParser, Namespace
from pathlib import Path

import pytest

from daggerml._cli.init import execute_init, setup_init_parser


class TestSetupInitParser:
    """Test init parser setup."""

    def test_parser_creation(self):
        parser = ArgumentParser()
        setup_init_parser(parser)
        args = parser.parse_args(["my-repo"])
        assert args.name == "my-repo"
        assert args.config_dir is None


class TestExecuteInit:
    """Test init command execution helper."""

    def test_execute_init_creates_repo_at_config_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            args = Namespace(name="my-repo", config_dir=temp_dir, repo=None)
            result = execute_init(args)
            expected_repo = Path(temp_dir) / "my-repo"
            assert result["repo_path"] == str(expected_repo)
            assert result["name"] == "my-repo"
            assert result["head"] == "head:main"
            assert expected_repo.exists()

    def test_execute_init_rejects_path_separators(self):
        args = Namespace(name="bad/name", config_dir="~/.config/dml/", repo=None)
        with pytest.raises(ValueError, match="must not contain path separators"):
            execute_init(args)

    def test_execute_init_requires_name_without_repo(self):
        args = Namespace(name=None, config_dir="~/.config/dml/", repo=None)
        with pytest.raises(ValueError, match="NAME is required when --repo is not provided"):
            execute_init(args)

    def test_execute_init_uses_repo_flag_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            explicit = Path(temp_dir) / "repo-from-flag"
            args = Namespace(name=None, config_dir="~/.config/dml/", repo=str(explicit))
            result = execute_init(args)
            assert result["repo_path"] == str(explicit)
            assert result["name"] is None
            assert explicit.exists()

    def test_execute_init_uses_env_config_dir_when_flag_missing(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.setenv("DML_CONFIG_DIR", temp_dir)
            args = Namespace(name="from-env", config_dir=None, repo=None)
            result = execute_init(args)
            expected_repo = Path(temp_dir) / "from-env"
            assert result["repo_path"] == str(expected_repo)
            assert expected_repo.exists()


class TestInitCLIIntegration:
    """Integration tests for init CLI command."""

    def run_cli_command(self, args):
        import sys
        from io import StringIO

        from daggerml._cli import cli

        old_argv = sys.argv
        old_stdout = sys.stdout
        old_stderr = sys.stderr

        sys.argv = ["dml"] + args
        stdout_capture = StringIO()
        stderr_capture = StringIO()
        sys.stdout = stdout_capture
        sys.stderr = stderr_capture

        try:
            cli()
            return stdout_capture.getvalue(), stderr_capture.getvalue()
        except SystemExit:
            return stdout_capture.getvalue(), stderr_capture.getvalue()
        finally:
            sys.argv = old_argv
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def test_init_creates_repo(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            stdout, stderr = self.run_cli_command(["init", "--config-dir", temp_dir, "named-repo"])
            assert not stderr
            payload = json.loads(stdout.strip())
            expected_repo = Path(temp_dir) / "named-repo"
            assert payload["repo_path"] == str(expected_repo)
            assert payload["head"] == "head:main"
            assert expected_repo.exists()

    def test_init_with_db_path_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            explicit = Path(temp_dir) / "db-only"
            stdout, stderr = self.run_cli_command(["--repo", str(explicit), "init"])
            assert not stderr
            payload = json.loads(stdout.strip())
            assert payload["repo_path"] == str(explicit)
            assert payload["name"] is None
            assert explicit.exists()
