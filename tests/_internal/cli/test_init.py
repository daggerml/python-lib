"""Unit and integration tests for init CLI functionality."""

import json
import tempfile
from argparse import ArgumentParser, Namespace
from pathlib import Path
from unittest.mock import patch

import pytest

from daggerml._cli.init import execute_init, setup_init_parser
from daggerml._internal.types import DmlRepoError


class TestSetupInitParser:
    """Test init parser setup."""

    def test_parser_creation(self):
        parser = ArgumentParser()
        setup_init_parser(parser)
        args = parser.parse_args(["my-repo"])
        assert args.name == "my-repo"


class TestExecuteInit:
    """Test init command execution helper."""

    def test_execute_init_initializes_current_directory(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.chdir(temp_dir)
            args = Namespace(
                name="my-repo",
                config_home=temp_dir,
                repo=None,
                owner=None,
                branch=None,
                project_uri=None,
                remote_uri="s3://test-bucket/test-prefix",
                no_hooks=True,
            )
            result = execute_init(args)
            expected_repo = Path(temp_dir)
            repo_path = result["repo_path"]
            assert repo_path is not None
            assert Path(repo_path).resolve() == expected_repo.resolve()
            assert result["name"] == "my-repo"
            assert result["head"] == "head:main"
            assert (expected_repo / ".dml" / "config.toml").exists()
            assert (expected_repo / ".dml" / "db").exists()

    def test_execute_init_rejects_path_separators(self):
        args = Namespace(name="bad/name", config_home="~/.config/dml/", repo=None)
        with pytest.raises(ValueError, match="must not contain path separators"):
            execute_init(args)

    def test_execute_init_accepts_project_uri_without_name(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.chdir(temp_dir)
            args = Namespace(
                name=None,
                config_home=None,
                repo=None,
                owner=None,
                branch=None,
                project_uri="dml://alice/demo#main",
                remote_uri="s3://test-bucket/test-prefix",
                no_hooks=True,
            )
            result = execute_init(args)
            assert result["name"] is None
            assert result["head"] == "head:main"

    def test_execute_init_rejects_name_with_project_uri(self):
        args = Namespace(
            name="demo",
            config_home=None,
            repo=None,
            owner=None,
            branch=None,
            project_uri="dml://alice/demo#main",
            remote_uri="s3://test-bucket/test-prefix",
            no_hooks=True,
        )
        with pytest.raises(
            ValueError,
            match=(
                "NAME and --project-uri are mutually exclusive; provide NAME to derive project URI "
                "or use --project-uri for an explicit URI"
            ),
        ):
            execute_init(args)

    def test_execute_init_requires_resolved_user_for_name_mode(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.chdir(temp_dir)
            monkeypatch.setenv("USER", "")
            monkeypatch.setattr("daggerml._internal.config.getuser", lambda: (_ for _ in ()).throw(RuntimeError()))
            monkeypatch.setattr("daggerml._internal.config.gethostname", lambda: (_ for _ in ()).throw(RuntimeError()))
            args = Namespace(
                name="demo",
                config_home=None,
                repo=None,
                owner=None,
                branch=None,
                project_uri=None,
                remote_uri="s3://test-bucket/test-prefix",
                no_hooks=True,
            )
            with pytest.raises(DmlRepoError, match="user is required to derive project URI from NAME"):
                execute_init(args)

    def test_execute_init_uses_repo_flag_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            explicit = Path(temp_dir) / "repo-from-flag"
            explicit.mkdir()
            args = Namespace(
                name=None,
                config_home="~/.config/dml/",
                repo=str(explicit),
                owner=None,
                branch=None,
                project_uri=None,
                remote_uri="s3://test-bucket/test-prefix",
                no_hooks=True,
            )
            result = execute_init(args)
            assert result["repo_path"] == str(explicit)
            assert result["name"] is None
            assert (explicit / ".dml" / "db").exists()

    def test_execute_init_uses_env_when_flag_missing(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.setenv("DML_CONFIG_HOME", temp_dir)
            monkeypatch.chdir(temp_dir)
            args = Namespace(
                name="from-env",
                config_home=None,
                repo=None,
                owner=None,
                branch=None,
                project_uri=None,
                remote_uri="s3://test-bucket/test-prefix",
                no_hooks=True,
            )
            result = execute_init(args)
            expected_repo = Path(temp_dir)
            repo_path = result["repo_path"]
            assert repo_path is not None
            assert Path(repo_path).resolve() == expected_repo.resolve()
            assert (expected_repo / ".dml" / "db").exists()

    def test_execute_init_requires_remote_when_project_uri_present(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.chdir(temp_dir)
            args = Namespace(
                name=None,
                config_home=None,
                repo=None,
                owner=None,
                branch=None,
                project_uri="dml://alice/demo#main",
                remote_uri="",
                no_hooks=True,
            )
            with pytest.raises(DmlRepoError, match="remote.uri is required"):
                execute_init(args)

    def test_execute_init_recovery_without_project_uri_does_not_require_remote(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dml = root / ".dml"
            dml.mkdir(parents=True)
            (dml / "config.toml").write_text('[project]\nuri = "dml://alice/demo#main"\n[remote]\nuri = "s3://bucket/prefix"\n')
            monkeypatch.chdir(temp_dir)
            args = Namespace(
                name="demo",
                config_home=None,
                repo=None,
                owner=None,
                branch=None,
                project_uri=None,
                remote_uri=None,
                no_hooks=True,
            )
            with patch("daggerml._cli.init.DmlOps.init") as mock_init:
                mock_init.return_value = {
                    "db_path": str(root / ".dml" / "db"),
                    "head": "head:main",
                }
                result = execute_init(args)

            assert result["head"] == "head:main"
            assert mock_init.call_args.kwargs["path"] is None


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

    def test_init_creates_repo(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.chdir(temp_dir)
            stdout, stderr = self.run_cli_command(
                ["init", "--config-home", temp_dir, "--remote-uri", "s3://test-bucket/test-prefix", "named-repo"]
            )
            assert not stderr
            payload = json.loads(stdout.strip())
            expected_repo = Path(temp_dir)
            repo_path = payload["repo_path"]
            assert repo_path is not None
            assert Path(repo_path).resolve() == expected_repo.resolve()
            assert payload["head"] == "head:main"
            assert (expected_repo / ".dml" / "db").exists()

    def test_init_with_db_path_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            explicit = Path(temp_dir) / "db-only"
            explicit.mkdir()
            stdout, stderr = self.run_cli_command(
                ["--repo", str(explicit), "init", "--remote-uri", "s3://test-bucket/test-prefix"]
            )
            assert not stderr
            payload = json.loads(stdout.strip())
            assert payload["repo_path"] == str(explicit)
            assert payload["name"] is None
            assert (explicit / ".dml" / "db").exists()
