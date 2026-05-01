import json
import tempfile
from pathlib import Path

import pytest


pytestmark = pytest.mark.slow


class TestInitCLIIntegration:
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

    def test_init_cli_creates_repo_at_config_home_when_name_is_provided(self, monkeypatch):
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

    def test_init_cli_respects_repo_flag_for_db_path_only_mode(self):
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
