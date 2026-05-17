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
                ["init", "--config-home", temp_dir, "--remote-root", "s3://test-bucket/test-prefix", "named-repo"]
            )
            assert not stderr
            payload = json.loads(stdout.strip())
            expected_repo = Path(temp_dir)
            project_home = payload["project_home"]
            assert project_home is not None
            assert Path(project_home).resolve() == expected_repo.resolve()
            assert payload["remote_uri"] == "s3://test-bucket/test-prefix"
            assert payload["created"] == {"db": True, "config": True}
            assert (expected_repo / ".dml" / "db").exists()

    def test_init_cli_requires_name_or_remote_project(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            explicit = Path(temp_dir) / "db-only"
            explicit.mkdir()
            stdout, stderr = self.run_cli_command(
                ["--project-home", str(explicit), "init", "--remote-root", "s3://test-bucket/test-prefix"]
            )
            assert not stdout
            payload = json.loads(stderr.strip())
            assert payload["type"] == "DmlRepoError"
            assert payload["command"] == "init"
            assert payload["error"] == "init: Either NAME or remote_project is required"
