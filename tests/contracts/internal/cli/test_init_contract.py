"""Unit and integration tests for init CLI functionality."""

import tempfile
from argparse import ArgumentParser, Namespace
from pathlib import Path
from unittest.mock import patch

import pytest

from daggerml._cli.init import execute_init, setup_init_parser
from daggerml._internal._db import Ref
from daggerml._internal.types import DmlRepoError


@patch("daggerml._cli.init.Dml.init")
def test_execute_init_does_not_forward_branch_argument(mock_dml_init):
    mock_dml_init.return_value = {
        "project_home": "/repo",
        "remote_uri": "",
        "user": None,
        "config_home": None,
        "created": {"db": True, "config": True},
    }

    args = Namespace(
        config_home=None,
        project_home=None,
        remote_project=None,
        remote_root="s3://test-bucket/test-prefix",
        no_hooks=True,
    )

    execute_init(args)

    mock_dml_init.assert_called_once()
    assert "branch" not in mock_dml_init.call_args.kwargs
    assert mock_dml_init.call_args.kwargs["user"] is None


class TestSetupInitParser:
    """Test init parser setup."""

    def test_parser_creation(self):
        parser = ArgumentParser()
        setup_init_parser(parser)
        args = parser.parse_args(["--remote-root", "s3://bucket/prefix"])
        assert args.remote_root == "s3://bucket/prefix"


class TestExecuteInit:
    """Test init command execution helper."""

    def test_execute_init_initializes_current_directory(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.chdir(temp_dir)
            args = Namespace(
                config_home=temp_dir,
                remote_root="s3://test-bucket/test-prefix",
                remote_project=None,
                no_hooks=True,
            )
            with patch("daggerml._internal.dml.Dml.fetch") as mock_fetch:
                result = execute_init(args)
            expected_repo = Path(temp_dir)
            repo_path = result["project_home"]
            assert repo_path is not None
            assert Path(repo_path).resolve() == expected_repo.resolve()
            assert result["remote_uri"] == "s3://test-bucket/test-prefix"
            assert result["created"] == {"db": True, "config": True}
            assert (expected_repo / ".dml" / "config.toml").exists()
            assert (expected_repo / ".dml" / "db").exists()
            mock_fetch.assert_not_called()

    def test_execute_init_allows_local_only_init(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.chdir(temp_dir)
            monkeypatch.delenv("DML_REMOTE_ROOT", raising=False)
            args = Namespace(
                config_home=None,
                project_home=None,
                remote_project=None,
                remote_root=None,
                no_hooks=True,
            )
            result = execute_init(args)
            assert result["project_home"] == str(Path(temp_dir).resolve())
            assert result["remote_uri"] == ""

    def test_execute_init_accepts_remote_project_without_name(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.chdir(temp_dir)
            args = Namespace(
                config_home=None,
                project_home=None,
                remote_project="dml://alice/demo",
                remote_root="s3://test-bucket/test-prefix",
                no_hooks=True,
            )
            with patch("daggerml._internal.dml.Dml.fetch", return_value=Ref("commit:9")):
                result = execute_init(args)
            assert result["project_home"] == str(Path(temp_dir).resolve())
            assert result["remote_uri"] == "s3://test-bucket/test-prefix"
            assert result["created"] == {"db": True, "config": True}

    def test_execute_init_rejects_remote_project_without_remote_root(self):
        args = Namespace(
            config_home=None,
            project_home=None,
            remote_project="dml://alice/demo",
            remote_root=None,
            no_hooks=True,
        )
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(DmlRepoError, match="remote.root is required"):
                execute_init(args)

    def test_execute_init_uses_env_remote_root_for_remote_project(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.chdir(temp_dir)
            monkeypatch.setenv("DML_REMOTE_ROOT", "s3://test-bucket/test-prefix")
            args = Namespace(
                config_home=None,
                project_home=None,
                remote_project="dml://alice/demo",
                remote_root=None,
                no_hooks=True,
            )
            with patch("daggerml._internal.dml.Dml.fetch", return_value=Ref("commit:9")):
                result = execute_init(args)
            assert result["remote_uri"] == "s3://test-bucket/test-prefix"
