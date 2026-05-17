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
        name="demo",
        config_home=None,
        project_home=None,
        owner=None,
        project_uri=None,
        remote_uri="s3://test-bucket/test-prefix",
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
        args = parser.parse_args(["my-repo"])
        assert args.name == "my-repo"


class TestExecuteInit:
    """Test init command execution helper."""

    def test_execute_init_initializes_current_directory(self, monkeypatch):
        monkeypatch.setenv("USER", "alice")
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.chdir(temp_dir)
            args = Namespace(
                name="my-repo",
                config_home=temp_dir,
                remote_uri="s3://test-bucket/test-prefix",
                no_hooks=True,
            )
            with patch("daggerml._internal.dml.Dml.fetch", return_value=Ref("commit:9")):
                result = execute_init(args)
            expected_repo = Path(temp_dir)
            repo_path = result["project_home"]
            assert repo_path is not None
            assert Path(repo_path).resolve() == expected_repo.resolve()
            assert result["remote_uri"] == "s3://test-bucket/test-prefix"
            assert result["created"] == {"db": True, "config": True}
            assert (expected_repo / ".dml" / "config.toml").exists()
            assert (expected_repo / ".dml" / "db").exists()

    def test_execute_init_rejects_path_separators(self):
        args = Namespace(name="bad/name", config_home="~/.config/dml/", project_home=None)
        with pytest.raises(ValueError, match="Invalid project name: 'bad/name'"):
            execute_init(args)

    def test_execute_init_accepts_project_uri_without_name(self, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_dir:
            monkeypatch.chdir(temp_dir)
            args = Namespace(
                name=None,
                config_home=None,
                project_home=None,
                project_uri="dml://alice/demo",
                remote_uri="s3://test-bucket/test-prefix",
                no_hooks=True,
            )
            with patch("daggerml._internal.dml.Dml.fetch", return_value=Ref("commit:9")):
                result = execute_init(args)
            assert result["project_home"] == str(Path(temp_dir).resolve())
            assert result["remote_uri"] == "s3://test-bucket/test-prefix"
            assert result["created"] == {"db": True, "config": True}

    def test_execute_init_rejects_name_with_project_uri(self):
        args = Namespace(
            name="demo",
            config_home=None,
            project_home=None,
            owner=None,
            project_uri="dml://alice/demo",
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
                project_home=None,
                owner=None,
                project_uri=None,
                remote_uri="s3://test-bucket/test-prefix",
                no_hooks=True,
            )
            with pytest.raises(DmlRepoError, match="user is required to derive project URI from NAME"):
                execute_init(args)
