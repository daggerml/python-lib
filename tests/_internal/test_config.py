import os
from unittest.mock import Mock, patch

import pytest

from daggerml import Dml
from daggerml._internal.config import DmlConfig


def test_config_waterfall_defaults_env_explicit():
    cfg = DmlConfig.resolve(
        defaults={"project.uri": "dml://alice/demo#defaults"},
        env={"DML_PROJECT_URI": "dml://alice/demo#env"},
        explicit={"project.uri": "dml://alice/demo#explicit"},
    )
    assert cfg.branch == "explicit"


def test_repo_env_resolution():
    cfg = DmlConfig.resolve(
        env={"DML_PROJECT_HOME": "/repo/new"},
    )
    assert cfg.repo == "/repo/new"


def test_default_user_uses_env_user_and_hostname_shape():
    cfg = DmlConfig.resolve(env={"USER": "alice"})
    assert cfg.user is not None
    assert cfg.user.startswith("alice")
    assert "@" in cfg.user or cfg.user == "alice"


def test_path_values_expand_user():
    home = os.path.expanduser("~")
    cfg = DmlConfig.resolve(
        explicit={"project.home": "~/repo"},
    )
    assert cfg.repo == f"{home}/repo"


def test_dml_uses_config_resolution_from_env(monkeypatch):
    monkeypatch.setenv("DML_PROJECT_HOME", "/tmp/from-env")
    monkeypatch.setenv("DML_PROJECT_URI", "dml://alice/demo#env-branch")
    dml = Dml()
    assert dml.repo == "/tmp/from-env"
    assert dml.branch == "env-branch"


def test_remote_config_from_canonical_env():
    cfg = DmlConfig.resolve(
        env={
            "DML_REMOTE_URI": "s3://bucket/project",
        }
    )
    assert cfg.remote.uri == "s3://bucket/project"


def test_project_uri_normalizes_branch_and_db_path():
    cfg = DmlConfig.resolve(
        explicit={
            "project.home": "/tmp/demo",
            "project.uri": "dml://alice/demo",
        },
        env={"DML_DEFAULT_BRANCH": "stable"},
    )
    assert cfg.project.uri == "dml://alice/demo#stable"
    assert cfg.project.branch == "stable"
    assert cfg.db.path == "/tmp/demo/.dml/db"


def test_global_scope_omits_project_config(tmp_path):
    dml_dir = tmp_path / ".dml"
    dml_dir.mkdir()
    (dml_dir / "config.toml").write_text(
        """
[project]
uri = "dml://alice/demo#feature"
""".strip()
        + "\n"
    )
    cfg = DmlConfig.resolve(scope="global", explicit={"project.home": str(tmp_path)})
    assert cfg.project.uri is None


def test_resolution_precedence_global_project_env_explicit(tmp_path):
    config_home = tmp_path / "cfg"
    config_home.mkdir()
    (config_home / "config.toml").write_text('[defaults]\nbranch = "global"\n')
    project_dir = tmp_path / "repo"
    (project_dir / ".dml").mkdir(parents=True)
    (project_dir / ".dml" / "config.toml").write_text(
        """
[project]
uri = "dml://alice/demo#project"
""".strip()
        + "\n"
    )

    cfg_from_project = DmlConfig.resolve(
        explicit={"project.home": str(project_dir)},
        env={"DML_CONFIG_HOME": str(config_home)},
    )
    assert cfg_from_project.project.uri == "dml://alice/demo#project"

    cfg_from_env = DmlConfig.resolve(
        explicit={"project.home": str(project_dir)},
        env={"DML_CONFIG_HOME": str(config_home), "DML_PROJECT_URI": "dml://alice/demo#env"},
    )
    assert cfg_from_env.project.uri == "dml://alice/demo#env"

    cfg = DmlConfig.resolve(
        explicit={"project.home": str(project_dir), "project.uri": "dml://alice/demo#explicit"},
        env={"DML_CONFIG_HOME": str(config_home), "DML_PROJECT_URI": "dml://alice/demo#env"},
    )
    assert cfg.project.uri == "dml://alice/demo#explicit"


def test_project_uri_rejects_tag_form():
    with pytest.raises(ValueError, match="branch, not a tag"):
        DmlConfig.resolve(explicit={"project.uri": "dml://alice/demo@v1"})


def test_branch_override_does_not_mask_tag_project_uri():
    with pytest.raises(ValueError, match="branch, not a tag"):
        DmlConfig.resolve(explicit={"project.uri": "dml://alice/demo@v1", "project.branch": "main"})


def test_remote_config_defaults_to_empty_string():
    cfg = DmlConfig.resolve(env={})
    assert cfg.remote.uri == ""


@patch("daggerml.api.DmlOps.open")
def test_dml_ops_receives_remote_context(mock_open, monkeypatch):
    monkeypatch.setenv("DML_REMOTE_URI", "s3://bucket/project")
    mock_open.return_value = Mock(__enter__=Mock(), __exit__=Mock())

    dml = Dml(repo="/tmp/test-repo")
    _ = dml.ops

    mock_open.assert_called_once_with(
        "/tmp/test-repo",
        remote_root="s3://bucket/project",
    )


@patch("daggerml.api.DmlOps.open")
def test_dml_ops_allows_local_access_without_remote(mock_open, monkeypatch):
    monkeypatch.delenv("DML_REMOTE_URI", raising=False)
    mock_open.return_value = Mock(__enter__=Mock(), __exit__=Mock())
    dml = Dml(repo="/tmp/test-repo")
    _ = dml.ops
    mock_open.assert_called_once_with("/tmp/test-repo", remote_root="")
