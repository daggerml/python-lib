import os
from unittest.mock import Mock, patch

from daggerml import Dml
from daggerml._config import DmlConfig


def test_config_waterfall_defaults_env_explicit():
    cfg = DmlConfig.resolve(
        defaults={"branch": "defaults"},
        env={"DML_BRANCH": "env"},
        explicit={"branch": "explicit"},
    )
    assert cfg.branch == "explicit"


def test_repo_env_resolution():
    cfg = DmlConfig.resolve(
        env={"DML_REPO": "/repo/new"},
    )
    assert cfg.repo == "/repo/new"


def test_xdg_default_config_dir():
    cfg = DmlConfig.resolve(env={"XDG_CONFIG_HOME": "/tmp/xdg"})
    assert cfg.config_dir == "/tmp/xdg/dml"


def test_default_user_uses_env_user_and_hostname_shape():
    cfg = DmlConfig.resolve(env={"USER": "alice"})
    assert cfg.user is not None
    assert cfg.user.startswith("alice")
    assert "@" in cfg.user or cfg.user == "alice"


def test_path_values_expand_user():
    home = os.path.expanduser("~")
    cfg = DmlConfig.resolve(
        explicit={"repo": "~/repo", "config_dir": "~/cfg"},
    )
    assert cfg.repo == f"{home}/repo"
    assert cfg.config_dir == f"{home}/cfg"


def test_dml_uses_config_resolution_from_env(monkeypatch):
    monkeypatch.setenv("DML_REPO", "/tmp/from-env")
    monkeypatch.setenv("DML_BRANCH", "env-branch")
    dml = Dml()
    assert dml.repo == "/tmp/from-env"
    assert dml.branch == "env-branch"


def test_remote_config_from_canonical_env():
    cfg = DmlConfig.resolve(
        env={
            "DML_REMOTE_ROOT": "s3://bucket/project",
        }
    )
    assert cfg.remote.root == "s3://bucket/project"


@patch("daggerml.api.DmlOps.open")
def test_dml_ops_receives_remote_context(mock_open, monkeypatch):
    monkeypatch.setenv("DML_REMOTE_ROOT", "s3://bucket/project")
    mock_open.return_value = Mock(__enter__=Mock(), __exit__=Mock())

    dml = Dml(repo="/tmp/test-repo")
    _ = dml.ops

    mock_open.assert_called_once_with(
        "/tmp/test-repo",
        remote_root="s3://bucket/project",
    )
