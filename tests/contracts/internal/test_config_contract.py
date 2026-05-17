import os
from unittest.mock import Mock, patch

import pytest

from daggerml import Dml
from daggerml._internal.config import DmlConfig


def test_config_waterfall_defaults_env_explicit():
    cfg = DmlConfig.resolve(
        defaults={"remote.project": "dml://alice/defaults"},
        env={"DML_REMOTE_PROJECT": "dml://alice/env"},
        explicit={"remote.project": "dml://alice/explicit"},
    )
    assert cfg.remote.project == "dml://alice/explicit"


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
    monkeypatch.setenv("DML_REMOTE_PROJECT", "dml://alice/demo")
    dml = Dml()
    assert dml._context.project_home == "/tmp/from-env"
    assert dml._context.remote_uri == dml._context.config.remote.root


def test_dml_constructor_rejects_legacy_aliases():
    with pytest.raises(TypeError):
        Dml(repo="/tmp/test-repo")


def test_remote_config_from_canonical_env():
    cfg = DmlConfig.resolve(
        env={
            "DML_REMOTE_ROOT": "s3://bucket/project",
        }
    )
    assert cfg.remote.root == "s3://bucket/project"


def test_remote_fetch_workers_defaults_to_16():
    cfg = DmlConfig.resolve(env={})
    assert cfg.remote.fetch_workers == 16


def test_remote_fetch_workers_can_be_set_from_env():
    cfg = DmlConfig.resolve(env={"DML_REMOTE_FETCH_WORKERS": "24"})
    assert cfg.remote.fetch_workers == 24


def test_remote_fetch_workers_rejects_invalid_values():
    with pytest.raises(ValueError, match="remote.fetch_workers must be a positive integer"):
        DmlConfig.resolve(env={"DML_REMOTE_FETCH_WORKERS": "0"})


def test_remote_project_stays_branchless_and_sets_db_path():
    cfg = DmlConfig.resolve(
        explicit={
            "project.home": "/tmp/demo",
            "remote.project": "dml://alice/demo",
        },
        env={"DML_DEFAULT_BRANCH": "stable"},
    )
    assert cfg.remote.project == "dml://alice/demo"
    assert cfg.db.path == "/tmp/demo/.dml/db"


def test_global_scope_omits_project_config(tmp_path):
    dml_dir = tmp_path / ".dml"
    dml_dir.mkdir()
    (dml_dir / "config.toml").write_text(
        """
[remote]
project = "dml://alice/demo"
""".strip()
        + "\n"
    )
    cfg = DmlConfig.resolve(scope="global", explicit={"project.home": str(tmp_path)})
    assert cfg.remote.project is None


def test_resolution_precedence_global_project_env_explicit(tmp_path):
    config_home = tmp_path / "cfg"
    config_home.mkdir()
    (config_home / "config.toml").write_text('[defaults]\nbranch = "global"\n')
    project_dir = tmp_path / "repo"
    (project_dir / ".dml").mkdir(parents=True)
    (project_dir / ".dml" / "config.toml").write_text(
        """
[remote]
project = "dml://alice/demo"
""".strip()
        + "\n"
    )

    cfg_from_project = DmlConfig.resolve(
        explicit={"project.home": str(project_dir)},
        env={"DML_CONFIG_HOME": str(config_home)},
    )
    assert cfg_from_project.remote.project == "dml://alice/demo"

    cfg_from_env = DmlConfig.resolve(
        explicit={"project.home": str(project_dir)},
        env={"DML_CONFIG_HOME": str(config_home), "DML_REMOTE_PROJECT": "dml://alice/env"},
    )
    assert cfg_from_env.remote.project == "dml://alice/env"

    cfg = DmlConfig.resolve(
        explicit={"project.home": str(project_dir), "remote.project": "dml://alice/explicit"},
        env={"DML_CONFIG_HOME": str(config_home), "DML_REMOTE_PROJECT": "dml://alice/env"},
    )
    assert cfg.remote.project == "dml://alice/explicit"


def test_remote_project_rejects_tag_form():
    with pytest.raises(ValueError, match="must not include a branch or tag"):
        DmlConfig.resolve(explicit={"remote.project": "dml://alice/demo@v1"})


def test_remote_project_rejects_branch_selector():
    with pytest.raises(ValueError, match="must not include a branch or tag"):
        DmlConfig.resolve(explicit={"remote.project": "dml://alice/demo#main"})


def test_remote_config_defaults_to_empty_string(tmp_path):
    old = os.getcwd()
    os.chdir(tmp_path)
    try:
        cfg = DmlConfig.resolve(env={"DML_CONFIG_HOME": str(tmp_path / "cfg")})
        assert cfg.remote.root == ""
    finally:
        os.chdir(old)


def test_project_home_defaults_to_cwd_when_unset(tmp_path, monkeypatch):
    monkeypatch.delenv("DML_PROJECT_HOME", raising=False)
    old = os.getcwd()
    os.chdir(tmp_path)
    try:
        cfg = DmlConfig.resolve(env={})
        assert cfg.project.home == str(tmp_path)
    finally:
        os.chdir(old)


@patch("daggerml._internal.ops.remote.RemoteOps")
def test_dml_ops_remote_uses_configured_fetch_workers(mock_remote_ops):
    from daggerml._internal.ops import DmlOps

    ops = DmlOps(path="/tmp/repo", remote_root="s3://bucket/prefix", _db=Mock())
    with patch("daggerml._internal.ops.DmlConfig.resolve", return_value=Mock(remote=Mock(fetch_workers=9))):
        ops.remote(client=object())

    kwargs = mock_remote_ops.call_args.kwargs
    assert kwargs["bucket"] == "bucket"
    assert kwargs["prefix"] == "prefix/dml"
    assert kwargs["fetch_workers"] == 9
