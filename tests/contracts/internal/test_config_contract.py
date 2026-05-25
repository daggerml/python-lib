import os
from contextlib import contextmanager
from unittest.mock import Mock, patch

import pytest

import daggerml._internal.dml as dml_module
from daggerml import Dml
from daggerml._internal._db import Ref
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
    assert cfg.project.home == "/repo/new"


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
    assert cfg.project.home == f"{home}/repo"


def test_dml_uses_config_resolution_from_env(monkeypatch):
    monkeypatch.setenv("DML_PROJECT_HOME", "/tmp/from-env")
    monkeypatch.setenv("DML_REMOTE_PROJECT", "dml://alice/demo")
    dml = Dml()
    assert dml._context.project_home == "/tmp/from-env"
    assert dml._context.remote_root == dml._context.config.remote.root


def test_execution_id_resolves_explicit_over_env():
    cfg = DmlConfig.resolve(
        env={"DML_EXECUTION_ID": "exec-env"},
        explicit={"execution.id": "exec-explicit"},
    )
    assert cfg.execution.id == "exec-explicit"


def test_execution_id_defaults_to_none_without_explicit_or_env():
    cfg = DmlConfig.resolve(env={})
    assert cfg.execution.id is None


def test_dml_uses_execution_id_from_env(monkeypatch):
    monkeypatch.setenv("DML_EXECUTION_ID", "exec-from-env")
    dml = Dml(project_home="/tmp/repo")
    assert dml._context.execution_id == "exec-from-env"


def test_dml_explicit_execution_id_overrides_env(monkeypatch):
    monkeypatch.setenv("DML_EXECUTION_ID", "exec-from-env")
    dml = Dml(project_home="/tmp/repo", execution_id="exec-explicit")
    assert dml._context.execution_id == "exec-explicit"


def test_runtime_start_fn_uses_created_execution_id_for_updates():
    dml = Dml(project_home="/tmp/repo", execution_id="exec-worker")
    fake_ops = Mock()
    fake_ops.start_fn.return_value = "result-node"

    with (
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_index_ops", return_value=fake_ops),
    ):
        result = dml.runtime.start_fn("idx-1", [Ref("node:abc")])

    assert result == "result-node"
    fake_ops.start_fn.assert_called_once_with(
        "idx-1",
        [Ref("node:abc")],
        kwargv=None,
        name=None,
    )


def test_runtime_start_fn_uses_positional_execution_id_only():
    dml = Dml(project_home="/tmp/repo")
    fake_ops = Mock()
    fake_ops.start_fn.return_value = None

    with (
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_index_ops", return_value=fake_ops),
    ):
        dml.runtime.start_fn("idx-root", [Ref("node:def")])

    fake_ops.start_fn.assert_called_once_with(
        "idx-root",
        [Ref("node:def")],
        kwargv=None,
        name=None,
    )


def test_runtime_commit_uses_created_execution_id_positionally():
    worker_dml = Dml(project_home="/tmp/repo", execution_id="exec-worker")
    root_dml = Dml(project_home="/tmp/repo")
    worker_ops = Mock()
    root_ops = Mock()
    worker_ops.commit.return_value = Ref("commit:" + "a" * 64)
    root_ops.commit.return_value = Ref("commit:" + "b" * 64)

    with (
        patch.object(dml_module, "with_db", side_effect=lambda _dml: _opened_db()),
        patch.object(dml_module, "make_index_ops", side_effect=[worker_ops, root_ops]),
    ):
        worker_dml.runtime.commit("idx-1", Ref("node:aaa"))
        root_dml.runtime.commit("idx-2", Ref("node:bbb"))

    worker_ops.commit.assert_called_once_with(
        "idx-1",
        Ref("node:aaa"),
        head=None,
        message=None,
        dag_name=None,
    )
    root_ops.commit.assert_called_once_with(
        "idx-2",
        Ref("node:bbb"),
        head=None,
        message=None,
        dag_name=None,
    )


@contextmanager
def _opened_db():
    yield Mock()


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


@patch("daggerml._internal.dml.RemoteOps")
def test_dml_ops_remote_uses_configured_fetch_workers(mock_remote_ops):
    from daggerml._internal.dml import Dml, make_remote_ops

    dml = Dml(project_home="/tmp/repo", remote_root="s3://bucket/prefix")
    object.__setattr__(dml._context.config.remote, "fetch_workers", 9)
    make_remote_ops(Mock(), dml)

    kwargs = mock_remote_ops.call_args.kwargs
    assert kwargs["bucket"] == "bucket"
    assert kwargs["prefix"] == "prefix/dml"
    assert kwargs["fetch_workers"] == 9
