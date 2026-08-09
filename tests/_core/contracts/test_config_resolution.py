from __future__ import annotations

import json

import pytest

from daggerml._core.config import Config, flatten_dict, unflatten_dict, validate_remote_root


def test_precedence_explicit_over_env_project_global_default(tmp_path, monkeypatch) -> None:
    config_home = tmp_path / "config"
    project_home = tmp_path / "project"
    project_home.mkdir()
    (config_home).mkdir()
    (config_home / "config.json").write_text(
        json.dumps({"remote": {"fetch_workers": 3}, "user": "global"}), encoding="utf-8"
    )
    (project_home / ".dml").mkdir()
    (project_home / ".dml" / "config.json").write_text(
        json.dumps({"remote": {"fetch_workers": 5}, "user": "project"}), encoding="utf-8"
    )
    monkeypatch.setenv("DML_REMOTE_FETCH_WORKERS", "7")

    config = Config.resolve(
        {"config_home": str(config_home), "project_home": str(project_home), "remote.fetch_workers": 11}
    )

    assert config.remote.fetch_workers == 11
    assert config.db_path == str(project_home / ".dml" / "db")


def test_flatten_unflatten_round_trip() -> None:
    nested = {"remote": {"root": "s3://bucket/root", "fetch_workers": 4}, "user": "me"}

    assert unflatten_dict(flatten_dict(nested)) == nested


@pytest.mark.parametrize("key", ["remote.fetch_workers", "default.db_map_size_headroom", "remote.prune_age_seconds"])
def test_positive_integer_config_rejects_non_positive(tmp_path, key: str) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        Config.resolve({"project_home": str(tmp_path), key: 0})


def test_removed_remote_project_config_and_environment_are_ignored(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / ".dml" / "config.json"
    config_path.parent.mkdir()
    config_path.write_text(json.dumps({"remote": {"project": "dml://acme/demo"}}), encoding="utf-8")
    monkeypatch.setenv("DML_REMOTE_PROJECT", "dml://ignored/project")
    monkeypatch.delenv("DML_REMOTE_ROOT", raising=False)

    config = Config.resolve({"project_home": str(tmp_path)})

    assert config.remote.root is None
    assert json.loads(config_path.read_text(encoding="utf-8")) == {"remote": {"project": "dml://acme/demo"}}


@pytest.mark.parametrize("key", ["remote.project", "remote.remotes"])
def test_removed_remote_configuration_keys_are_rejected(tmp_path, key: str) -> None:
    config = Config.init(tmp_path)

    with pytest.raises(ValueError, match="Unsupported configuration key"):
        config.update(key, "removed", scope="local")


@pytest.mark.parametrize("root", ["s3://bucket", "s3://bucket/prefix/"])
def test_remote_root_accepts_s3_bucket_or_prefix(root: str) -> None:
    assert validate_remote_root(root) == root.rstrip("/")


@pytest.mark.parametrize("root", ["", "file:///tmp", "s3://", "s3://bucket///"])
def test_remote_root_rejects_invalid_non_empty_values(root: str) -> None:
    if root == "":
        assert validate_remote_root(root) == ""
    else:
        with pytest.raises(ValueError):
            validate_remote_root(root)
