from __future__ import annotations

import json

import pytest

from daggerml._core.config import Config, flatten_dict, unflatten_dict, validate_remote_root
from daggerml._core.types import DmlRepoError


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


@pytest.mark.parametrize(
    ("payload", "key"),
    [
        ({"remote": {"project": "dml://acme/demo"}}, "remote.project"),
        ({"remote": {"remotes": {}}}, "remote.remotes"),
        ({"remote": {"unknown": True}}, "remote.unknown"),
        ({"unknown": {}}, "unknown"),
        ({"remote": "s3://bucket"}, "remote"),
        ({"remote.root": "s3://bucket"}, "remote.root"),
    ],
)
def test_invalid_persisted_configuration_is_rejected_with_source(tmp_path, payload, key) -> None:
    config_path = tmp_path / ".dml" / "config.json"
    config_path.parent.mkdir()
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DmlRepoError) as exc_info:
        Config.resolve({"project_home": str(tmp_path)})

    assert str(config_path) in str(exc_info.value)
    assert key in str(exc_info.value)


@pytest.mark.parametrize("payload", [None, [], "config", 1])
def test_persisted_configuration_root_must_be_an_object(tmp_path, payload) -> None:
    config_path = tmp_path / ".dml" / "config.json"
    config_path.parent.mkdir()
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DmlRepoError, match=f"{config_path}.*<root>"):
        Config.resolve({"project_home": str(tmp_path)})


@pytest.mark.parametrize(
    ("payload", "key"),
    [
        ({"user": None}, "user"),
        ({"default": {"branch_name": ["main"]}}, "default.branch_name"),
        ({"remote": {"fetch_workers": True}}, "remote.fetch_workers"),
        ({"default": {"db_map_size_max": "1024"}}, "default.db_map_size_max"),
    ],
)
def test_persisted_configuration_rejects_noncanonical_leaf_types(tmp_path, payload, key) -> None:
    config_path = tmp_path / ".dml" / "config.json"
    config_path.parent.mkdir()
    config_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(DmlRepoError, match=rf"{config_path}.*{key}"):
        Config.resolve({"project_home": str(tmp_path)})


def test_invalid_global_configuration_is_validated_before_precedence(tmp_path) -> None:
    config_home = tmp_path / "config"
    config_home.mkdir()
    config_path = config_home / "config.json"
    config_path.write_text(json.dumps({"remote": {"project": "removed"}}), encoding="utf-8")

    with pytest.raises(DmlRepoError, match=rf"{config_path}.*remote\.project"):
        Config.resolve({"config_home": str(config_home), "remote.root": "s3://explicit"})


def test_update_validates_complete_document_without_mutation(tmp_path) -> None:
    config = Config.init(tmp_path)
    config_path = tmp_path / ".dml" / "config.json"
    original = '{"remote":{"project":"removed"}}\n'
    config_path.write_text(original, encoding="utf-8")

    with pytest.raises(DmlRepoError, match=rf"{config_path}.*remote\.project"):
        config.update("user", "me", scope="local")

    assert config_path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize(
    "name",
    [
        "DML_DEFAULT_BRANCH",
        "DML_PROJECT_NAME",
        "DML_PROJECT_OWNER",
        "DML_REMOTE_PROJECT",
        "DML_REMOTE_NAME",
        "DML_BRANCH",
        "DML_REMOTE",
        "DML_REMOTE_BUCKET",
        "DML_REMOTE_PREFIX",
        "DML_REPO",
        "DML_DYNAMODB_TABLE",
        "DML_REMOTE_CACHE",
        "DML_HOOK",
    ],
)
def test_retired_environment_variables_are_unmapped(tmp_path, monkeypatch, name) -> None:
    before = Config.resolve({"project_home": str(tmp_path)})
    monkeypatch.setenv(name, "removed")

    after = Config.resolve({"project_home": str(tmp_path)})

    assert after == before


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
