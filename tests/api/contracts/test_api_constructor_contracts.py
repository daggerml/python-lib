from __future__ import annotations

import inspect
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from daggerml._core import Dml


def _config(*, project_home: str = "/tmp/project", remote_root: str | None = "s3://bucket/root", user: str = "tester"):
    return SimpleNamespace(
        project_home=project_home,
        db_path=f"{project_home}/.dml/db",
        remote=SimpleNamespace(
            root=remote_root,
            prune_age_seconds=123,
            fetch_workers=9,
        ),
        default=SimpleNamespace(
            db_map_size_headroom=1024,
            db_map_size_max=2048,
            branch_name="trunk",
        ),
        user=user,
        config_home="/tmp/config",
    )


def test_api_ctor_001__dml_constructor_and_init_expose_full_config_surface() -> None:
    constructor_params = list(inspect.signature(Dml.__init__).parameters)
    init_params = list(inspect.signature(Dml.init).parameters)
    clone_params = list(inspect.signature(Dml.clone).parameters)

    assert constructor_params == [
        "self",
        "project_home",
        "db_path",
        "db_map_size_headroom",
        "db_map_size_max",
        "default_branch_name",
        "remote_root",
        "remote_prune_age_seconds",
        "remote_fetch_workers",
        "user",
        "config_home",
    ]

    fetch = inspect.signature(Dml.fetch).parameters
    pull = inspect.signature(Dml.pull).parameters
    assert list(fetch) == ["self", "revision", "dep", "depth", "unshallow"]
    assert fetch["depth"].kind is inspect.Parameter.KEYWORD_ONLY
    assert fetch["depth"].default is None
    assert fetch["unshallow"].default is False
    assert list(pull) == ["self", "ff_only", "depth"]
    assert pull["depth"].kind is inspect.Parameter.KEYWORD_ONLY
    assert pull["depth"].default is None
    assert init_params == [
        "project_home",
        "db_path",
        "db_map_size_headroom",
        "db_map_size_max",
        "default_branch_name",
        "remote_root",
        "remote_prune_age_seconds",
        "remote_fetch_workers",
        "user",
        "config_home",
        "branch",
    ]
    assert clone_params == [
        "revision",
        "project_home",
        "db_path",
        "db_map_size_headroom",
        "db_map_size_max",
        "default_branch_name",
        "remote_root",
        "remote_prune_age_seconds",
        "remote_fetch_workers",
        "user",
        "config_home",
        "depth",
    ]


def test_api_ctor_002__constructor_maps_python_kwargs_to_canonical_config_vars() -> None:
    resolved = _config()

    with (
        patch("daggerml._core.dml.Config.resolve", return_value=resolved) as resolve,
        patch("daggerml._core.dml.DmlDB"),
    ):
        Dml(
            project_home="/tmp/project",
            db_path="/tmp/project/custom-db",
            db_map_size_headroom=11,
            db_map_size_max=22,
            default_branch_name="release",
            remote_root="s3://bucket/root",
            remote_prune_age_seconds=33,
            remote_fetch_workers=44,
            user="tester",
            config_home="/tmp/config",
        )

    assert resolve.call_args.kwargs["explicit"] == {
        "project_home": "/tmp/project",
        "db_path": "/tmp/project/custom-db",
        "default.db_map_size_headroom": 11,
        "default.db_map_size_max": 22,
        "default.branch_name": "release",
        "remote.root": "s3://bucket/root",
        "remote.prune_age_seconds": 33,
        "remote.fetch_workers": 44,
        "user": "tester",
        "config_home": "/tmp/config",
    }


def test_api_ctor_003__from_config_vars_passes_flattened_keys_directly_to_shared_resolution() -> None:
    resolved = _config(project_home="/tmp/from-config")
    config_vars = {
        "project_home": "/tmp/from-config",
        "remote.root": "s3://bucket/root",
        "default.branch_name": "release",
        "remote.fetch_workers": 17,
    }

    with (
        patch("daggerml._core.dml.Config.resolve", return_value=resolved) as resolve,
        patch("daggerml._core.dml.DmlDB"),
    ):
        dml = Dml.from_config_vars(config_vars)

    assert dml._explicit_config == config_vars
    assert resolve.call_args.kwargs["explicit"] == config_vars


def test_api_ctor_004__init_reuses_shared_config_surface_and_returns_runtime() -> None:
    bootstrap = _config(project_home="/tmp/bootstrap", remote_root="s3://bucket/bootstrap")
    runtime = SimpleNamespace(_config=_config(project_home="/tmp/bootstrap", user="worker"), _db=MagicMock())
    head = MagicMock()
    head.lock.return_value = nullcontext()
    head.get_head.side_effect = FileNotFoundError()

    with patch("daggerml._core.dml.Config.init", return_value=bootstrap), patch(
        "daggerml._core.dml.Dml.from_config_vars", return_value=runtime
    ) as from_config_vars, patch("daggerml._core.dml.Head", return_value=head):
        result = Dml.init(
            project_home="/tmp/requested",
            db_path="/tmp/bootstrap/custom-db",
            db_map_size_headroom=11,
            db_map_size_max=22,
            default_branch_name="release",
            remote_root="s3://bucket/bootstrap",
            remote_prune_age_seconds=33,
            remote_fetch_workers=44,
            user="worker",
            config_home="/tmp/override-config",
        )

    assert from_config_vars.call_args.args == (
        {
            "project_home": "/tmp/bootstrap",
            "db_path": "/tmp/bootstrap/custom-db",
            "default.db_map_size_headroom": 11,
            "default.db_map_size_max": 22,
            "default.branch_name": "release",
            "remote.root": "s3://bucket/bootstrap",
            "remote.prune_age_seconds": 33,
            "remote.fetch_workers": 44,
            "user": "worker",
            "config_home": "/tmp/override-config",
        },
    )
    runtime._db.init.assert_called_once_with()
    head.init.assert_called_once_with(None, "trunk")
    assert result is runtime
