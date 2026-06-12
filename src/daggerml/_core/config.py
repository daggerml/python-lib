from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from getpass import getuser
from pathlib import Path
from socket import gethostname
from typing import Any, Literal, Mapping, cast, overload

from daggerml._core.types import DmlRepoError
from daggerml._core.uri import ProjectUri

_ENV_KEYS = {
    "config_home": "DML_CONFIG_HOME",
    "db_path": "DML_DB_PATH",
    "default.db_map_size_headroom": "DML_DEFAULT_DB_MAP_SIZE_HEADROOM",
    "default.db_map_size_max": "DML_DEFAULT_DB_MAP_SIZE_MAX",
    "default.branch_name": "DML_DEFAULT_BRANCH_NAME",
    "remote.prune_age_seconds": "DML_REMOTE_PRUNE_AGE_SECONDS",
    "project_home": "DML_PROJECT_HOME",
    "remote.project": "DML_REMOTE_PROJECT",
    "remote.root": "DML_REMOTE_ROOT",
    "remote.fetch_workers": "DML_REMOTE_FETCH_WORKERS",
    "user": "DML_USER",
}


def global_config_home() -> str:
    if os.getenv("XDG_CONFIG_HOME"):
        return os.path.join(os.path.expanduser(os.environ["XDG_CONFIG_HOME"]), "dml")
    return os.path.expanduser("~/.config/dml")


def default_user() -> str:
    user = os.getenv("USER")
    if not user:
        try:
            user = getuser()
        except Exception:
            user = "<unknown>"
    try:
        host = gethostname().split(".", 1)[0]
    except Exception:
        host = ""
    return f"{user}@{host}" if host else user


_DEFAULTS: dict[str, callable | str | None | int] = {
    "config_home": global_config_home,
    "db_path": None,
    "project_home": Path.cwd,
    "default.db_map_size_headroom": 1024 * 1024,
    "default.db_map_size_max": 10 * 1024**3,  # 10 GiB
    "default.branch_name": "main",
    "remote.prune_age_seconds": 24 * 3600,
    "remote.project": None,
    "remote.root": None,
    "remote.fetch_workers": 32,
    "user": default_user,
}


def validate_remote_root(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("remote.root must be a string")
    if not value:
        return ""
    if not value.startswith("s3://"):
        raise ValueError("remote.root must be s3://bucket or s3://bucket/prefix")
    rest = value[5:]
    if not rest:
        raise ValueError("remote.root must include a bucket name")
    bucket, sep, prefix = rest.partition("/")
    if not bucket or (sep and not prefix.strip("/")):
        raise ValueError("remote.root must be s3://bucket or s3://bucket/prefix")
    return value.rstrip("/")


def _coerce_path(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, Path):
        value = str(value)
    if not isinstance(value, str):
        raise ValueError(f"Expected path-like string, got {type(value).__name__}")
    return os.path.expanduser(value)


def _coerce_positive_int(value, *, key: str) -> int:
    parsed = value if isinstance(value, int) else int(str(value), 10)
    if parsed <= 0:
        raise ValueError(f"{key} must be a positive integer")
    return parsed


_COERCION_MAP = {
    "config_home": _coerce_path,
    "db_path": _coerce_path,
    "project_home": _coerce_path,
    "default.db_map_size_headroom": lambda v: _coerce_positive_int(v, key="default.db_map_size_headroom"),
    "default.db_map_size_max": lambda v: _coerce_positive_int(v, key="default.db_map_size_max"),
    "default.branch_name": str,
    "remote.prune_age_seconds": lambda v: _coerce_positive_int(v, key="remote.prune_age_seconds"),
    "remote.project": lambda v: str(ProjectUri.from_uri(str(v)).ensure_project(strict=True)),
    "remote.root": validate_remote_root,
    "remote.fetch_workers": lambda v: _coerce_positive_int(v, key="remote.fetch_workers"),
    "user": str,
}


def coalesce(name: str, explicit: Mapping[str, object], *configs) -> object:
    value = None
    if name in explicit and explicit[name] not in {None, ""}:
        value = explicit[name]
    elif name in _ENV_KEYS and os.getenv(_ENV_KEYS[name]):
        value = os.environ[_ENV_KEYS[name]]
    else:
        for config in configs:
            if name in config and config[name] not in {None, ""}:
                value = config[name]
                break
    if value is None:
        default = _DEFAULTS.get(name)
        value = default() if callable(default) else default
    if value is None:
        return None
    return _COERCION_MAP.get(name, lambda v: v)(value)


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, separators=(",", ":"), sort_keys=True) + "\n"
    # Write via a temp file so readers never observe truncated or partial JSON.
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
        tmp.write(payload)
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)


def flatten_dict(data: Mapping[str, Any]) -> dict[str, Any]:
    result = {}
    for key, value in data.items():
        if isinstance(value, dict):
            nested = flatten_dict(value)
            for nested_key, nested_value in nested.items():
                result[f"{key}.{nested_key}"] = nested_value
        else:
            result[key] = value
    return result


def unflatten_dict(data: dict[str, Any]) -> dict[str, Any]:
    result = {}
    for key, value in data.items():
        parts = key.split(".")
        current = result
        for part in parts[:-1]:
            if part not in current or not isinstance(current[part], dict):
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
    return result


@dataclass(frozen=True)
class DefaultSettings:
    db_map_size_headroom: int
    db_map_size_max: int
    branch_name: str


@dataclass(frozen=True)
class RemoteSettings:
    prune_age_seconds: int
    project: str | None
    root: str | None
    fetch_workers: int


@dataclass(frozen=True)
class Config:
    project_home: str
    db_path: str
    remote: RemoteSettings
    default: DefaultSettings
    user: str
    config_home: str

    @classmethod
    def resolve(cls, explicit: Mapping[str, object] | None = None) -> "Config":
        explicit = dict(explicit or {})
        config_home = cast(str, coalesce("config_home", explicit))
        glob_conf = flatten_dict(_read_json(Path(config_home) / "config.json"))
        project_home = cast(str, coalesce("project_home", explicit, glob_conf))
        proj_conf = flatten_dict(_read_json(Path(project_home) / ".dml" / "config.json"))
        config = unflatten_dict({k: coalesce(k, explicit, proj_conf, glob_conf) for k in _DEFAULTS.keys()})
        db_path = _coerce_path(config["db_path"])
        if db_path is None:
            db_path = str(Path(project_home) / ".dml" / "db")
        config["db_path"] = db_path
        config["project_home"] = project_home
        config["remote"] = RemoteSettings(**config["remote"])
        config["default"] = DefaultSettings(**config["default"])
        return cls(**config)

    @classmethod
    def init(
        cls,
        project_home: str | Path = ".",
        *,
        remote_root: str | None = None,
        remote_project: str | None = None,
    ) -> "Config":
        root = Path(project_home).resolve()
        if not root.exists():
            raise FileNotFoundError(f"{root} does not exist")
        dml_dir = root / ".dml"
        dml_dir.mkdir(parents=True, exist_ok=True)
        (dml_dir / "db").mkdir(parents=True, exist_ok=True)
        if not (dml_dir / ".gitignore").exists():
            (dml_dir / ".gitignore").write_text("db\nHEAD\nrefs\n", encoding="utf-8")
        if not (dml_dir / "config.json").exists():
            data = {"remote": {}}
            if remote_project:
                data["remote"]["project"] = _COERCION_MAP["remote.project"](remote_project)
            if remote_root:
                data["remote"]["root"] = validate_remote_root(remote_root)
            _write_json(dml_dir / "config.json", data)
        return cls.resolve(explicit={"project_home": str(root)})

    @overload
    def update(self, key: str, value: str, *, scope: Literal["global", "local"]) -> str: ...
    @overload
    def update(self, key: str, value: int, *, scope: Literal["global", "local"]) -> int: ...
    @overload
    def update(self, key: str, value: None, *, scope: Literal["global", "local"]) -> None: ...
    def update(self, key: str, value: str | int | None, *, scope: Literal["global", "local"]) -> str | int | None:
        if scope == "global":
            path = Path(self.config_home) / "config.json"
        else:
            if not self.project_home:
                raise DmlRepoError("project_home is required")
            path = Path(self.project_home) / ".dml" / "config.json"
        data = flatten_dict(_read_json(path))
        if value is None:
            data.pop(key, None)
        elif key == "remote.project":
            data[key] = _COERCION_MAP["remote.project"](value)
        else:
            data[key] = _COERCION_MAP.get(key, lambda v: v)(value)
        _write_json(path, unflatten_dict(data))
        return value
