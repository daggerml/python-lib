from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from getpass import getuser
from pathlib import Path
from socket import gethostname
from typing import Any, Mapping
from urllib.parse import urlsplit

from daggerml._internal.revision_uri import (
    canonicalize_revision_uri,
    parse_revision_uri,
    stringify_revision_uri,
    validate_ref_name,
    validate_segment,
)

_PROJECT_SCOPE = "project/runtime"
_GLOBAL_SCOPE = "global"
_ENV_KEYS: dict[str, str] = {
    "project.home": "DML_PROJECT_HOME",
    "remote.project": "DML_REMOTE_PROJECT",
    "db.path": "DML_DB_PATH",
    "remote.root": "DML_REMOTE_ROOT",
    "remote.fetch_workers": "DML_REMOTE_FETCH_WORKERS",
    "user": "DML_USER",
    "default_branch": "DML_DEFAULT_BRANCH",
    "config_home": "DML_CONFIG_HOME",
    "execution.id": "DML_EXECUTION_ID",
}


@dataclass(frozen=True)
class ParsedProjectUri:
    owner: str
    project: str
    branch: str | None = None
    tag: str | None = None

    def canonical(self) -> str:
        uri = f"dml://{self.owner}/{self.project}"
        if self.branch is not None:
            return f"{uri}#{self.branch}"
        if self.tag is not None:
            return f"{uri}@{self.tag}"
        return uri


def _validate_ref_name(label: str, value: str) -> str:
    return validate_ref_name(label, value)


def parse_dml_project_uri(uri: str, *, require_identifier: bool = False) -> ParsedProjectUri:
    if require_identifier:
        parsed = parse_revision_uri(uri, require_identifier=True)
        return ParsedProjectUri(parsed.owner, parsed.project, branch=parsed.branch, tag=parsed.tag)
    if "#" in uri or "@" in uri:
        parsed = parse_revision_uri(uri, require_identifier=True)
        return ParsedProjectUri(parsed.owner, parsed.project, branch=parsed.branch, tag=parsed.tag)
    if not isinstance(uri, str) or not uri.startswith("dml://"):
        raise ValueError(f"Invalid DML URI: {uri!r}")
    parsed = urlsplit(uri)
    if parsed.scheme != "dml" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError(f"Invalid DML URI: {uri!r}")
    project = parsed.path.strip("/")
    if "/" in project or not project:
        raise ValueError(f"Invalid DML URI project path: {uri!r}")
    return ParsedProjectUri(
        owner=validate_segment("project owner", parsed.netloc),
        project=validate_segment("project name", project),
        branch=None,
        tag=None,
    )


def normalize_project_uri(uri: str, *, default_branch: str | None = None, require_branch: bool = False) -> str:
    parsed = parse_revision_uri(
        uri,
        default_branch=default_branch,
        require_identifier=require_branch,
    )
    return canonicalize_revision_uri(stringify_revision_uri(parsed), require_identifier=True)


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
    if not bucket:
        raise ValueError("remote.root must include a bucket name")
    if sep and not prefix.strip("/"):
        raise ValueError("remote.root prefix must be non-empty when '/' is provided")
    return value.rstrip("/")


def global_config_home(env: Mapping[str, str] | None = None) -> Path:
    env_map = os.environ if env is None else env
    if env_map.get("DML_CONFIG_HOME"):
        return Path(os.path.expanduser(env_map["DML_CONFIG_HOME"]))
    if env_map.get("XDG_CONFIG_HOME"):
        return Path(os.path.expanduser(env_map["XDG_CONFIG_HOME"])) / "dml"
    return Path(os.path.expanduser("~/.config/dml"))


def default_user(env: Mapping[str, str] | None = None) -> str | None:
    env_map = os.environ if env is None else env
    user = env_map.get("USER")
    if not user:
        try:
            user = getuser()
        except Exception:
            user = None
    if not user:
        return None
    try:
        host = gethostname().split(".", 1)[0]
    except Exception:
        host = ""
    return f"{user}@{host}" if host else user


def _read_toml(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text())


def _coerce_path(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, Path):
        value = str(value)
    if not isinstance(value, str):
        raise ValueError(f"Expected path-like string, got {type(value).__name__}")
    return os.path.expanduser(value)


def _coerce_positive_int(value: object, *, key: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{key} must be a positive integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = int(text, 10)
        except ValueError as exc:
            raise ValueError(f"{key} must be a positive integer") from exc
    else:
        raise ValueError(f"{key} must be a positive integer")
    if parsed <= 0:
        raise ValueError(f"{key} must be a positive integer")
    return parsed


def _load_global_layer(config_home: str, env: Mapping[str, str]) -> dict[str, object]:
    path = Path(config_home) / "config.toml"
    layer: dict[str, object] = {"config_home": config_home}
    if not path.exists():
        return layer
    data = _read_toml(path)
    defaults = data.get("defaults", {}) or {}
    remote = data.get("remote", {}) or {}
    user = data.get("user", {}) or {}
    layer["user"] = user.get("name")
    layer["default_branch"] = defaults.get("branch")
    layer["remote.fetch_workers"] = remote.get("fetch_workers")
    return layer


def _load_project_layer(project_home: str | None) -> dict[str, object]:
    if not project_home:
        return {}
    path = Path(project_home) / ".dml" / "config.toml"
    if not path.exists():
        return {}
    data = _read_toml(path)
    remote = data.get("remote", {}) or {}
    layer: dict[str, object] = {"project.home": project_home}
    if remote.get("project"):
        layer["remote.project"] = str(remote["project"])
    if remote.get("root"):
        layer["remote.root"] = remote.get("root")
    if remote.get("fetch_workers") is not None:
        layer["remote.fetch_workers"] = remote.get("fetch_workers")
    return layer


def _normalize_key(key: str) -> str:
    return key


def _normalize_inputs(values: Mapping[str, object] | None) -> dict[str, object]:
    out: dict[str, object] = {}
    if not values:
        return out
    for key, value in values.items():
        normalized = _normalize_key(key)
        out[normalized] = value
    return out


def _overlay(base: dict[str, object], layer: Mapping[str, object]) -> dict[str, object]:
    out = dict(base)
    for key, value in layer.items():
        if value is None:
            continue
        out[key] = value
    return out


def _env_layer(env: Mapping[str, str]) -> dict[str, object]:
    out: dict[str, object] = {}
    for key, name in _ENV_KEYS.items():
        if name not in env:
            continue
        value = env[name]
        if value == "":
            continue
        out[key] = value
    return out


@dataclass(frozen=True)
class DmlProjectSettings:
    home: str | None = None


@dataclass(frozen=True)
class DmlDbSettings:
    path: str | None = None


@dataclass(frozen=True)
class DmlRemoteSettings:
    project: str | None = None
    root: str = ""
    fetch_workers: int = 16


@dataclass(frozen=True)
class DmlExecutionSettings:
    id: str | None = None


@dataclass(frozen=True)
class DmlConfig:
    project: DmlProjectSettings = field(default_factory=DmlProjectSettings)
    db: DmlDbSettings = field(default_factory=DmlDbSettings)
    remote: DmlRemoteSettings = field(default_factory=DmlRemoteSettings)
    execution: DmlExecutionSettings = field(default_factory=DmlExecutionSettings)
    user: str | None = None
    default_branch: str = "main"
    config_home: str = ""

    @property
    def repo(self) -> str | None:
        return self.project.home

    @property
    def branch(self) -> str:
        return self.default_branch

    @property
    def db_path(self) -> str | None:
        return self.db.path

    @classmethod
    def resolve(
        cls,
        *,
        scope: str = _PROJECT_SCOPE,
        explicit: Mapping[str, object] | None = None,
        env: Mapping[str, str] | None = None,
        defaults: Mapping[str, object] | None = None,
    ) -> "DmlConfig":
        if scope not in {_PROJECT_SCOPE, _GLOBAL_SCOPE}:
            raise ValueError(f"Unknown config scope: {scope!r}")
        env_map = os.environ if env is None else env
        defaults_layer = _normalize_inputs(defaults)
        explicit_layer = _normalize_inputs(explicit)
        env_layer = _env_layer(env_map)
        raw_config_home = (
            explicit_layer.get("config_home") or env_layer.get("config_home") or defaults_layer.get("config_home")
        )
        config_home = _coerce_path(raw_config_home) or str(global_config_home(env_map))
        base: dict[str, object] = {"config_home": config_home}
        merged = _overlay(base, defaults_layer)
        merged = _overlay(merged, _load_global_layer(config_home, env_map))
        project_home_input = (
            explicit_layer.get("project.home") or env_layer.get("project.home") or merged.get("project.home")
        )
        project_home = _coerce_path(project_home_input)
        if project_home is None and scope == _PROJECT_SCOPE:
            project_home = str(Path.cwd())
        if scope == _PROJECT_SCOPE:
            merged = _overlay(merged, _load_project_layer(project_home))
        merged = _overlay(merged, env_layer)
        merged = _overlay(merged, explicit_layer)
        project_home = _coerce_path(merged.get("project.home"))
        if project_home is None and scope == _PROJECT_SCOPE:
            project_home = str(Path.cwd())
        default_branch_value = merged.get("default_branch")
        default_branch = str(default_branch_value) if default_branch_value else "main"
        _validate_ref_name("branch", default_branch)
        remote_project: str | None = None
        raw_remote_project = merged.get("remote.project")
        if raw_remote_project is not None:
            if not isinstance(raw_remote_project, str):
                raise ValueError("remote.project must be a string")
            remote_project = validate_dml_project_uri(raw_remote_project)
        db_path = _coerce_path(merged.get("db.path"))
        if db_path is None and project_home and scope == _PROJECT_SCOPE:
            db_path = str(Path(project_home) / ".dml" / "db")
        remote_root = merged.get("remote.root")
        if remote_root is None:
            remote_root_s = ""
        else:
            if not isinstance(remote_root, str):
                raise ValueError("remote.root must be a string")
            remote_root_s = validate_remote_root(remote_root)
        remote_fetch_workers = _coerce_positive_int(merged.get("remote.fetch_workers"), key="remote.fetch_workers")
        if remote_fetch_workers is None:
            remote_fetch_workers = 16
        user_value = merged.get("user")
        user = str(user_value) if user_value else default_user(env_map)
        execution_id_value = merged.get("execution.id")
        execution_id = str(execution_id_value) if execution_id_value else None
        return cls(
            project=DmlProjectSettings(home=project_home),
            db=DmlDbSettings(path=db_path),
            remote=DmlRemoteSettings(project=remote_project, root=remote_root_s, fetch_workers=remote_fetch_workers),
            execution=DmlExecutionSettings(id=execution_id),
            user=user,
            default_branch=default_branch,
            config_home=config_home,
        )

    def envvars(self) -> dict[str, object]:
        env: dict[str, object] = {
            "DML_USER": self.user,
            "DML_DEFAULT_BRANCH": self.default_branch,
            "DML_CONFIG_HOME": self.config_home,
            "DML_DB_PATH": self.db.path,
            "DML_REMOTE_ROOT": self.remote.root,
            "DML_REMOTE_FETCH_WORKERS": str(self.remote.fetch_workers),
            "DML_REMOTE_PROJECT": self.remote.project,
            "DML_PROJECT_HOME": self.project.home,
            "DML_EXECUTION_ID": self.execution.id,
        }
        return env

    def to_dict(self) -> dict[str, object]:
        return {
            "project": {
                "home": self.project.home,
            },
            "db": {
                "path": self.db.path,
            },
            "remote": {
                "project": self.remote.project,
                "root": self.remote.root,
                "fetch_workers": self.remote.fetch_workers,
            },
            "user": self.user,
            "default_branch": self.default_branch,
            "config_home": self.config_home,
        }


def _validate_name(label: str, value: str) -> str:
    return validate_segment(label, value)


def validate_dml_project_uri(uri: str) -> str:
    parsed = parse_dml_project_uri(uri, require_identifier=False)
    if parsed.branch is not None or parsed.tag is not None:
        raise ValueError(f"Project URI must not include a branch or tag: {uri!r}")
    return f"dml://{parsed.owner}/{parsed.project}"


@dataclass(frozen=True)
class DmlGlobalConfig:
    user: str | None = None
    default_branch: str = "main"

    @classmethod
    def load(cls, config_home: Path | str | None = None, *, env: Mapping[str, str] | None = None) -> "DmlGlobalConfig":
        resolved = DmlConfig.resolve(
            scope="global",
            explicit={"config_home": str(config_home)} if config_home is not None else None,
            env=env,
        )
        return cls(
            user=resolved.user,
            default_branch=resolved.default_branch,
        )


@dataclass(frozen=True)
class DmlProjectConfig:
    name: str | None = None
    owner: str | None = None
    remote_root: str = ""

    @property
    def uri(self) -> str | None:
        if self.owner is None or self.name is None:
            return None
        return validate_dml_project_uri(f"dml://{self.owner}/{self.name}")

    @property
    def remote_project(self) -> str | None:
        return self.uri

    def __post_init__(self) -> None:
        if (self.name is None) != (self.owner is None):
            raise ValueError("Project config requires both name and owner when remote.project is configured")
        if self.name is not None:
            _validate_name("project name", self.name)
        if self.owner is not None:
            _validate_name("project owner", self.owner)
        if self.remote_root:
            validate_remote_root(self.remote_root)

    @classmethod
    def load(cls, project_dir: Path | str = ".") -> "DmlProjectConfig":
        resolved = DmlConfig.resolve(explicit={"project.home": str(project_dir)}, env={})
        if not resolved.remote.project:
            return cls(remote_root=resolved.remote.root)
        parsed = parse_dml_project_uri(resolved.remote.project, require_identifier=False)
        return cls(name=parsed.project, owner=parsed.owner, remote_root=resolved.remote.root)

    def save(self, project_dir: Path | str = ".") -> None:
        dml_dir = Path(project_dir) / ".dml"
        dml_dir.mkdir(parents=True, exist_ok=True)
        lines = ["[remote]"]
        if self.remote_project:
            lines.append(f'project = "{self.remote_project}"')
        if self.remote_root:
            lines.append(f'root = "{validate_remote_root(self.remote_root)}"')
        (dml_dir / "config.toml").write_text("\n".join(lines) + "\n")


def init_project_layout(project_dir: Path | str, cfg: DmlProjectConfig) -> Path:
    root = Path(project_dir)
    dml_dir = root / ".dml"
    db_dir = dml_dir / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    (dml_dir / ".gitignore").write_text("db\nHEAD\nrefs\n")
    cfg.save(root)
    return db_dir
