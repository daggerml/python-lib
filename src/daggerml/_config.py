"""Configuration resolution for DaggerML.

Resolution precedence is:
defaults < environment variables < explicit values
"""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import asdict, dataclass, field
from getpass import getuser
from pathlib import Path
from socket import gethostname
from typing import Mapping, Optional, cast

import tomllib

_PATH_KEYS = {"repo"}
_ENV_KEYS: dict[str, tuple[str, ...]] = {
    "repo": ("DML_REPO",),
    "branch": ("DML_BRANCH",),
    "user": ("DML_USER",),
    "remote.root": ("DML_REMOTE_ROOT",),
}
_PROJECT_ENV_KEYS: dict[str, tuple[str, ...]] = {
    "project.name": ("DML_PROJECT_NAME",),
    "project.owner": ("DML_PROJECT_OWNER", "DML_USER"),
    "project.uri": ("DML_PROJECT_URI",),
    "branch.current": ("DML_BRANCH",),
    "remote.name": ("DML_REMOTE",),
    "remote.uri": ("DML_REMOTE_URI",),
    "remote.bucket": ("DML_REMOTE_BUCKET",),
    "remote.prefix": ("DML_REMOTE_PREFIX",),
}


def _validate_name(label: str, value: str) -> str:
    if not isinstance(value, str) or not re.match(r"^[a-z0-9][a-z0-9._-]{0,127}$", value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def _validate_remote_name(value: str) -> str:
    return _validate_name("remote name", value)


def validate_dml_project_uri(uri: str) -> str:
    if not isinstance(uri, str) or not uri.startswith("dml://"):
        raise ValueError(f"Invalid DML project URI: {uri!r}")
    rest = uri[6:]
    if "#" in rest or "@" in rest or "?" in rest:
        raise ValueError(f"Project URI must not include branch, tag, or query: {uri!r}")
    parts = rest.split("/")
    if len(parts) != 2:
        raise ValueError(f"Invalid DML project URI: {uri!r}")
    owner, project = parts
    canonical = f"dml://{_validate_name('project owner', owner)}/{_validate_name('project name', project)}"
    if len(canonical.encode("utf-8")) > 64:
        raise ValueError("Canonical DML project URI exceeds 64-byte ref limit")
    return canonical


def global_config_home(env: Mapping[str, str] | None = None) -> Path:
    env_map = os.environ if env is None else env
    if env_map.get("DML_CONFIG_HOME"):
        return Path(os.path.expanduser(env_map["DML_CONFIG_HOME"]))
    if env_map.get("XDG_CONFIG_HOME"):
        return Path(os.path.expanduser(env_map["XDG_CONFIG_HOME"])) / "dml"
    return Path(os.path.expanduser("~/.config/dml"))


def _default_user(env_map: Mapping[str, str]) -> Optional[str]:
    """Compute a stable default user identity."""
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


def _validate_remote_root(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("remote.root must be a string")
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
    return value


@dataclass(frozen=True)
class DmlRemoteConfig:
    """Resolved remote configuration."""

    root: str = ""


@dataclass(frozen=True)
class DmlConfig:
    """Resolved DaggerML configuration."""

    repo: Optional[str] = None
    branch: str = "main"
    user: Optional[str] = None
    remote: DmlRemoteConfig = field(default_factory=DmlRemoteConfig)

    @classmethod
    def resolve(
        cls,
        *,
        explicit: Mapping[str, object] | None = None,
        env: Mapping[str, str] | None = None,
        defaults: Mapping[str, object] | None = None,
    ) -> "DmlConfig":
        """Resolve configuration with waterfall precedence.

        Precedence: defaults < env < explicit.
        """
        env_map = os.environ if env is None else env
        data: dict[str, object] = asdict(cls())
        cls._set(data, "user", _default_user(env_map))

        if defaults:
            for key, value in defaults.items():
                cls._set(data, key, value)

        for key, env_names in _ENV_KEYS.items():
            value = cls._from_env(env_map, key, env_names)
            cls._set(data, key, value)

        if explicit:
            for key, value in explicit.items():
                cls._set(data, key, value)

        remote_data = cast(dict[str, str], data["remote"])
        remote = DmlRemoteConfig(**remote_data)
        return cls(
            repo=cast(Optional[str], data["repo"]),
            branch=cast(str, data["branch"]),
            user=cast(Optional[str], data["user"]),
            remote=remote,
        )

    def envvars(self) -> dict[str, object]:
        """Return canonical DML environment variable mapping."""
        return {
            "DML_REPO": self.repo,
            "DML_BRANCH": self.branch,
            "DML_USER": self.user,
            "DML_REMOTE_ROOT": self.remote.root,
        }

    @staticmethod
    def _from_env(env_map: Mapping[str, str], _key: str, names: tuple[str, ...]) -> object | None:
        for name in names:
            if name not in env_map:
                continue
            value = env_map[name]
            if value == "":
                continue
            return value
        return None

    @staticmethod
    def _set(data: dict[str, object], key: str, value: object) -> None:
        if value is None:
            return
        if key == "remote":
            if not isinstance(value, dict):
                return
            DmlConfig._set(data, "remote.root", value.get("root"))
            return
        if key == "remote.root":
            if isinstance(value, str):
                value = _validate_remote_root(value)
            remote = cast(dict[str, object], data["remote"])
            remote["root"] = value
            return
        if key not in data:
            return
        if isinstance(value, str) and key in _PATH_KEYS:
            data[key] = os.path.expanduser(value)
            return
        data[key] = value


@dataclass(frozen=True)
class DmlGlobalConfig:
    user: str | None = None
    default_branch: str = "main"
    post_init: tuple[str, ...] = ()
    post_clone: tuple[str, ...] = ()

    @classmethod
    def load(cls, config_home: Path | str | None = None, *, env: Mapping[str, str] | None = None) -> "DmlGlobalConfig":
        resolved_config_home = Path(config_home) if config_home is not None else global_config_home(env)
        path = resolved_config_home / "config.toml"
        if not path.exists():
            env_map = os.environ if env is None else env
            return cls(user=env_map.get("DML_USER"), default_branch=env_map.get("DML_DEFAULT_BRANCH", "main"))
        data = tomllib.loads(path.read_text())
        hooks = data.get("hooks", {}) or {}
        defaults = data.get("defaults", {}) or {}
        user = data.get("user", {}) or {}
        return cls(
            user=user.get("name"),
            default_branch=defaults.get("branch", "main"),
            post_init=tuple(hooks.get("post-init", []) or ()),
            post_clone=tuple(hooks.get("post-clone", []) or ()),
        )


@dataclass(frozen=True)
class DmlRemoteProjectConfig:
    uri: str
    bucket: str
    prefix: str

    def __post_init__(self) -> None:
        validate_dml_project_uri(self.uri)
        if not self.bucket:
            raise ValueError("Remote bucket is required")
        if self.prefix is None:
            raise ValueError("Remote prefix is required")


@dataclass(frozen=True)
class DmlProjectConfig:
    name: str
    owner: str
    branch: str = "main"
    remotes: dict[str, DmlRemoteProjectConfig] = field(default_factory=dict)

    @property
    def uri(self) -> str:
        return validate_dml_project_uri(f"dml://{self.owner}/{self.name}")

    def __post_init__(self) -> None:
        _validate_name("project name", self.name)
        _validate_name("project owner", self.owner)
        _validate_name("branch", self.branch)
        for remote_name in self.remotes:
            _validate_remote_name(remote_name)

    @classmethod
    def load(cls, project_dir: Path | str = ".") -> "DmlProjectConfig":
        path = Path(project_dir) / ".dml" / "config.toml"
        data = tomllib.loads(path.read_text())
        project = data.get("project", {}) or {}
        branch = data.get("branch", {}) or {}
        remotes_data = data.get("remotes", {}) or {}
        remotes = {
            name: DmlRemoteProjectConfig(
                uri=remote["uri"],
                bucket=remote["bucket"],
                prefix=remote["prefix"],
            )
            for name, remote in remotes_data.items()
        }
        expected_uri = validate_dml_project_uri(project["uri"])
        cfg = cls(name=project["name"], owner=project["owner"], branch=branch["current"], remotes=remotes)
        if cfg.uri != expected_uri:
            raise ValueError(f"Project URI mismatch: expected {cfg.uri}, got {expected_uri}")
        return cfg

    def save(self, project_dir: Path | str = ".") -> None:
        dml_dir = Path(project_dir) / ".dml"
        dml_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            "[project]",
            f'name = "{self.name}"',
            f'owner = "{self.owner}"',
            f'uri = "{self.uri}"',
            "",
            "[branch]",
            f'current = "{self.branch}"',
        ]
        for name, remote in self.remotes.items():
            _validate_remote_name(name)
            lines.extend(
                [
                    "",
                    f"[remotes.{name}]",
                    f'uri = "{validate_dml_project_uri(remote.uri)}"',
                    f'bucket = "{remote.bucket}"',
                    f'prefix = "{remote.prefix}"',
                ]
            )
        (dml_dir / "config.toml").write_text("\n".join(lines) + "\n")


def resolve_waterfall(
    explicit: object | None,
    env: Mapping[str, str],
    env_name: str,
    config_value: object | None,
) -> object | None:
    if explicit is not None:
        return explicit
    value = env.get(env_name)
    if value:
        return value
    return config_value


def init_project_layout(project_dir: Path | str, cfg: DmlProjectConfig) -> Path:
    root = Path(project_dir)
    dml_dir = root / ".dml"
    db_dir = dml_dir / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    (dml_dir / ".gitignore").write_text("*\n")
    cfg.save(root)
    return db_dir


def run_project_hooks(
    hook: str,
    commands: tuple[str, ...],
    *,
    project_dir: Path | str,
    project: DmlProjectConfig,
    config_home: Path | str,
    remote_name: str | None = None,
    no_hooks: bool = False,
) -> None:
    if no_hooks:
        return
    env = os.environ.copy()
    env.update(
        {
            "DML_HOOK": hook,
            "DML_PROJECT_HOME": str(Path(project_dir).resolve()),
            "DML_PROJECT_NAME": project.name,
            "DML_PROJECT_OWNER": project.owner,
            "DML_CONFIG_HOME": str(config_home),
            "DML_BRANCH": project.branch,
        }
    )
    if remote_name is not None:
        env["DML_REMOTE_NAME"] = remote_name
        remote = project.remotes.get(remote_name)
        if remote is not None:
            env["DML_REMOTE_URI"] = remote.uri
    for command in commands:
        completed = subprocess.run(command, cwd=project_dir, env=env, shell=True, check=False)
        if completed.returncode != 0:
            raise RuntimeError(f"Hook {hook!r} failed ({completed.returncode}): {command}")
