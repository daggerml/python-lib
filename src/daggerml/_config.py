"""Configuration resolution for DaggerML.

Resolution precedence is:
defaults < environment variables < explicit values
"""

from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass, field, replace
from getpass import getuser
from socket import gethostname
from typing import Mapping, Optional, cast

_PATH_KEYS = {"repo", "config_dir"}
_ENV_KEYS: dict[str, tuple[str, ...]] = {
    "repo": ("DML_REPO",),
    "branch": ("DML_BRANCH",),
    "user": ("DML_USER",),
    "config_dir": ("DML_CONFIG_DIR",),
    "remote.root": ("DML_REMOTE_ROOT",),
    "remote.cache": ("DML_REMOTE_CACHE",),
}

_REMOTE_CACHE_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")


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


def _default_config_dir(env_map: Mapping[str, str]) -> str:
    """Compute default config dir using XDG base directory semantics."""
    base = env_map.get("XDG_CONFIG_HOME")
    if not base:
        base = os.path.expanduser("~/.config")
    return os.path.join(base, "dml")


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


def _validate_remote_cache(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("remote.cache must be a string")
    if not _REMOTE_CACHE_RE.match(value):
        raise ValueError("remote.cache must match [a-z0-9][a-z0-9._-]{0,127}")
    return value


@dataclass(frozen=True)
class DmlRemoteConfig:
    """Resolved remote configuration."""

    root: Optional[str] = None
    cache: Optional[str] = None


@dataclass(frozen=True)
class DmlConfig:
    """Resolved DaggerML configuration."""

    repo: Optional[str] = None
    branch: str = "main"
    user: Optional[str] = None
    config_dir: Optional[str] = None
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
        cls._set(data, "config_dir", _default_config_dir(env_map))

        if defaults:
            for key, value in defaults.items():
                cls._set(data, key, value)

        for key, env_names in _ENV_KEYS.items():
            value = cls._from_env(env_map, key, env_names)
            cls._set(data, key, value)

        if explicit:
            for key, value in explicit.items():
                cls._set(data, key, value)

        remote_data = cast(dict[str, Optional[str]], data["remote"])
        remote = DmlRemoteConfig(**remote_data)
        return cls(
            repo=cast(Optional[str], data["repo"]),
            branch=cast(str, data["branch"]),
            user=cast(Optional[str], data["user"]),
            config_dir=cast(Optional[str], data["config_dir"]),
            remote=remote,
        )

    def with_repo_defaults(self) -> "DmlConfig":
        """Fill config_dir from repo when absent."""
        if not self.repo:
            return self
        return replace(
            self,
            config_dir=self.config_dir or self.repo,
        )

    def envvars(self) -> dict[str, object]:
        """Return canonical DML environment variable mapping."""
        return {
            "DML_REPO": self.repo,
            "DML_BRANCH": self.branch,
            "DML_USER": self.user,
            "DML_CONFIG_DIR": self.config_dir,
            "DML_REMOTE_ROOT": self.remote.root,
            "DML_REMOTE_CACHE": self.remote.cache,
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
            DmlConfig._set(data, "remote.cache", value.get("cache"))
            return
        if key == "remote.root":
            if isinstance(value, str):
                value = _validate_remote_root(value)
            remote = cast(dict[str, object], data["remote"])
            remote["root"] = value
            return
        if key == "remote.cache":
            if isinstance(value, str):
                value = _validate_remote_cache(value)
            remote = cast(dict[str, object], data["remote"])
            remote["cache"] = value
            return
        if key not in data:
            return
        if isinstance(value, str) and key in _PATH_KEYS:
            data[key] = os.path.expanduser(value)
            return
        data[key] = value
