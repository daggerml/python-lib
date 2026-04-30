from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass, field
from getpass import getuser
from pathlib import Path
from socket import gethostname
from typing import Any, Mapping
from urllib.parse import urlsplit

import tomllib

_SEGMENT_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_PROJECT_SCOPE = "project/runtime"
_GLOBAL_SCOPE = "global"
_PATH_KEYS = {"project.home", "db.path", "config_home"}
_ENV_KEYS: dict[str, tuple[str, ...]] = {
    "project.home": ("DML_PROJECT_HOME",),
    "project.uri": ("DML_PROJECT_URI",),
    "db.path": ("DML_DB_PATH",),
    "remote.uri": ("DML_REMOTE_URI",),
    "user": ("DML_USER",),
    "default_branch": ("DML_DEFAULT_BRANCH",),
    "config_home": ("DML_CONFIG_HOME",),
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


def _validate_segment(label: str, value: str) -> str:
    if not isinstance(value, str) or not _SEGMENT_RE.match(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def _validate_ref_name(label: str, value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"Invalid {label}: must be a non-empty string")
    if value in {".", ".."} or "\\" in value:
        raise ValueError(f"Invalid {label}: {value!r}")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Invalid {label}: {value!r}")
    for part in parts:
        _validate_segment(f"{label} segment", part)
    return value


def parse_dml_project_uri(uri: str, *, require_identifier: bool = False) -> ParsedProjectUri:
    if not isinstance(uri, str) or not uri.startswith("dml://"):
        raise ValueError(f"Invalid DML URI: {uri!r}")
    if "#" in uri and "@" in uri:
        raise ValueError(f"Invalid DML URI: cannot include both branch and tag: {uri!r}")
    base = uri
    branch: str | None = None
    tag: str | None = None
    if "#" in uri:
        base, branch = uri.split("#", 1)
    elif "@" in uri:
        base, tag = uri.split("@", 1)
    parsed = urlsplit(base)
    if parsed.scheme != "dml" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError(f"Invalid DML URI: {uri!r}")
    project = parsed.path.strip("/")
    if "/" in project or not project:
        raise ValueError(f"Invalid DML URI project path: {uri!r}")
    result = ParsedProjectUri(
        owner=_validate_segment("project owner", parsed.netloc),
        project=_validate_segment("project name", project),
        branch=_validate_ref_name("branch", branch) if branch is not None else None,
        tag=_validate_ref_name("tag", tag) if tag is not None else None,
    )
    if require_identifier and result.branch is None and result.tag is None:
        raise ValueError(f"DML URI must include a branch or tag: {uri!r}")
    if len(result.canonical().encode("utf-8")) > 64:
        raise ValueError("Canonical DML URI exceeds 64-byte ref limit")
    return result


def normalize_project_uri(uri: str, *, default_branch: str | None = None, require_branch: bool = False) -> str:
    parsed = parse_dml_project_uri(uri, require_identifier=False)
    if parsed.tag is not None:
        raise ValueError(f"Project URI must target a branch, not a tag: {uri!r}")
    branch = parsed.branch or default_branch
    if require_branch and not branch:
        raise ValueError(f"Project URI must include a branch: {uri!r}")
    return ParsedProjectUri(parsed.owner, parsed.project, branch=branch).canonical()


def validate_remote_uri(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("remote.uri must be a string")
    if not value:
        return ""
    if not value.startswith("s3://"):
        raise ValueError("remote.uri must be s3://bucket or s3://bucket/prefix")
    rest = value[5:]
    if not rest:
        raise ValueError("remote.uri must include a bucket name")
    bucket, sep, prefix = rest.partition("/")
    if not bucket:
        raise ValueError("remote.uri must include a bucket name")
    if sep and not prefix.strip("/"):
        raise ValueError("remote.uri prefix must be non-empty when '/' is provided")
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


def _coerce_commands(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    raise ValueError("Hook commands must be a string or sequence of strings")


def _load_global_layer(config_home: str, env: Mapping[str, str]) -> dict[str, object]:
    path = Path(config_home) / "config.toml"
    layer: dict[str, object] = {"config_home": config_home}
    if not path.exists():
        return layer
    data = _read_toml(path)
    hooks = data.get("hooks", {}) or {}
    defaults = data.get("defaults", {}) or {}
    user = data.get("user", {}) or {}
    layer["user"] = user.get("name")
    layer["default_branch"] = defaults.get("branch")
    layer["hooks.post-init"] = hooks.get("post-init")
    return layer


def _load_project_layer(project_home: str | None) -> dict[str, object]:
    if not project_home:
        return {}
    path = Path(project_home) / ".dml" / "config.toml"
    if not path.exists():
        return {}
    data = _read_toml(path)
    project = data.get("project", {}) or {}
    remote = data.get("remote", {}) or {}
    layer: dict[str, object] = {"project.home": project_home}
    if project.get("uri"):
        layer["project.uri"] = str(project["uri"])
    if remote.get("uri"):
        layer["remote.uri"] = remote.get("uri")
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
    for key, names in _ENV_KEYS.items():
        for name in names:
            if name not in env:
                continue
            value = env[name]
            if value == "":
                continue
            out[key] = value
            break
    return out


@dataclass(frozen=True)
class DmlProjectSettings:
    home: str | None = None
    uri: str | None = None

    @property
    def branch(self) -> str | None:
        if not self.uri:
            return None
        return parse_dml_project_uri(self.uri, require_identifier=True).branch


@dataclass(frozen=True)
class DmlDbSettings:
    path: str | None = None


@dataclass(frozen=True)
class DmlRemoteSettings:
    uri: str = ""

    @property
    def root(self) -> str:
        return self.uri


@dataclass(frozen=True)
class DmlHookSettings:
    post_init: tuple[str, ...] = ()


@dataclass(frozen=True)
class DmlConfig:
    project: DmlProjectSettings = field(default_factory=DmlProjectSettings)
    db: DmlDbSettings = field(default_factory=DmlDbSettings)
    remote: DmlRemoteSettings = field(default_factory=DmlRemoteSettings)
    user: str | None = None
    default_branch: str = "main"
    hooks: DmlHookSettings = field(default_factory=DmlHookSettings)
    config_home: str = ""
    branch_name: str | None = None

    @property
    def repo(self) -> str | None:
        return self.project.home

    @property
    def branch(self) -> str:
        return self.project.branch or self.branch_name or self.default_branch

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

        project_uri: str | None = None
        branch_override: str | None = None
        raw_project_branch = merged.get("project.branch")
        if raw_project_branch is not None:
            branch_override = str(raw_project_branch)
            _validate_ref_name("branch", branch_override)
        raw_project_uri = merged.get("project.uri")
        if raw_project_uri is not None:
            if not isinstance(raw_project_uri, str):
                raise ValueError("project.uri must be a string")
            parsed_project = parse_dml_project_uri(raw_project_uri, require_identifier=False)
            if parsed_project.tag is not None:
                raise ValueError(f"Project URI must target a branch, not a tag: {raw_project_uri!r}")
            if branch_override is not None:
                raw_project_uri = f"dml://{parsed_project.owner}/{parsed_project.project}#{branch_override}"
            project_uri = normalize_project_uri(
                raw_project_uri,
                default_branch=default_branch,
                require_branch=True,
            )

        db_path = _coerce_path(merged.get("db.path"))
        if db_path is None and project_home and scope == _PROJECT_SCOPE:
            db_path = str(Path(project_home) / ".dml" / "db")

        remote_uri = merged.get("remote.uri")
        if remote_uri is None:
            remote_uri_s = ""
        else:
            if not isinstance(remote_uri, str):
                raise ValueError("remote.uri must be a string")
            remote_uri_s = validate_remote_uri(remote_uri)

        user_value = merged.get("user")
        user = str(user_value) if user_value else default_user(env_map)

        hooks = DmlHookSettings(
            post_init=_coerce_commands(merged.get("hooks.post-init")) or (),
        )
        return cls(
            project=DmlProjectSettings(home=project_home, uri=project_uri),
            db=DmlDbSettings(path=db_path),
            remote=DmlRemoteSettings(uri=remote_uri_s),
            user=user,
            default_branch=default_branch,
            hooks=hooks,
            config_home=config_home,
            branch_name=branch_override,
        )

    def envvars(self) -> dict[str, object]:
        env: dict[str, object] = {
            "DML_USER": self.user,
            "DML_DEFAULT_BRANCH": self.default_branch,
            "DML_CONFIG_HOME": self.config_home,
            "DML_DB_PATH": self.db.path,
            "DML_REMOTE_URI": self.remote.uri,
            "DML_PROJECT_HOME": self.project.home,
            "DML_PROJECT_URI": self.project.uri,
        }
        return env

    def to_dict(self) -> dict[str, object]:
        return {
            "project": {
                "home": self.project.home,
                "uri": self.project.uri,
                "branch": self.branch,
            },
            "db": {
                "path": self.db.path,
            },
            "remote": {
                "uri": self.remote.uri,
            },
            "user": self.user,
            "default_branch": self.default_branch,
            "hooks": {
                "post-init": list(self.hooks.post_init),
            },
            "config_home": self.config_home,
        }


def _validate_name(label: str, value: str) -> str:
    if not isinstance(value, str) or not re.match(r"^[a-z0-9][a-z0-9._-]{0,127}$", value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def _validate_branch(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"Invalid branch: {value!r}")
    if value in {".", ".."} or "\\" in value:
        raise ValueError(f"Invalid branch: {value!r}")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"Invalid branch: {value!r}")
    for part in parts:
        _validate_name("branch segment", part)
    return value


def validate_dml_project_uri(uri: str) -> str:
    parsed = parse_dml_project_uri(uri, require_identifier=False)
    if parsed.branch is not None or parsed.tag is not None:
        raise ValueError(f"Project URI must not include a branch or tag: {uri!r}")
    return f"dml://{parsed.owner}/{parsed.project}"


@dataclass(frozen=True)
class DmlGlobalConfig:
    user: str | None = None
    default_branch: str = "main"
    post_init: tuple[str, ...] = ()

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
            post_init=resolved.hooks.post_init,
        )


@dataclass(frozen=True)
class DmlProjectConfig:
    name: str
    owner: str
    branch: str = "main"
    remote_uri: str = ""

    @property
    def uri(self) -> str:
        return validate_dml_project_uri(f"dml://{self.owner}/{self.name}")

    @property
    def project_uri(self) -> str:
        return normalize_project_uri(f"{self.uri}#{self.branch}", require_branch=True)

    def __post_init__(self) -> None:
        _validate_name("project name", self.name)
        _validate_name("project owner", self.owner)
        _validate_branch(self.branch)
        if self.remote_uri:
            validate_remote_uri(self.remote_uri)

    @classmethod
    def load(cls, project_dir: Path | str = ".") -> "DmlProjectConfig":
        resolved = DmlConfig.resolve(explicit={"project.home": str(project_dir)}, env={})
        if not resolved.project.uri:
            raise ValueError("Project config must define project.uri")
        parsed = parse_dml_project_uri(resolved.project.uri, require_identifier=True)
        if parsed.tag is not None:
            raise ValueError(f"Project URI must not include a tag: {resolved.project.uri!r}")
        assert parsed.branch is not None
        return cls(name=parsed.project, owner=parsed.owner, branch=parsed.branch, remote_uri=resolved.remote.uri)

    def save(self, project_dir: Path | str = ".") -> None:
        dml_dir = Path(project_dir) / ".dml"
        dml_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            "[project]",
            f'uri = "{self.project_uri}"',
        ]
        if self.remote_uri:
            lines.extend(["", "[remote]", f'uri = "{validate_remote_uri(self.remote_uri)}"'])
        (dml_dir / "config.toml").write_text("\n".join(lines) + "\n")


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
            "DML_PROJECT_URI": project.project_uri,
            "DML_REMOTE_URI": project.remote_uri,
        }
    )
    if remote_name is not None:
        env["DML_REMOTE_NAME"] = remote_name
    for command in commands:
        completed = subprocess.run(command, cwd=project_dir, env=env, shell=True, check=False)
        if completed.returncode != 0:
            raise RuntimeError(f"Hook {hook!r} failed ({completed.returncode}): {command}")
