from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from daggerml._internal.config import _validate_ref_name, validate_dml_project_uri, validate_remote_root
from daggerml._internal.types import DmlRepoError

SCOPE_GLOBAL = "global"
SCOPE_LOCAL = "local"

GLOBAL_KEYS = {"user", "default_branch", "remote.fetch_workers"}
LOCAL_KEYS = {"remote.project", "remote.root", "remote.fetch_workers"}
ALL_KEYS = GLOBAL_KEYS | LOCAL_KEYS


def _read_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return tomllib.loads(path.read_text())


def _toml_value(value: Any) -> str:
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, list):
        return f"[{', '.join(_toml_value(item) for item in value)}]"
    raise DmlRepoError(f"Unsupported config value type: {type(value).__name__}")


def _write_toml(path: Path, data: dict[str, Any]) -> None:
    lines: list[str] = []
    for section in ("project", "remote", "user", "defaults"):
        section_data = data.get(section)
        if not isinstance(section_data, dict) or not section_data:
            continue
        if lines:
            lines.append("")
        lines.append(f"[{section}]")
        for key in sorted(section_data):
            lines.append(f"{key} = {_toml_value(section_data[key])}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + ("\n" if lines else ""))


def _set_nested(data: dict[str, Any], section: str, key: str, value: Any) -> None:
    section_data = data.setdefault(section, {})
    if not isinstance(section_data, dict):
        section_data = {}
        data[section] = section_data
    section_data[key] = value


@dataclass
class ConfigOps:
    project_home: str | None
    config_home: str

    def _path_for_scope(self, scope: Literal["global", "local"]) -> Path:
        if scope == SCOPE_GLOBAL:
            return Path(self.config_home) / "config.toml"
        if scope == SCOPE_LOCAL:
            if not self.project_home:
                raise DmlRepoError("Local config requires project.home (--project-home or DML_PROJECT_HOME)")
            return Path(self.project_home) / ".dml" / "config.toml"
        raise DmlRepoError(f"Unknown config scope: {scope}")

    def _validate_scope_key(self, scope: Literal["global", "local"], key: str) -> None:
        if key not in ALL_KEYS:
            raise DmlRepoError(f"Unsupported config key: {key}")
        if scope == SCOPE_GLOBAL and key not in GLOBAL_KEYS:
            raise DmlRepoError(f"Config key {key!r} is not valid in global scope")
        if scope == SCOPE_LOCAL and key not in LOCAL_KEYS:
            raise DmlRepoError(f"Config key {key!r} is not valid in local scope")

    def get(self, key: str, *, scope: Literal["global", "local"]) -> str | list[str] | None:
        self._validate_scope_key(scope, key)
        data = _read_toml(self._path_for_scope(scope))
        if key == "remote.project":
            value = (data.get("remote") or {}).get("project")
            return str(value) if value else None
        if key == "remote.root":
            value = (data.get("remote") or {}).get("root")
            return str(value) if value else None
        if key == "remote.fetch_workers":
            value = (data.get("remote") or {}).get("fetch_workers")
            return str(value) if value is not None else None
        if key == "user":
            value = (data.get("user") or {}).get("name")
            return str(value) if value else None
        if key == "default_branch":
            value = (data.get("defaults") or {}).get("branch")
            return str(value) if value else None
        raise DmlRepoError(f"Unsupported config key: {key}")

    def set(self, key: str, value: str, *, scope: Literal["global", "local"]) -> str | list[str]:
        self._validate_scope_key(scope, key)
        if key == "remote.project":
            value = validate_dml_project_uri(str(value))
        elif key == "remote.root":
            value = validate_remote_root(str(value))
        elif key == "default_branch":
            _validate_ref_name("branch", str(value))
        elif key == "remote.fetch_workers":
            try:
                workers = int(str(value), 10)
            except ValueError as exc:
                raise DmlRepoError("remote.fetch_workers must be a positive integer") from exc
            if workers <= 0:
                raise DmlRepoError("remote.fetch_workers must be a positive integer")
            value = str(workers)
        elif key == "user" and not str(value):
            raise DmlRepoError("user must be a non-empty string")
        path = self._path_for_scope(scope)
        data = _read_toml(path)
        if key == "remote.project":
            _set_nested(data, "remote", "project", value)
        elif key == "remote.root":
            _set_nested(data, "remote", "root", value)
        elif key == "remote.fetch_workers":
            _set_nested(data, "remote", "fetch_workers", int(str(value)))
        elif key == "user":
            _set_nested(data, "user", "name", value)
        elif key == "default_branch":
            _set_nested(data, "defaults", "branch", value)
        else:
            raise DmlRepoError(f"Unsupported config key: {key}")
        _write_toml(path, data)
        return value
