"""Persistent, local-only dashboard project registration."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

from daggerml import __version__
from daggerml._core import Dml, DmlRepoError


class DashboardProjects:
    """Manage the explicit local projects visible to one dashboard installation.

    Registration lives below DaggerML's global configuration home, is never
    stored in a project, and is the dashboard's only configuration mutation.
    """

    def __init__(self, config_home: str | Path | None = None):
        config_vars: dict[str, object] | None = {"config_home": str(config_home)} if config_home is not None else None
        resolved = Dml.from_config_vars(config_vars).config.show()
        self.config_home = Path(str(resolved["config_home"])).expanduser().resolve()
        registered = self._read()
        configured = self._configured_project()
        default = registered[0]["path"] if registered else configured
        self.default_project = Path(default).expanduser().resolve() if default else None

    @property
    def directory(self) -> Path:
        version = str(__version__).replace("/", "_")
        return self.config_home / version / "dashboard"

    @property
    def path(self) -> Path:
        return self.directory / "projects.json"

    def list(self) -> dict[str, Any]:
        projects = self._read()
        current = self._project(self.default_project) if self.default_project is not None else None
        if current is not None and not any(item["id"] == current["id"] for item in projects):
            projects.insert(0, current)
        return {"items": projects, "default_project_id": current["id"] if current is not None else None}

    def get(self, project_id: str | None) -> Path:
        if project_id is None:
            if self.default_project is None:
                raise DmlRepoError("Dashboard has no configured project")
            return self.default_project
        for item in self.list()["items"]:
            if item["id"] == project_id:
                return Path(item["path"])
        raise DmlRepoError("Dashboard project is not registered")

    def register(self, path: str | Path, *, name: str | None = None) -> dict[str, Any]:
        project = Path(path).expanduser().resolve()
        if not project.is_dir():
            raise ValueError("Dashboard project path must be an existing directory")
        item = self._project(project, name=name)
        projects = self._read()
        projects = [existing for existing in projects if existing["id"] != item["id"]]
        projects.append(item)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"schema": 1, "projects": projects}, indent=2) + "\n", encoding="utf-8")
        if self.default_project is None:
            self.default_project = project
        return item

    def unregister(self, project_id: str) -> bool:
        projects = self._read()
        remaining = [item for item in projects if item["id"] != project_id]
        if len(remaining) == len(projects):
            return False
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"schema": 1, "projects": remaining}, indent=2) + "\n", encoding="utf-8")
        return True

    @staticmethod
    def _project(path: Path, *, name: str | None = None) -> dict[str, Any]:
        text = str(path)
        return {
            "id": hashlib.sha256(text.encode()).hexdigest()[:16],
            "path": text,
            "name": name or path.name or text,
            "registered_at": int(time.time()),
        }

    def _read(self) -> list[dict[str, Any]]:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return []
        except (OSError, json.JSONDecodeError):
            return []
        projects = payload.get("projects", []) if isinstance(payload, dict) else []
        return [
            item
            for item in projects
            if isinstance(item, dict) and isinstance(item.get("id"), str) and isinstance(item.get("path"), str)
        ]

    def _configured_project(self) -> str | None:
        try:
            payload = json.loads((self.config_home / "config.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return None
        value = payload.get("project_home") if isinstance(payload, dict) else None
        return value if isinstance(value, str) and value else None
