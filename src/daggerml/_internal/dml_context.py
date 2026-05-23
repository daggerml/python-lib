from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from daggerml._internal.config import DmlConfig, DmlProjectConfig
from daggerml._internal.revision_uri import RevisionUri, parse_revision_uri, stringify_revision_uri
from daggerml._internal.types import DmlRepoError


@dataclass(frozen=True)
class DmlRuntimeContext:
    config: DmlConfig

    @property
    def project_home(self) -> str | None:
        return self.config.project.home

    @property
    def remote_root(self) -> str:
        return self.config.remote.root

    @property
    def user(self) -> str | None:
        return self.config.user

    @property
    def execution_id(self) -> str | None:
        return self.config.execution.id

    @property
    def default_branch(self) -> str:
        return self.config.default_branch


def resolve_runtime_context(
    *,
    project_home: str | None = None,
    remote_root: str | None = None,
    user: str | None = None,
    config_home: str | None = None,
    execution_id: str | None = None,
) -> DmlRuntimeContext:
    config = DmlConfig.resolve(
        explicit={
            "project.home": project_home,
            "remote.root": remote_root,
            "user": user,
            "config_home": config_home,
            "execution.id": execution_id,
        }
    )
    return DmlRuntimeContext(config)


def current_head_branch(head_ops) -> str | None:
    return head_ops.get_attached_head_branch()


def current_head_state(head_ops):
    return head_ops.get_head_state()


def mutable_branch(*, branch: str | None, head_ops) -> str:
    return branch or head_ops.require_attached_head_branch()


def project_remote_root(*, project_home: str, remote_or_uri: str, branch: str | None, default_branch: str) -> str:
    project = DmlProjectConfig.load(project_home)
    if not project.remote_project or project.owner is None or project.name is None:
        raise DmlRepoError("remote.project is required for project sync")
    if remote_or_uri.startswith("dml://"):
        if "#" in remote_or_uri or "@" in remote_or_uri:
            return remote_or_uri
        selector = parse_revision_uri(remote_or_uri, default_branch=branch or default_branch)
        return stringify_revision_uri(selector)
    if remote_or_uri != "origin":
        raise DmlRepoError(f"Unknown remote: {remote_or_uri}")
    return stringify_revision_uri(RevisionUri(project.owner, project.name, branch=branch or default_branch))


def require_project_home(project_home: str | None) -> str:
    if not project_home:
        raise DmlRepoError("project.home is required")
    return project_home


def require_user(user: str | None, *, message: str) -> str:
    if not user:
        raise DmlRepoError(message)
    return user


def db_path_for_project(project_home: str) -> Path:
    return Path(project_home) / ".dml" / "db"


def project_config_exists(project_home: str) -> bool:
    return (Path(project_home) / ".dml" / "config.toml").exists()


def gitignore_exists(project_home: str) -> bool:
    return (Path(project_home) / ".dml" / ".gitignore").exists()


def load_project_config(project_home: str) -> DmlProjectConfig:
    return DmlProjectConfig.load(project_home)


def config_dict(config: DmlConfig) -> dict[str, Any]:
    return config.to_dict()
