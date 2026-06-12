from __future__ import annotations

import re
from dataclasses import dataclass

from daggerml._core.head import _validate_ref_name, _validate_segment

PROJECT_URI_REGEX = re.compile(
    r"^(dml://(?:(?P<owner>[^/]+))(?:/(?P<project>[^@#]+)))?(?:@(?P<tag>[^@#]+))?(?:#(?P<branch>[^@#]+))?$"
)


@dataclass(frozen=True)
class ProjectUri:
    owner: str | None
    project: str | None
    branch: str | None = None
    tag: str | None = None

    def __post_init__(self) -> None:
        if (self.owner is None) != (self.project is None):
            raise ValueError("Project URI requires both owner and project or neither")
        if self.owner is not None:
            _validate_segment("project owner", self.owner)
        if self.project is not None:
            _validate_segment("project name", self.project)
        if self.branch is not None and self.tag is not None:
            raise ValueError("Project URI cannot include both a branch and a tag")
        if self.branch is not None:
            _validate_ref_name("branch", self.branch)
        if self.tag is not None:
            _validate_ref_name("tag", self.tag)

    def ensure_project(self, strict: bool = False) -> ProjectUriWRemote:
        if self.owner is None or self.project is None:
            raise ValueError("Project URI requires both owner and project")
        if strict and (self.branch is not None or self.tag is not None):
            raise ValueError("Strict project URI cannot include branch or tag")
        return ProjectUriWRemote(owner=self.owner, project=self.project, branch=self.branch, tag=self.tag)

    @classmethod
    def from_uri(cls, uri: str) -> "ProjectUri":
        match = PROJECT_URI_REGEX.match(uri)
        if not match:
            raise ValueError(f"Invalid DML URI: {uri!r}")
        return cls(**match.groupdict())

    def __str__(self) -> str:
        base = ""
        if self.owner is not None and self.project is not None:
            base = f"dml://{self.owner}/{self.project}"
        if self.branch is not None:
            return f"{base}#{self.branch}"
        if self.tag is not None:
            return f"{base}@{self.tag}"
        return base


@dataclass(frozen=True)
class ProjectUriWRemote(ProjectUri):
    owner: str
    project: str
    branch: str | None = None
    tag: str | None = None
