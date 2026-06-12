from __future__ import annotations

import fcntl
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Literal, TypedDict
from urllib.parse import quote, unquote

from daggerml._core.db import Ref
from daggerml._core.types import DmlRepoError

_HEAD_ATTACHED_PREFIX = "ref: refs/local/heads/"


def _validate_segment(label: str, value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"Invalid {label}: {value!r}")
    if "/" in value or value[0] not in "abcdefghijklmnopqrstuvwxyz0123456789":
        raise ValueError(f"Invalid {label}: {value!r}")
    if any(ch not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for ch in value):
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


class HeadInfo(TypedDict):
    mode: Literal["attached", "detached"]
    branch: str | None
    commit: Ref | None


@dataclass(frozen=True)
class Head:
    project_home: str

    @contextmanager
    def lock(self) -> Iterator[None]:
        path = Path(self.project_home) / ".dml" / "lock"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def init(self, commit: Ref | None, default_branch: str, detached: bool = False) -> Ref | None:
        if detached:
            if commit is None:
                raise DmlRepoError("Cannot initialize detached HEAD without a commit")
            self.write_detached_head(commit)
        else:
            if commit is not None:
                self.create_local_ref(default_branch, commit)
            self.write_attached_head(default_branch)
        return commit

    def get_local_ref(self, name: str, *, kind: Literal["branch", "tag"] = "branch") -> Ref:
        path = self.local_ref_path(name, kind=kind)
        if not path.exists():
            raise DmlRepoError(f"Pointer does not exist: {path}")
        return Ref(f"commit:{path.read_text(encoding='utf-8').strip()}")

    def create_local_ref(self, name: str, commit: Ref, *, kind: Literal["branch", "tag"] = "branch") -> str:
        path = self.local_ref_path(name, kind=kind)
        if path.exists():
            raise DmlRepoError(f"{kind.title()} already exists: {name}")
        self.write_ref(path, commit)
        return name

    def update_local_ref(self, name: str, commit: Ref, *, kind: Literal["branch", "tag"] = "branch") -> str:
        self.write_ref(self.local_ref_path(name, kind=kind), commit)
        return name

    def delete_local_ref(self, name: str, *, kind: Literal["branch", "tag"] = "branch") -> None:
        self.local_ref_path(name, kind=kind).unlink()

    def rename_local_ref(self, old: str, new: str, *, kind: Literal["branch", "tag"] = "branch") -> str:
        src = self.local_ref_path(old, kind=kind)
        dst = self.local_ref_path(new, kind=kind)
        if not src.exists():
            raise DmlRepoError(f"{kind.title()} does not exist: {old}")
        if dst.exists():
            raise DmlRepoError(f"{kind.title()} already exists: {new}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.replace(src, dst)
        return new

    def get_remote_ref(
        self,
        owner: str,
        project: str,
        name: str,
        *,
        kind: Literal["branch", "tag"] = "branch",
    ) -> Ref:
        path = self.remote_ref_path(owner, project, name, kind=kind)
        if not path.exists():
            raise DmlRepoError(f"Pointer does not exist: {path}")
        return Ref(f"commit:{path.read_text(encoding='utf-8').strip()}")

    def create_remote_ref(
        self,
        owner: str,
        project: str,
        name: str,
        commit: Ref,
        *,
        kind: Literal["branch", "tag"] = "branch",
    ) -> str:
        path = self.remote_ref_path(owner, project, name, kind=kind)
        if path.exists():
            raise DmlRepoError(f"{kind.title()} already exists: {name}")
        self.write_ref(path, commit)
        return name

    def update_remote_ref(
        self,
        owner: str,
        project: str,
        name: str,
        commit: Ref,
        *,
        kind: Literal["branch", "tag"] = "branch",
    ) -> str:
        self.write_ref(self.remote_ref_path(owner, project, name, kind=kind), commit)
        return name

    def delete_remote_ref(
        self,
        owner: str,
        project: str,
        name: str,
        *,
        kind: Literal["branch", "tag"] = "branch",
    ) -> None:
        self.remote_ref_path(owner, project, name, kind=kind).unlink()

    def write_attached_head(self, branch: str) -> str:
        branch = _validate_ref_name("branch", branch)
        self.write_head_payload(f"{_HEAD_ATTACHED_PREFIX}{branch}")
        return branch

    def write_detached_head(self, commit: Ref) -> Ref:
        self.write_head_payload(commit.to)
        return commit

    def get_head(self) -> HeadInfo:
        payload = self.head_path().read_text(encoding="utf-8").strip()
        if payload.startswith(_HEAD_ATTACHED_PREFIX):
            branch = payload[len(_HEAD_ATTACHED_PREFIX) :]
            path = self.local_ref_path(branch, kind="branch")
            commit = self.get_local_ref(branch) if path.exists() else None
            return {"mode": "attached", "branch": branch, "commit": commit}
        return {"mode": "detached", "branch": None, "commit": Ref(payload)}

    def head_path(self) -> Path:
        return Path(self.project_home) / ".dml" / "HEAD"

    def local_ref_path(self, name: str, *, kind: Literal["branch", "tag"]) -> Path:
        fname = quote(_validate_ref_name(kind, name), safe="")
        leaf = "heads" if kind == "branch" else "tags"
        return Path(self.project_home) / ".dml" / "refs" / "local" / leaf / fname

    def remote_ref_path(self, owner: str, project: str, name: str, *, kind: Literal["branch", "tag"]) -> Path:
        fname = quote(_validate_ref_name(kind, name), safe="")
        leaf = "heads" if kind == "branch" else "tags"
        return (
            Path(self.project_home)
            / ".dml"
            / "refs"
            / "remote"
            / _validate_segment("owner", owner)
            / _validate_segment("project", project)
            / leaf
            / fname
        )

    def write_head_payload(self, payload: str) -> None:
        path = self.head_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
            tmp.write(payload)
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)

    def write_ref(self, path: Path, commit: Ref) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
            tmp.write(commit.id())
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)

    def list_remote_projects(self) -> list[tuple[str, str]]:
        base = Path(self.project_home) / ".dml" / "refs" / "remote"
        if not base.exists():
            return []
        projects = set()
        for path in base.rglob("*"):
            if path.is_file():
                parts = path.relative_to(base).parts
                if len(parts) >= 4:
                    owner, project = parts[0], parts[1]
                    projects.add((owner, unquote(project)))
        return sorted(projects)

    def list_local_refs(self, kind: Literal["branch", "tag"] = "branch") -> list[str]:
        base = Path(self.project_home) / ".dml" / "refs" / "local" / ("heads" if kind == "branch" else "tags")
        refs = []
        if not base.exists():
            return refs
        for path in base.rglob("*"):
            if path.is_file():
                refs.append(unquote(path.relative_to(base).as_posix()))
        return sorted(refs)

    def list_remote_refs(self, owner: str, project: str, kind: Literal["branch", "tag"] = "branch") -> list[str]:
        base = (
            Path(self.project_home)
            / ".dml"
            / "refs"
            / "remote"
            / _validate_segment("owner", owner)
            / quote(_validate_segment("project", project), safe="")
            / ("heads" if kind == "branch" else "tags")
        )
        refs = []
        if not base.exists():
            return refs
        for path in base.rglob("*"):
            if path.is_file():
                refs.append(unquote(path.relative_to(base).as_posix()))
        return sorted(refs)
