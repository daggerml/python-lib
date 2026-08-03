from __future__ import annotations

import fcntl
import json
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
    allowed = "lowercase letters, digits, '.', '_', and '-'"
    if not isinstance(value, str):
        raise ValueError(f"Invalid {label}: expected a string, got a {type(value).__name__}.")
    if not value:
        raise ValueError(f"Invalid {label}: expected a non-empty string.")
    if "/" in value:
        raise ValueError(f"Invalid {label}: {value!r} contains '/'; expected a single segment.")
    if value[0] not in "abcdefghijklmnopqrstuvwxyz0123456789":
        raise ValueError(f"Invalid {label}: {value!r} must start with a lowercase letter or digit.")
    if any(ch not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for ch in value):
        raise ValueError(f"Invalid {label}: {value!r} contains invalid characters. Use only {allowed}.")
    return value


def _validate_ref_name(label: str, value: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Invalid {label}: expected a string, got a {type(value).__name__}.")
    if not value:
        raise ValueError(f"Invalid {label}: expected a non-empty string.")
    if "\\" in value:
        raise ValueError(f"Invalid {label}: {value!r} contains '\\'.")
    if value in {".", ".."}:
        raise ValueError(f"Invalid {label}: {value!r} is a reserved path segment.")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(
            f"Invalid {label}: {value!r} contains an empty or reserved path segment."
        )
    for part in parts:
        _validate_segment(f"{label} segment", part)
    return value


class HeadInfo(TypedDict):
    mode: Literal["attached", "detached"]
    branch: str | None
    commit: Ref | None


class UpstreamInfo(TypedDict):
    remote: str
    merge: str


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
        if kind == "branch":
            self.delete_upstream(name)

    def rename_local_ref(self, old: str, new: str, *, kind: Literal["branch", "tag"] = "branch") -> str:
        src = self.local_ref_path(old, kind=kind)
        dst = self.local_ref_path(new, kind=kind)
        if not src.exists():
            raise DmlRepoError(f"{kind.title()} does not exist: {old}")
        if dst.exists():
            raise DmlRepoError(f"{kind.title()} already exists: {new}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.replace(src, dst)
        if kind == "branch":
            self.rename_upstream(old, new)
        return new

    def get_upstream(self, branch: str) -> UpstreamInfo | None:
        path = self.upstream_path(branch)
        if not path.exists():
            return None
        upstream = json.loads(path.read_text(encoding="utf-8"))
        return {
            "remote": _validate_segment("remote", upstream["remote"]),
            "merge": _validate_ref_name("upstream branch", upstream["merge"]),
        }

    def set_upstream(self, branch: str, remote: str, merge: str) -> UpstreamInfo:
        upstream: UpstreamInfo = {
            "remote": _validate_segment("remote", remote),
            "merge": _validate_ref_name("upstream branch", merge),
        }
        path = self.upstream_path(branch)
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
            json.dump(upstream, tmp, separators=(",", ":"), sort_keys=True)
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)
        return upstream

    def rename_upstream(self, old: str, new: str) -> None:
        src = self.upstream_path(old)
        if src.exists():
            dst = self.upstream_path(new)
            dst.parent.mkdir(parents=True, exist_ok=True)
            os.replace(src, dst)

    def delete_upstream(self, branch: str) -> None:
        self.upstream_path(branch).unlink(missing_ok=True)

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

    def get_remote_tracking_ref(self, remote: str, name: str, *, kind: Literal["branch", "tag"] = "branch") -> Ref:
        path = self.remote_tracking_ref_path(remote, name, kind=kind)
        if not path.exists():
            raise DmlRepoError(f"Pointer does not exist: {path}")
        return Ref(f"commit:{path.read_text(encoding='utf-8').strip()}")

    def create_remote_tracking_ref(
        self, remote: str, name: str, commit: Ref, *, kind: Literal["branch", "tag"] = "branch"
    ) -> str:
        path = self.remote_tracking_ref_path(remote, name, kind=kind)
        if path.exists():
            raise DmlRepoError(f"{kind.title()} already exists: {name}")
        self.write_ref(path, commit)
        return name

    def update_remote_tracking_ref(
        self, remote: str, name: str, commit: Ref, *, kind: Literal["branch", "tag"] = "branch"
    ) -> str:
        self.write_ref(self.remote_tracking_ref_path(remote, name, kind=kind), commit)
        return name

    def delete_remote_tracking_ref(self, remote: str, name: str, *, kind: Literal["branch", "tag"] = "branch") -> None:
        self.remote_tracking_ref_path(remote, name, kind=kind).unlink()

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

    def upstream_path(self, branch: str) -> Path:
        return (
            Path(self.project_home)
            / ".dml"
            / "refs"
            / "local"
            / "upstreams"
            / quote(_validate_ref_name("branch", branch), safe="")
        )

    def remote_tracking_ref_path(self, remote: str, name: str, *, kind: Literal["branch", "tag"]) -> Path:
        leaf = "heads" if kind == "branch" else "tags"
        return (
            Path(self.project_home)
            / ".dml"
            / "refs"
            / "remote"
            / _validate_segment("remote", remote)
            / leaf
            / quote(_validate_ref_name(kind, name), safe="")
        )

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

    def list_remote_tracking_remotes(self) -> list[str]:
        base = Path(self.project_home) / ".dml" / "refs" / "remote"
        if not base.exists():
            return []
        return sorted(
            path.name
            for path in base.iterdir()
            if path.is_dir() and ((path / "heads").exists() or (path / "tags").exists())
        )

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

    def list_remote_tracking_refs(self, remote: str, kind: Literal["branch", "tag"] = "branch") -> list[str]:
        base = (
            Path(self.project_home)
            / ".dml"
            / "refs"
            / "remote"
            / _validate_segment("remote", remote)
            / ("heads" if kind == "branch" else "tags")
        )
        if not base.exists():
            return []
        return sorted(unquote(path.relative_to(base).as_posix()) for path in base.rglob("*") if path.is_file())

    def iter_remote_tracking_refs(self) -> Iterator[Ref]:
        for remote in self.list_remote_tracking_remotes():
            for kind in ("branch", "tag"):
                for name in self.list_remote_tracking_refs(remote, kind=kind):
                    yield self.get_remote_tracking_ref(remote, name, kind=kind)

    def iter_all_remote_tracking_refs(self) -> Iterator[Ref]:
        yield from self.iter_remote_tracking_refs()
        for owner, project in self.list_remote_projects():
            for kind in ("branch", "tag"):
                for name in self.list_remote_refs(owner, project, kind=kind):
                    yield self.get_remote_ref(owner, project, name, kind=kind)

    def migrate_legacy_remote_refs(self, remote: str, owner: str, project: str) -> None:
        for kind in ("branch", "tag"):
            for name in self.list_remote_refs(owner, project, kind=kind):
                legacy = self.remote_ref_path(owner, project, name, kind=kind)
                tracking = self.remote_tracking_ref_path(remote, name, kind=kind)
                if not tracking.exists():
                    tracking.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(legacy, tracking)
