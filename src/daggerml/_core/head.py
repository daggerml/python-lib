from __future__ import annotations

import fcntl
import json
import os
import re
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Literal, TypedDict
from urllib.parse import quote, unquote

from daggerml._core.db import Ref
from daggerml._core.types import DmlRepoError

_HEAD_ATTACHED_PREFIX = "ref: refs/local/heads/"
_SHALLOW_VERSION = 1


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


def _read_commit_ref(path: Path) -> Ref:
    commit_id = path.read_text(encoding="utf-8").strip()
    if re.fullmatch(r"[0-9a-f]{64}", commit_id) is None:
        raise DmlRepoError(f"Invalid commit pointer: {path}")
    return Ref(f"commit:{commit_id}")


class HeadInfo(TypedDict):
    mode: Literal["attached", "detached"]
    branch: str | None
    commit: Ref | None


class UpstreamInfo(TypedDict):
    branch: str


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
        return _read_commit_ref(path)

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
        try:
            upstream = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(upstream, dict) or set(upstream) != {"branch"}:
                raise ValueError("expected an object containing only 'branch'")
            upstream_branch = upstream["branch"]
            if not isinstance(upstream_branch, str):
                raise ValueError("expected 'branch' to be a string")
            return {"branch": _validate_ref_name("upstream branch", upstream_branch)}
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise DmlRepoError(f"Invalid upstream config: {branch}") from exc

    def set_upstream(self, branch: str, upstream_branch: str) -> UpstreamInfo:
        upstream: UpstreamInfo = {"branch": _validate_ref_name("upstream branch", upstream_branch)}
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

    def get_remote_tracking_ref(self, name: str, *, kind: Literal["branch", "tag"] = "branch") -> Ref:
        path = self.remote_tracking_ref_path(name, kind=kind)
        if not path.exists():
            raise DmlRepoError(f"Pointer does not exist: {path}")
        return _read_commit_ref(path)

    def create_remote_tracking_ref(
        self, name: str, commit: Ref, *, kind: Literal["branch", "tag"] = "branch"
    ) -> str:
        path = self.remote_tracking_ref_path(name, kind=kind)
        if path.exists():
            raise DmlRepoError(f"{kind.title()} already exists: {name}")
        self.write_ref(path, commit)
        return name

    def update_remote_tracking_ref(
        self, name: str, commit: Ref, *, kind: Literal["branch", "tag"] = "branch"
    ) -> str:
        self.write_ref(self.remote_tracking_ref_path(name, kind=kind), commit)
        return name

    def delete_remote_tracking_ref(self, name: str, *, kind: Literal["branch", "tag"] = "branch") -> None:
        self.remote_tracking_ref_path(name, kind=kind).unlink()

    def get_dependency_config(self, dependency: str) -> dict[str, str]:
        path = self.dependency_config_path(dependency)
        try:
            config = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise DmlRepoError(f"Dependency does not exist: {dependency}") from exc
        if not isinstance(config, dict) or set(config) != {"backend", "root"}:
            raise DmlRepoError(f"Invalid dependency config: {dependency}")
        if config.get("backend") != "s3" or not isinstance(config.get("root"), str):
            raise DmlRepoError(f"Invalid dependency config: {dependency}")
        from daggerml._core.config import validate_remote_root

        try:
            root = validate_remote_root(config["root"])
        except ValueError as exc:
            raise DmlRepoError(f"Invalid dependency config: {dependency}") from exc
        if not root:
            raise DmlRepoError(f"Invalid dependency config: {dependency}")
        return {"backend": "s3", "root": root}

    def add_dependency(self, dependency: str, root: str) -> str:
        from daggerml._core.config import validate_remote_root

        dependency = _validate_segment("dependency", dependency)
        path = self.dependency_config_path(dependency)
        if path.exists():
            raise DmlRepoError(f"Dependency already exists: {dependency}")
        path.parent.mkdir(parents=True, exist_ok=True)
        normalized_root = validate_remote_root(root)
        if not normalized_root:
            raise ValueError("Dependency root must be a non-empty s3:// URI")
        payload = {"backend": "s3", "root": normalized_root}
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
            json.dump(payload, tmp, separators=(",", ":"), sort_keys=True)
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)
        return dependency

    def delete_dependency(self, dependency: str) -> None:
        path = self.dependency_path(dependency)
        if not path.exists():
            raise DmlRepoError(f"Dependency does not exist: {dependency}")
        for child in sorted(path.rglob("*"), reverse=True):
            if child.is_file():
                child.unlink()
            elif child.is_dir():
                child.rmdir()
        path.rmdir()

    def list_dependencies(self) -> list[str]:
        base = Path(self.project_home) / ".dml" / "refs" / "dep"
        if not base.exists():
            return []
        return sorted(
            path.name for path in base.iterdir() if path.is_dir() and self.dependency_config_path(path.name).exists()
        )

    def get_dependency_ref(self, dependency: str, name: str, *, kind: Literal["branch", "tag"] = "branch") -> Ref:
        path = self.dependency_ref_path(dependency, name, kind=kind)
        if not path.exists():
            raise DmlRepoError(f"Pointer does not exist: {path}")
        return _read_commit_ref(path)

    def update_dependency_ref(
        self, dependency: str, name: str, commit: Ref, *, kind: Literal["branch", "tag"] = "branch"
    ) -> str:
        self.write_ref(self.dependency_ref_path(dependency, name, kind=kind), commit)
        return name

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

    def shallow_path(self) -> Path:
        return Path(self.project_home) / ".dml" / "shallow.json"

    def get_shallow_commits(self) -> set[Ref]:
        path = self.shallow_path()
        if not path.exists():
            return set()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or set(payload) != {"version", "missing"}:
                raise ValueError("expected version and missing fields")
            if payload["version"] != _SHALLOW_VERSION:
                raise ValueError("unsupported shallow metadata version")
            missing = payload["missing"]
            if not isinstance(missing, list) or missing != sorted(set(missing)):
                raise ValueError("missing refs must be a sorted unique list")
            if not all(isinstance(value, str) and re.fullmatch(r"commit:[0-9a-f]{64}", value) for value in missing):
                raise ValueError("missing refs must be exact commit refs")
            return {Ref(value) for value in missing}
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise DmlRepoError(f"Invalid shallow metadata: {path}") from exc

    def write_shallow_commits(self, commits: set[Ref]) -> None:
        values = sorted(ref.to for ref in commits)
        if any(re.fullmatch(r"commit:[0-9a-f]{64}", value) is None for value in values):
            raise ValueError("Shallow metadata accepts only exact commit refs")
        path = self.shallow_path()
        if not values:
            path.unlink(missing_ok=True)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"version": _SHALLOW_VERSION, "missing": values}
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
            json.dump(payload, tmp, separators=(",", ":"), sort_keys=True)
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)

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

    def remote_tracking_ref_path(self, name: str, *, kind: Literal["branch", "tag"]) -> Path:
        leaf = "heads" if kind == "branch" else "tags"
        return (
            Path(self.project_home)
            / ".dml"
            / "refs"
            / "remote"
            / leaf
            / quote(_validate_ref_name(kind, name), safe="")
        )

    def dependency_path(self, dependency: str) -> Path:
        return Path(self.project_home) / ".dml" / "refs" / "dep" / _validate_segment("dependency", dependency)

    def dependency_config_path(self, dependency: str) -> Path:
        return self.dependency_path(dependency) / "config.json"

    def dependency_ref_path(self, dependency: str, name: str, *, kind: Literal["branch", "tag"]) -> Path:
        fname = quote(_validate_ref_name(kind, name), safe="")
        leaf = "heads" if kind == "branch" else "tags"
        return self.dependency_path(dependency) / leaf / fname

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

    def list_local_refs(self, kind: Literal["branch", "tag"] = "branch") -> list[str]:
        base = Path(self.project_home) / ".dml" / "refs" / "local" / ("heads" if kind == "branch" else "tags")
        refs = []
        if not base.exists():
            return refs
        for path in base.rglob("*"):
            if path.is_file():
                refs.append(unquote(path.relative_to(base).as_posix()))
        return sorted(refs)

    def list_local_ref_tips(self, kind: Literal["branch", "tag"] = "branch") -> list[tuple[str, Ref]]:
        return [(name, self.get_local_ref(name, kind=kind)) for name in self.list_local_refs(kind=kind)]

    def list_remote_tracking_refs(self, kind: Literal["branch", "tag"] = "branch") -> list[str]:
        base = (
            Path(self.project_home)
            / ".dml"
            / "refs"
            / "remote"
            / ("heads" if kind == "branch" else "tags")
        )
        if not base.exists():
            return []
        return sorted(unquote(path.relative_to(base).as_posix()) for path in base.rglob("*") if path.is_file())

    def list_remote_tracking_ref_tips(self, kind: Literal["branch", "tag"] = "branch") -> list[tuple[str, Ref]]:
        return [
            (name, self.get_remote_tracking_ref(name, kind=kind))
            for name in self.list_remote_tracking_refs(kind=kind)
        ]

    def list_dependency_ref_tips(
        self, dependency: str, kind: Literal["branch", "tag"] = "branch"
    ) -> list[tuple[str, Ref]]:
        base = self.dependency_path(dependency) / ("heads" if kind == "branch" else "tags")
        if not base.exists():
            return []
        names = sorted(unquote(path.relative_to(base).as_posix()) for path in base.rglob("*") if path.is_file())
        return [(name, self.get_dependency_ref(dependency, name, kind=kind)) for name in names]

    def iter_remote_tracking_refs(self) -> Iterator[Ref]:
        for kind in ("branch", "tag"):
            for name in self.list_remote_tracking_refs(kind=kind):
                yield self.get_remote_tracking_ref(name, kind=kind)

    def iter_all_remote_tracking_refs(self) -> Iterator[Ref]:
        yield from self.iter_remote_tracking_refs()
        for dependency in self.list_dependencies():
            for kind in ("branch", "tag"):
                base = self.dependency_path(dependency) / ("heads" if kind == "branch" else "tags")
                if base.exists():
                    for path in base.rglob("*"):
                        if path.is_file():
                            name = unquote(path.relative_to(base).as_posix())
                            yield self.get_dependency_ref(dependency, name, kind=kind)
