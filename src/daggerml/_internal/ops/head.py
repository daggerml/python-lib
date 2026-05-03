"""Branch and index pointer operations."""

import hashlib
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.revision_uri import parse_revision_uri
from daggerml._internal.types import Commit, DmlPointerConflictError, DmlRepoError, Tree

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9\-\*\|_]+$")


@dataclass
class HeadOps(BaseOps):
    def list_branches(self, *, txn=None) -> list[str]:
        del txn
        return self._list_ref_names(self._local_heads_dir())

    def list_indexes(self, *, txn=None) -> list[str]:
        del txn
        return self._list_ref_names(self._local_indexes_dir())

    def create_branch(self, branch_name: str, from_commit: Ref | None = None, *, txn=None) -> str:
        if txn is not None:
            target_commit_ref = from_commit
            if target_commit_ref is None:
                tree_ref = txn.put(Tree(dags={}))
                target_commit_ref = txn.put(Commit(tree=tree_ref, parents=[], author="dml", message="Initial commit"))
            self._require_commit(target_commit_ref, txn)
            self._create_pointer(self._branch_path(branch_name), target_commit_ref)
            return branch_name
        with self._tx(readonly=False) as owned_txn:
            return self.create_branch(branch_name, from_commit, txn=owned_txn)

    def delete_branch(self, branch_name: str, *, txn=None) -> None:
        if txn is not None:
            self._delete_pointer(self._branch_path(branch_name))
            return None
        with self._tx(readonly=False) as owned_txn:
            self.delete_branch(branch_name, txn=owned_txn)

    def get_branch_commit(self, branch_name: str, *, txn=None) -> Ref:
        if txn is not None:
            return self._get_pointer_commit(self._branch_path(branch_name), txn)
        with self._tx(readonly=True) as owned_txn:
            return self.get_branch_commit(branch_name, txn=owned_txn)

    def update_branch_commit(self, branch_name: str, old_commit: Ref, new_commit: Ref, *, txn=None) -> Ref:
        if txn is not None:
            return self._update_pointer_commit(self._branch_path(branch_name), old_commit, new_commit, txn)
        with self._tx(readonly=False) as owned_txn:
            return self.update_branch_commit(branch_name, old_commit, new_commit, txn=owned_txn)

    def create_index(self, commit_ref: Ref, *, txn=None) -> str:
        if txn is not None:
            self._require_commit(commit_ref, txn)
            while True:
                index_id = f"{uuid4().hex}{uuid4().hex}"
                index_path = self._index_path(index_id)
                if not index_path.exists():
                    self._create_pointer(index_path, commit_ref)
                    return index_id
        with self._tx(readonly=False) as owned_txn:
            return self.create_index(commit_ref, txn=owned_txn)

    def delete_index(self, index_id: str, *, txn=None) -> None:
        if txn is not None:
            self._delete_pointer(self._index_path(index_id))
            return None
        with self._tx(readonly=False) as owned_txn:
            self.delete_index(index_id, txn=owned_txn)

    def get_index_commit(self, index_id: str, *, txn=None) -> Ref:
        if txn is not None:
            return self._get_pointer_commit(self._index_path(index_id), txn)
        with self._tx(readonly=True) as owned_txn:
            return self.get_index_commit(index_id, txn=owned_txn)

    def list_pointer_roots(self, *, txn=None) -> list[Ref]:
        if txn is not None:
            return [
                *[self._get_pointer_commit(self._local_branch_path(branch_name), txn) for branch_name in self.list_branches()],
                *[self._get_pointer_commit(self._index_path(index_id), txn) for index_id in self.list_indexes()],
            ]
        with self._tx(readonly=True) as owned_txn:
            return self.list_pointer_roots(txn=owned_txn)

    def update_index_commit(self, index_id: str, old_commit: Ref, new_commit: Ref, *, txn=None) -> Ref:
        if txn is not None:
            return self._update_pointer_commit(self._index_path(index_id), old_commit, new_commit, txn)
        with self._tx(readonly=False) as owned_txn:
            return self.update_index_commit(index_id, old_commit, new_commit, txn=owned_txn)

    def _branch_path(self, branch_name: str) -> Path:
        if branch_name.startswith("dml://"):
            parsed = parse_revision_uri(branch_name, require_identifier=True)
            return self._remote_ref_path(parsed.owner, parsed.project, parsed.branch, parsed.tag)
        if "://" in branch_name or "/" in branch_name:
            return self._external_tracking_path(branch_name)
        return self._local_branch_path(branch_name)

    def _local_branch_path(self, branch_name: str) -> Path:
        return self._local_heads_dir() / self._validate_identifier("branch", branch_name)

    def _index_path(self, index_id: str) -> Path:
        return self._local_indexes_dir() / self._validate_index_id(index_id)

    def _remote_ref_path(self, owner: str, project: str, branch: str | None, tag: str | None) -> Path:
        remote_root = self._refs_root() / "remote"
        owner_name = self._validate_identifier("owner", owner)
        project_name = self._validate_identifier("project", project)
        if branch is not None:
            return remote_root / owner_name / project_name / "heads" / self._validate_identifier("branch", branch)
        if tag is not None:
            return remote_root / owner_name / project_name / "tags" / self._validate_identifier("tag", tag)
        raise DmlRepoError("Remote tracking refs require a branch or tag")

    def _external_tracking_path(self, branch_name: str) -> Path:
        digest = hashlib.sha256(branch_name.encode("utf-8")).hexdigest()
        return self._refs_root() / "remote" / "_external" / "heads" / digest

    def _project_home(self) -> Path:
        db_path_value = getattr(self._db, "path", None)
        if not db_path_value:
            fallback = getattr(self, "_fallback_project_home", None)
            if fallback is None:
                fallback = Path(tempfile.gettempdir()) / "daggerml-headops" / str(id(self._db))
                fallback.mkdir(parents=True, exist_ok=True)
                self._fallback_project_home = fallback
            return Path(fallback)
        db_path = Path(db_path_value).resolve()
        if db_path.name == "db" and db_path.parent.name == ".dml":
            return db_path.parent.parent
        return db_path

    def _refs_root(self) -> Path:
        return self._project_home() / ".dml" / "refs"

    def _local_heads_dir(self) -> Path:
        return self._refs_root() / "local" / "heads"

    def _local_indexes_dir(self) -> Path:
        return self._refs_root() / "local" / "indexes"

    @staticmethod
    def _validate_identifier(label: str, value: str) -> str:
        if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
            raise DmlRepoError(f"Invalid {label}: {value!r}")
        return value

    @staticmethod
    def _validate_index_id(index_id: str) -> str:
        if not isinstance(index_id, str) or not index_id or "/" in index_id or "\\" in index_id:
            raise DmlRepoError(f"Invalid index id: {index_id!r}")
        if index_id in {".", ".."}:
            raise DmlRepoError(f"Invalid index id: {index_id!r}")
        return index_id

    @staticmethod
    def _list_ref_names(ref_dir: Path) -> list[str]:
        if not ref_dir.exists():
            return []
        return sorted(entry.name for entry in ref_dir.iterdir() if entry.is_file() and not entry.name.endswith(".lock"))

    def _create_pointer(self, pointer_path: Path, commit_ref: Ref) -> None:
        self._mutate_pointer(pointer_path, expected_old_commit=None, new_commit=commit_ref, create_only=True)

    def _delete_pointer(self, pointer_path: Path) -> None:
        with self._pointer_lock(pointer_path):
            if not pointer_path.exists():
                raise DmlRepoError(f"Pointer does not exist: {pointer_path}")
            pointer_path.unlink()

    def _get_pointer_commit(self, pointer_path: Path, txn) -> Ref:
        commit_ref = self._read_pointer_commit(pointer_path)
        self._require_commit(commit_ref, txn)
        return commit_ref

    def _update_pointer_commit(self, pointer_path: Path, old_commit: Ref, new_commit: Ref, txn) -> Ref:
        self._require_commit(old_commit, txn)
        self._require_commit(new_commit, txn)
        self._mutate_pointer(pointer_path, expected_old_commit=old_commit, new_commit=new_commit)
        return new_commit

    def _read_pointer_commit(self, pointer_path: Path) -> Ref:
        if not pointer_path.exists():
            raise DmlRepoError(f"Pointer does not exist: {pointer_path}")
        commit_id = pointer_path.read_text(encoding="utf-8").strip()
        if not commit_id or "/" in commit_id or "\\" in commit_id or ":" in commit_id:
            raise DmlRepoError(f"Invalid pointer payload in {pointer_path}")
        return Ref(f"commit:{commit_id}")

    def _mutate_pointer(
        self,
        pointer_path: Path,
        *,
        expected_old_commit: Ref | None,
        new_commit: Ref,
        create_only: bool = False,
    ) -> None:
        with self._pointer_lock(pointer_path):
            if create_only:
                if pointer_path.exists():
                    raise DmlRepoError(f"Branch already exists: {pointer_path.name}")
            else:
                current_commit = self._read_pointer_commit(pointer_path)
                if expected_old_commit is not None and current_commit != expected_old_commit:
                    msg = f"Stale pointer update rejected for {pointer_path.name}"
                    raise DmlPointerConflictError(msg, current_commit=current_commit)
            self._write_pointer_commit(pointer_path, new_commit)

    def _write_pointer_commit(self, pointer_path: Path, commit_ref: Ref) -> None:
        if commit_ref.ns() != "commit":
            raise DmlRepoError(f"Expected commit ref, got: {commit_ref}")
        pointer_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=pointer_path.parent, delete=False) as tmp:
            tmp.write(commit_ref.id())
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, pointer_path)

    def _pointer_lock(self, pointer_path: Path):
        pointer_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = pointer_path.parent / f"{pointer_path.name}.lock"
        return _FileLock(lock_path)

    @staticmethod
    def _require_commit(commit_ref: Ref, txn) -> None:
        if commit_ref.ns() != "commit":
            raise DmlRepoError(f"Expected commit ref, got: {commit_ref}")
        if not txn.exists(commit_ref):
            raise DmlRepoError(f"Commit does not exist: {commit_ref}")


class _FileLock:
    def __init__(self, path: Path):
        self._path = path
        self._fh = None

    def __enter__(self):
        import fcntl

        self._fh = self._path.open("a+", encoding="utf-8")
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb):
        import fcntl

        assert self._fh is not None
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        self._fh.close()
        self._fh = None
