"""Main DaggerML repository class and operations.

Public API:
    Dml - Main repository class providing complete DML functionality
"""

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, ContextManager, Optional

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from daggerml._internal._db import DmlDbEnv, Ref
from daggerml._internal.config import DmlConfig, DmlProjectConfig, init_project_layout, run_project_hooks
from daggerml._internal.types import DEFAULT_HEAD, NAMESPACES, DmlRepoError

if TYPE_CHECKING:
    from daggerml._internal.ops.cache import CacheOps
    from daggerml._internal.ops.commit import CommitOps
    from daggerml._internal.ops.config import ConfigOps
    from daggerml._internal.ops.dag import DagOps
    from daggerml._internal.ops.gc import GcOps
    from daggerml._internal.ops.head import HeadOps
    from daggerml._internal.ops.index import IndexOps
    from daggerml._internal.ops.node import NodeOps
    from daggerml._internal.ops.remote import RemoteOps


@dataclass
class DmlOps:
    """DaggerML repository interface for managing versioned data and DAGs.
    This class provides a high-level interface for interacting with a DaggerML
    repository. It manages the database connection and exposes dynamic operation
    classes for commits, heads, indexes, DAGs, nodes, caching, and garbage collection.
    Attributes
    ----------
    path : str
        Filesystem path to the DaggerML repository.
    """

    path: str
    remote_root: str
    _db: Optional[DmlDbEnv] = None

    def __enter__(self) -> Self:
        """Enter context manager - open database connection."""
        if self._db is None:
            self._db = DmlDbEnv.open(self._db_path(self.path), namespaces=sorted(NAMESPACES), map_size=1024**3)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - close database connection."""
        self.close()

    def close(self):
        """Close database connection and clean up resources."""
        if self._db is not None:
            db = self._db
            self._db = None
            db.close()

    def commit(self) -> "CommitOps":
        """Return commit operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml._internal.ops.commit import CommitOps

        return CommitOps(_db=self._db)

    def head(self) -> "HeadOps":
        """Return head operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml._internal.ops.head import HeadOps

        return HeadOps(_db=self._db)

    def index(self) -> "IndexOps":
        """Return index operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml._internal.ops.index import IndexOps

        return IndexOps(
            _db=self._db,
            remote_root=self.remote_root,
        )

    def dag(self) -> "DagOps":
        """Return DAG operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml._internal.ops.dag import DagOps

        return DagOps(_db=self._db)

    def node(self) -> "NodeOps":
        """Return node operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml._internal.ops.node import NodeOps

        return NodeOps(_db=self._db)

    def cache(self) -> "CacheOps":
        """Return cache operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml._internal.ops.cache import CacheOps

        return CacheOps(
            _db=self._db,
            remote_root=self.remote_root,
        )

    def gc(self) -> "GcOps":
        """Return garbage collection operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml._internal.ops.gc import GcOps

        return GcOps(_db=self._db)

    def config(self) -> "ConfigOps":
        """Return config operations."""
        from daggerml._internal.ops.config import ConfigOps

        cfg = DmlConfig.resolve(explicit={"project.home": self.path})
        return ConfigOps(project_home=cfg.project.home, config_home=cfg.config_home)

    @staticmethod
    def _split_remote_root(remote_root: str) -> tuple[str, str]:
        if not remote_root.startswith("s3://"):
            raise ValueError(f"Invalid remote root URI: {remote_root!r}")
        rest = remote_root[5:]
        if not rest:
            raise ValueError(f"Invalid remote root URI: {remote_root!r}")
        if "/" not in rest:
            return rest, "dml"
        bucket, prefix = rest.split("/", 1)
        prefix = prefix.strip("/")
        return bucket, f"{prefix}/dml" if prefix else "dml"

    def remote(
        self,
        client: Optional[Any] = None,
    ) -> "RemoteOps":
        """Return remote operations."""
        if self._db is None:
            raise RuntimeError("Database is not open.")
        from daggerml._internal.ops.remote import RemoteOps

        bucket, prefix = self._split_remote_root(self.remote_root)
        remote_kwargs: dict[str, Any] = {"bucket": bucket, "prefix": prefix}
        if client is not None:
            remote_kwargs["client"] = client

        return RemoteOps(_db=self._db, **remote_kwargs)

    def _load_project_config(self) -> DmlProjectConfig:
        return DmlProjectConfig.load(self.path)

    def _project_remote_uri(self, remote_or_uri: str, branch: str | None) -> str:
        project = self._load_project_config()
        if remote_or_uri.startswith("dml://"):
            if "#" in remote_or_uri or "@" in remote_or_uri:
                return remote_or_uri
            return f"{remote_or_uri}#{branch or project.branch}"
        if remote_or_uri != "origin":
            raise DmlRepoError(f"Unknown remote: {remote_or_uri}")
        return f"{project.uri}#{branch or project.branch}"

    @staticmethod
    def _looks_like_commit_id(value: str) -> bool:
        return len(value) == 64 and all(ch in "0123456789abcdef" for ch in value)

    def fetch_project(self, remote_or_uri: str, branch: str | None, *, s3_client: Any) -> Ref:
        return self.remote(client=s3_client).fetch_uri(self._project_remote_uri(remote_or_uri, branch))

    def pull_project(self, remote_or_uri: str, branch: str | None, *, head: Ref, user: str, s3_client: Any) -> Ref:
        return self.remote(client=s3_client).pull_uri_into_head(
            self._project_remote_uri(remote_or_uri, branch),
            head,
            user=user,
        )

    def push_project(
        self,
        tag: str | None,
        *,
        head: Ref,
        create: bool,
        force: bool,
        s3_client: Any,
    ) -> str:
        project = self._load_project_config()
        remote = self.remote(client=s3_client)
        if tag:
            return remote.push_project_tag(f"{project.uri}@{tag}", head)
        return remote.push_project_branch(
            f"{project.uri}#{project.branch}",
            head,
            create=create,
            force=force,
        )

    def checkout_project(self, revision: str) -> dict[str, str | None]:
        project = self._load_project_config()
        resolution = self.commit().resolve_revision(
            revision,
            current_branch=project.branch,
            project_dir=self.path,
        )
        if resolution.kind == "branch" and resolution.branch is not None:
            DmlProjectConfig(
                name=project.name,
                owner=project.owner,
                branch=resolution.branch,
                remote_uri=project.remote_uri,
            ).save(self.path)
            return {
                "commit": str(resolution.commit),
                "mode": "attached",
                "head": f"head:{resolution.branch}",
                "target": resolution.branch,
                "message": f"Checked out branch '{resolution.branch}' (attached)",
            }
        return {
            "commit": str(resolution.commit),
            "mode": "detached",
            "head": None,
            "target": revision,
            "message": f"Checked out {revision!r} in detached scratch mode",
        }

    def merge_project(self, revision: str, head: Ref, user: str) -> Ref:
        commit_ops = self.commit()
        other = commit_ops.resolve_revision_ref(revision, project_dir=self.path)
        return commit_ops.merge_into_head(head, other, user)

    def revert_project(self, revision: str, head: Ref, user: str) -> Ref:
        commit_ops = self.commit()
        commit_ref = commit_ops.resolve_revision_ref(revision, project_dir=self.path)
        return commit_ops.revert(head, commit_ref, user)

    @staticmethod
    def _db_path(path: str) -> str:
        root = Path(path)
        dml_db = root / ".dml" / "db"
        return str(dml_db)

    @classmethod
    def create(
        cls,
        path: str,
        user: Optional[str] = None,
        *,
        remote_root: str,
        branch: str | None = None,
    ) -> Self:
        """Create new repository at path (instantiates db instance)."""
        branch = branch or DEFAULT_HEAD.id()
        db_path = cls._db_path(path)
        Path(db_path).mkdir(parents=True, exist_ok=True)
        db = DmlDbEnv.create(db_path, namespaces=sorted(NAMESPACES), map_size=1024**3)
        self = cls(_db=db, path=path, remote_root=remote_root)
        self.head().create(branch)
        return self

    @classmethod
    def open(
        cls,
        path: str,
        map_size: int = 1024**3,
        *,
        remote_root: str,
    ) -> Self:
        """Open existing repository (instantiates db instance).

        Parameters
        ----------
        path : str
            Directory path of the repository.
        map_size : int
            Optional LMDB map size in bytes.

        Returns
        -------
        Self
            Opened repository instance.
        """
        db = DmlDbEnv.open(cls._db_path(path), namespaces=sorted(NAMESPACES), map_size=map_size)
        return cls(
            _db=db,
            path=path,
            remote_root=remote_root,
        )

    @classmethod
    def temporary(
        cls,
        user: Optional[str] = None,
        *,
        remote_root: str,
    ) -> ContextManager[Self]:
        """Create temporary repository for testing."""

        @contextmanager
        def _temporary():
            with TemporaryDirectory() as tmpdir:
                with cls.create(
                    f"{tmpdir}/db",
                    user,
                    remote_root=remote_root,
                ) as repo:
                    yield repo

        return _temporary()

    @classmethod
    def clone_project(
        cls,
        *,
        uri: str,
        bucket: str,
        prefix: str,
        branch: str | None,
        no_hooks: bool,
        s3_client: Any,
    ) -> dict[str, str | None]:
        from daggerml._internal.ops.remote import RemoteOps

        cfg = DmlConfig.resolve(scope="global")
        parsed = RemoteOps.parse_dml_uri(uri, require_identifier=False)
        if parsed.tag is not None and cls._looks_like_commit_id(parsed.tag):
            raise DmlRepoError(
                "Clone direct-commit targets are not supported yet; fetch currently supports only branch/tag refs"
            )

        local_branch = branch or parsed.branch or cfg.default_branch
        target = parsed.branch or parsed.tag or local_branch
        if target is None:
            raise DmlRepoError("Clone target could not be resolved")

        project_dir = Path(parsed.project)
        if project_dir.exists():
            raise FileExistsError(f"Project directory exists: {project_dir}")

        remote_root = f"s3://{bucket}/{prefix.strip('/')}" if prefix.strip("/") else f"s3://{bucket}"
        project = DmlProjectConfig(
            name=parsed.project,
            owner=parsed.owner,
            branch=local_branch,
            remote_uri=remote_root,
        )

        project_dir.mkdir()
        init_project_layout(project_dir, project)

        with cls.create(str(project_dir), remote_root=project.remote_uri, branch=local_branch) as ops:
            remote_target = f"{project.uri}#{target}" if parsed.tag is None else f"{project.uri}@{target}"
            ops.fetch_project(remote_target, None, s3_client=s3_client)
            checkout_result = ops.checkout_project(target)

        run_project_hooks(
            "post-clone",
            cfg.hooks.post_clone,
            project_dir=project_dir,
            project=project,
            config_home=cfg.config_home,
            remote_name="origin",
            no_hooks=no_hooks,
        )

        return {
            "project_dir": str(project_dir),
            "head": checkout_result["head"],
            "mode": str(checkout_result["mode"]),
            "commit": str(checkout_result["commit"]),
            "message": str(checkout_result["message"]),
        }
