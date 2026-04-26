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

from daggerml._internal._db import DmlDbEnv
from daggerml._internal.types import DEFAULT_HEAD, NAMESPACES

if TYPE_CHECKING:
    from daggerml._internal.ops.cache import CacheOps
    from daggerml._internal.ops.commit import CommitOps
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
