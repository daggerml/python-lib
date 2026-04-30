"""Main DaggerML repository class and operations.

Public API:
    Dml - Main repository class providing complete DML functionality
"""

import importlib
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
from daggerml._internal.config import (
    DmlConfig,
    DmlProjectConfig,
    init_project_layout,
    parse_dml_project_uri,
    run_project_hooks,
)
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

    def fetch_project(self, remote_or_uri: str, branch: str | None, *, s3_client: Any | None = None) -> Ref:
        client = s3_client or self._create_s3_client()
        return self.remote(client=client).fetch_uri(self._project_remote_uri(remote_or_uri, branch))

    def pull_project(
        self,
        remote_or_uri: str,
        branch: str | None,
        *,
        head: Ref,
        user: str,
        s3_client: Any | None = None,
    ) -> Ref:
        client = s3_client or self._create_s3_client()
        return self.remote(client=client).pull_uri_into_head(
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
        s3_client: Any | None = None,
    ) -> str:
        project = self._load_project_config()
        client = s3_client or self._create_s3_client()
        remote = self.remote(client=client)
        if tag:
            return remote.push_project_tag(f"{project.uri}@{tag}", head)
        return remote.push_project_branch(
            f"{project.uri}#{project.branch}",
            head,
            create=create,
            force=force,
        )

    def checkout_dag_from_revision(
        self,
        revision: str,
        source_name: str,
        *,
        target_name: str | None = None,
        replace: bool = False,
        head: Ref | None = None,
        branch: str | None = None,
        user: str | None = None,
    ) -> Ref:
        project = self._load_project_config()
        current_branch = branch or project.branch
        effective_user = user or DmlConfig.resolve(explicit={"project.home": self.path}).user
        if not effective_user:
            raise DmlRepoError("user is required for dag checkout; pass --user or set DML_USER/config user.name")
        head_ref = head or Ref(f"head:{current_branch}")
        commit_ops = self.commit()
        source_commit = commit_ops.resolve_revision_ref(
            revision,
            current_branch=current_branch,
            project_dir=self.path,
        )
        return commit_ops.checkout_dag(
            head_ref,
            source_commit,
            source_name,
            target_name=target_name,
            replace=replace,
            user=effective_user,
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

    @staticmethod
    def _default_owner(value: str) -> str:
        import re

        owner = value.split("@", 1)[0].lower()
        owner = re.sub(r"[^a-z0-9._-]+", "-", owner).strip("-._")
        return owner or "dml"

    @staticmethod
    def _create_s3_client() -> Any:
        try:
            boto3 = importlib.import_module("boto3")
        except ImportError as exc:
            raise DmlRepoError("Remote commands require boto3; install boto3 to continue") from exc
        return boto3.client("s3")

    @classmethod
    def init(
        cls,
        path: str | None = None,
        *,
        name: str | None = None,
        owner: str | None = None,
        branch: str | None = None,
        project_uri: str | None = None,
        remote_uri: str | None = None,
        user: str | None = None,
        config_home: str | None = None,
        no_hooks: bool = False,
    ) -> dict[str, str | None]:
        root = Path(path) if path else Path.cwd()
        if name is not None and project_uri is not None:
            raise ValueError(
                "NAME and --project-uri are mutually exclusive; provide NAME to derive project URI or use "
                "--project-uri for an explicit URI"
            )

        global_cfg = DmlConfig.resolve(
            scope="global",
            explicit={
                "project.home": str(root),
                "user": user,
                "config_home": config_home,
            }
        )
        if not root.exists():
            raise FileNotFoundError(f"Project directory does not exist: {root}")
        if not root.is_dir():
            raise NotADirectoryError(f"Project path is not a directory: {root}")

        project_name = name or root.name
        if project_name and ("/" in project_name or "\\" in project_name):
            raise ValueError("Repository NAME must not contain path separators")
        branch_name = branch or global_cfg.default_branch
        owner_name = owner or cls._default_owner(str(global_cfg.user or "dml"))

        config_path = root / ".dml" / "config.toml"
        config_exists = config_path.exists()
        if name is not None and not project_uri:
            if not global_cfg.user:
                raise DmlRepoError(
                    "user is required to derive project URI from NAME; set DML_USER or configure global user.name"
                )
            owner_name = cls._default_owner(global_cfg.user)
            project_uri = f"dml://{owner_name}/{project_name}#{branch_name}"
        elif not project_uri and not config_exists:
            project_uri = f"dml://{owner_name}/{project_name}#{branch_name}"
        if remote_uri is None:
            remote_uri = global_cfg.remote.uri

        cfg = DmlConfig.resolve(
            explicit={
                "project.home": str(root),
                "project.uri": project_uri,
                "remote.uri": remote_uri,
                "user": user,
                "default_branch": branch_name,
                "config_home": config_home,
            }
        )
        if not cfg.project.home:
            raise DmlRepoError("project.home is required for init")

        db_path = Path(cls._db_path(str(root)))
        db_exists = db_path.exists()
        needs_recovery_pull = config_exists and not db_exists and bool(
            DmlConfig.resolve(explicit={"project.home": str(root)}).project.uri
        )

        if (project_uri or config_exists) and not cfg.remote.uri:
            raise DmlRepoError("remote.uri is required when project.uri is configured")

        gitignore_path = root / ".dml" / ".gitignore"
        if not config_exists or not db_exists or not gitignore_path.exists():
            if config_exists:
                layout_cfg = DmlProjectConfig.load(root)
            else:
                project_uri_value = cfg.project.uri
                if not project_uri_value:
                    raise DmlRepoError("project.uri is required for init")
                project_ref = parse_dml_project_uri(project_uri_value, require_identifier=True)
                if project_ref.branch is None:
                    raise DmlRepoError("project.uri is required for init")
                layout_cfg = DmlProjectConfig(
                    name=project_ref.project,
                    owner=project_ref.owner,
                    branch=project_ref.branch,
                    remote_uri=cfg.remote.uri,
                )
            init_project_layout(root, layout_cfg)

        if not db_exists:
            with cls.create(str(root), user=cfg.user, remote_root=cfg.remote.uri, branch=cfg.branch):
                pass

        recovered_ref: str | None = None
        if needs_recovery_pull:
            if not cfg.user:
                raise DmlRepoError("user is required for recovery pull")
            s3_client = cls._create_s3_client()
            with cls.open(str(root), remote_root=cfg.remote.uri) as ops:
                recovered_ref = ops.pull_project(
                    "origin", None, head=Ref(f"head:{cfg.branch}"), user=cfg.user, s3_client=s3_client
                ).to

        project_cfg = DmlProjectConfig.load(root)
        run_project_hooks(
            "post-init",
            cfg.hooks.post_init,
            project_dir=root,
            project=project_cfg,
            config_home=Path(cfg.config_home),
            no_hooks=no_hooks,
        )

        return {
            "name": name,
            "repo_path": str(root),
            "db_path": str(db_path),
            "head": f"head:{cfg.branch}",
            "recovered": recovered_ref,
        }

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
