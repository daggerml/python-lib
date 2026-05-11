from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal

from daggerml._internal._db import Ref
from daggerml._internal.config import DmlProjectConfig, init_project_layout, run_project_hooks
from daggerml._internal.dml_context import (
    config_dict,
    current_head_branch,
    current_head_state,
    db_path_for_project,
    effective_project_branch,
    gitignore_exists,
    load_project_config,
    mutable_branch,
    project_config_exists,
    project_remote_uri,
    require_project_home,
    require_user,
    resolve_global_context,
    resolve_runtime_context,
)
from daggerml._internal.dml_resolution import resolve_dag_ref, resolve_node_ref, resolve_revision, resolve_revision_ref
from daggerml._internal.ops import DmlOps
from daggerml._internal.ops.config import ConfigOps
from daggerml._internal.types import DEFAULT_HEAD, DmlRepoError


@dataclass(frozen=True)
class _OpsProxy:
    _dml: "Dml"
    _factory: str
    _factory_kwargs: dict[str, Any] | None = None

    def __getattr__(self, name: str):
        return lambda *args, **kwargs: self._dml._call_ops_method(
            self._factory, name, *args, factory_kwargs=self._factory_kwargs, **kwargs
        )


@dataclass(frozen=True)
class _OpsNamespace:
    _dml: "Dml"

    def commit(self):
        return self._dml._ops_proxy("commit")

    def head(self):
        return self._dml._ops_proxy("head")

    def dag(self):
        return self._dml._ops_proxy("dag")

    def node(self):
        return self._dml._ops_proxy("node")

    def index(self):
        return self._dml._ops_proxy("index")

    def cache(self):
        return self._dml._ops_proxy("cache")

    def remote(self):
        return self._dml._ops_proxy("remote")

    def gc(self):
        return self._dml._ops_proxy("gc")

    def config(self):
        return self._dml._ops_proxy("config")


@dataclass(frozen=True)
class _ConfigNamespace:
    _dml: "Dml"

    def get(self, key: str, *, scope: Literal["global", "local"] = "local"):
        return self._dml._config_ops().get(key, scope=scope)

    def set(self, key: str, values: list[str], *, scope: Literal["global", "local"] = "local"):
        return self._dml._config_ops().set(key, values, scope=scope)

    def show(self, *, contrib: bool = False) -> dict[str, Any]:
        payload = config_dict(self._dml._context.config)
        if contrib:
            from daggerml.contrib import status as contrib_status

            payload["contrib"] = contrib_status.status()
        return payload


@dataclass(frozen=True)
class _RuntimeNamespace:
    _dml: "Dml"

    def create(
        self,
        *,
        head: str | None = None,
        commit: Ref | None = None,
        argv_ptr: str | None = None,
        index_id: str | None = None,
    ) -> str:
        if head is None and commit is None and argv_ptr is None:
            head_state = self._dml._head_ops().get_head_state()
            head = head_state.branch
            commit = head_state.commit if head is None else None
        return self._dml._index_ops().create(head=head, commit=commit, argv_ptr=argv_ptr, index_id=index_id)

    def describe(self, index_id: str) -> dict[str, Any]:
        return self._dml._index_ops().describe(index_id)

    def get_node(self, index_id: str, name: str) -> Ref:
        return self._dml._index_ops().get_node(index_id, name)

    def get_argv(self, index_id: str) -> Ref:
        return self._dml._index_ops().get_argv(index_id)

    def put_literal(self, index_id: str, value: Any, *, name: str | None = None) -> Ref:
        return self._dml._index_ops().put_literal(index_id, value, name=name)

    def put_import(self, index_id: str, dag: Ref, *, node: Ref | None = None, name: str | None = None) -> Ref:
        return self._dml._index_ops().put_import(index_id, dag, node=node, name=name)

    def set_node_name(self, index_id: str, name: str, node: Ref) -> Ref:
        return self._dml._index_ops().set_node_name(index_id, name, node)

    def start_fn(
        self,
        index_id: str,
        argv: list[Ref],
        *,
        kwargv: dict[str, Ref] | None = None,
        name: str | None = None,
    ) -> Ref | None:
        return self._dml._index_ops().start_fn(index_id, argv, kwargv=kwargv, name=name)

    def commit(
        self,
        index_id: str,
        value: Ref | Any,
        *,
        head: str | None = None,
        message: str | None = None,
        dag_name: str | None = None,
    ) -> Ref:
        return self._dml._index_ops().commit(index_id, value, head=head, message=message, dag_name=dag_name)


@dataclass(frozen=True)
class _DagNamespace:
    _dml: "Dml"

    @staticmethod
    def _stringify_node_selector(node_selector: str | Ref) -> str:
        return node_selector.to if isinstance(node_selector, Ref) else node_selector

    def list(self, revision: str = "HEAD") -> dict[str, Any]:
        resolved = self._dml._resolve_revision(revision)
        return {
            "revision": self._dml._revision_payload(revision, resolved),
            "dags": self._dml._dag_map_for_commit(resolved.commit),
        }

    def describe(self, value: str | Ref, *, revision: str | None = None) -> dict[str, Any]:
        resolved = resolve_dag_ref(
            value=value,
            revision=revision,
            commit_ops=self._dml._commit_ops(),
            head_ops=self._dml._head_ops(),
            project_dir=require_project_home(self._dml._context.project_home),
            operation="describe",
        )
        payload = {"selector": resolved.selector, "dag": self._dml._dag_summary_payload(resolved.ref)}
        if resolved.revision is not None:
            payload["revision"] = self._dml._revision_payload(revision or "HEAD", resolved.revision)
        return payload

    def get(self, value: str | Ref, *, revision: str | None = None) -> dict[str, Any]:
        resolved = resolve_dag_ref(
            value=value,
            revision=revision,
            commit_ops=self._dml._commit_ops(),
            head_ops=self._dml._head_ops(),
            project_dir=require_project_home(self._dml._context.project_home),
            operation="get",
        )
        payload = {"selector": resolved.selector, "dag": self._dml._dag_payload(resolved.ref)}
        if resolved.revision is not None:
            payload["revision"] = self._dml._revision_payload(revision or "HEAD", resolved.revision)
        return payload

    def describe_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: str | None = None,
    ) -> dict[str, Any]:
        resolved = resolve_node_ref(
            value=node_selector,
            dag_selector=dag_selector,
            revision=revision,
            commit_ops=self._dml._commit_ops(),
            dag_ops=self._dml._dag_ops(),
            head_ops=self._dml._head_ops(),
            project_dir=require_project_home(self._dml._context.project_home),
            operation="describe-node",
        )
        payload: dict[str, Any] = {
            "selector": self._stringify_node_selector(node_selector),
            "dag_selector": resolved.dag_selector,
            "node": self._dml._node_ops().describe(resolved.ref),
        }
        if resolved.revision is not None:
            payload["revision"] = self._dml._revision_payload(revision or "HEAD", resolved.revision)
        return payload

    def get_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: str | None = None,
    ) -> dict[str, Any]:
        resolved = resolve_node_ref(
            value=node_selector,
            dag_selector=dag_selector,
            revision=revision,
            commit_ops=self._dml._commit_ops(),
            dag_ops=self._dml._dag_ops(),
            head_ops=self._dml._head_ops(),
            project_dir=require_project_home(self._dml._context.project_home),
            operation="get-node",
        )
        payload: dict[str, Any] = {
            "selector": self._stringify_node_selector(node_selector),
            "dag_selector": resolved.dag_selector,
            "node": self._dml._node_ops().get(resolved.ref),
        }
        if resolved.revision is not None:
            payload["revision"] = self._dml._revision_payload(revision or "HEAD", resolved.revision)
        return payload

    def unroll_node(
        self,
        node_selector: str | Ref,
        *,
        dag_selector: str | Ref | None = None,
        revision: str | None = None,
    ) -> dict[str, Any]:
        resolved = resolve_node_ref(
            value=node_selector,
            dag_selector=dag_selector,
            revision=revision,
            commit_ops=self._dml._commit_ops(),
            dag_ops=self._dml._dag_ops(),
            head_ops=self._dml._head_ops(),
            project_dir=require_project_home(self._dml._context.project_home),
            operation="unroll-node",
        )
        payload: dict[str, Any] = {
            "selector": self._stringify_node_selector(node_selector),
            "dag_selector": resolved.dag_selector,
            "node": self._dml._node_ops().unroll(resolved.ref),
        }
        if resolved.revision is not None:
            payload["revision"] = self._dml._revision_payload(revision or "HEAD", resolved.revision)
        return payload

    def checkout(
        self,
        revision: str,
        dag_name: str,
        *,
        branch: str | None = None,
        target_name: str | None = None,
        replace: bool = False,
        user: str | None = None,
    ) -> Ref:
        target_branch = mutable_branch(branch=branch, head_ops=self._dml._head_ops())
        author = require_user(user or self._dml._context.user, message="user is required for dag checkout")
        return self._dml._commit_ops().checkout_dag(
            target_branch,
            self._dml._resolve_revision_ref(revision),
            dag_name,
            target_name=target_name,
            replace=replace,
            user=author,
        )

    def delete(self, name: str, *, branch: str | None = None, user: str | None = None):
        author = require_user(user or self._dml._context.user, message="user is required for dag delete")
        return self._dml._commit_ops().delete_dag(name, branch, author)


@dataclass(frozen=True)
class _AdminIndexNamespace:
    _dml: "Dml"

    def list(self) -> dict[str, Any]:
        indexes = [self.get(index_id)["index"] for index_id in self._dml._head_ops().list_indexes()]
        return {"indexes": indexes}

    def get(self, index_id: str) -> dict[str, Any]:
        index = dict(self._dml._index_ops().describe(index_id))
        commit_ref = index["commit"]
        index["commit"] = {
            "ref": commit_ref,
            "summary": self._dml._commit_ops().describe(commit_ref),
        }
        return {"index": index}

    def delete(self, index_id: str) -> dict[str, Any]:
        self._dml._index_ops().delete(index_id)
        return {"index": index_id, "deleted": True}


@dataclass(frozen=True)
class _AdminCacheNamespace:
    _dml: "Dml"

    def invalidate(self, cache_keys: list[str]) -> dict[str, Any]:
        if not cache_keys:
            raise DmlRepoError("At least one cache key is required")
        for cache_key in cache_keys:
            if ":" in cache_key:
                raise DmlRepoError("Admin cache invalidation accepts exact cache keys only")
        requested_by = self._dml._context.user or "cli"
        invalidated = self._dml._remote_ops().invalidate_cache(cache_keys, requested_by=requested_by)
        return {"cache_keys": cache_keys, "invalidated": invalidated}


@dataclass(frozen=True)
class _AdminRemoteNamespace:
    _dml: "Dml"

    def list(self, project: str | None = None, *, owner: str | None = None) -> dict[str, Any]:
        refs = self._dml._remote_ops().list("projects")
        if project is None:
            projects: set[str] = set()
            for ref in refs:
                ref_path = ref.get("ref_path")
                if not isinstance(ref_path, str):
                    continue
                parts = ref_path.split("/")
                if len(parts) < 5 or parts[0] != "projects":
                    continue
                candidate_owner = parts[1]
                candidate_project = parts[2]
                if owner is not None and candidate_owner != owner:
                    continue
                projects.add(f"dml://{candidate_owner}/{candidate_project}")
            return {"projects": sorted(projects)}

        if not project.startswith("dml://") or "#" in project or "@" in project:
            raise DmlRepoError("Admin remote list expects a bare dml://<owner>/<project> project URI")
        parsed = self._dml._remote_ops().parse_dml_uri(project, require_identifier=False)
        branches: list[str] = []
        tags: list[str] = []
        for ref in refs:
            ref_path = ref.get("ref_path")
            if not isinstance(ref_path, str):
                continue
            parts = ref_path.split("/")
            if len(parts) < 5 or parts[0] != "projects" or parts[1] != parsed.owner or parts[2] != parsed.project:
                continue
            name = "/".join(parts[4:])
            if not name.endswith(".json"):
                continue
            name = name[:-5]
            if parts[3] == "heads":
                branches.append(f"dml://{parsed.owner}/{parsed.project}#{name}")
            elif parts[3] == "tags":
                tags.append(f"dml://{parsed.owner}/{parsed.project}@{name}")
        return {"project": project, "branches": sorted(branches), "tags": sorted(tags)}

    def gc(self, *, min_age_seconds: int = 24 * 3600, malformed: str = "warn") -> dict[str, int]:
        return self._dml._remote_ops().gc(min_age_seconds=min_age_seconds, malformed=malformed)


@dataclass(frozen=True)
class _AdminNamespace:
    _dml: "Dml"

    @property
    def index(self) -> _AdminIndexNamespace:
        return _AdminIndexNamespace(self._dml)

    @property
    def cache(self) -> _AdminCacheNamespace:
        return _AdminCacheNamespace(self._dml)

    @property
    def remote(self) -> _AdminRemoteNamespace:
        return _AdminRemoteNamespace(self._dml)

    def gc(self, *, dry_run: bool = False) -> dict[str, Any]:
        if dry_run:
            orphans = self._dml._gc_ops().list_orphans()
            return {"dry_run": True, "would_delete": len(orphans), "orphans": orphans}
        return {"dry_run": False, "deleted": self._dml._gc_ops().gc()}


class Dml:
    def __init__(
        self,
        project_home: str | None = None,
        *,
        remote_uri: str | None = None,
        user: str | None = None,
        config_home: str | None = None,
    ):
        self._context = resolve_runtime_context(
            project_home=project_home,
            remote_uri=remote_uri,
            user=user,
            config_home=config_home,
        )
        self._tempdirs: list[TemporaryDirectory[str]] = []

    def __enter__(self) -> "Dml":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.cleanup()

    def cleanup(self) -> None:
        while self._tempdirs:
            self._tempdirs.pop().cleanup()

    @contextmanager
    def _with_ops(self):
        ops = self._ops()
        try:
            yield ops
        finally:
            ops.close()

    def _ops_proxy(self, factory: str, **factory_kwargs) -> _OpsProxy:
        return _OpsProxy(self, factory, factory_kwargs or None)

    def _call_ops_method(
        self, factory: str, method: str, *args, factory_kwargs: dict[str, Any] | None = None, **kwargs
    ):
        with self._with_ops() as ops:
            return getattr(getattr(ops, factory)(**(factory_kwargs or {})), method)(*args, **kwargs)

    def _ops(self):
        project_home = require_project_home(self._context.project_home)
        return DmlOps.open(project_home, remote_root=self._context.remote_uri)

    def _head_ops(self):
        return self._ops_proxy("head")

    def _commit_ops(self):
        return self._ops_proxy("commit")

    def _dag_ops(self):
        return self._ops_proxy("dag")

    def _node_ops(self):
        return self._ops_proxy("node")

    def _index_ops(self):
        return self._ops_proxy("index")

    def _cache_ops(self):
        return self._ops_proxy("cache")

    def _remote_ops(self, *, s3_client=None, client=None):
        return self._ops_proxy("remote", client=s3_client or client)

    def _gc_ops(self):
        return self._ops_proxy("gc")

    def _config_ops(self):
        return ConfigOps(project_home=self._context.project_home, config_home=self._context.config.config_home)

    def _tree_dags(self, tree_ref: Ref) -> dict[str, Ref]:
        with self._with_ops() as ops:
            with ops.commit()._tx(readonly=True) as txn:
                tree = txn.get(tree_ref)
                return dict(tree.dags)

    def _dag_map_for_commit(self, commit_ref: Ref) -> dict[str, Ref]:
        return self._tree_dags(self._commit_ops().describe(commit_ref)["tree"])

    def _dag_summary_payload(self, dag_ref: Ref) -> dict[str, Any]:
        dag = dict(self._dag_ops().describe(dag_ref))
        dag["ref"] = dag_ref
        return dag

    @staticmethod
    def _dag_map_diff(left: dict[str, Ref], right: dict[str, Ref]) -> dict[str, Any]:
        added: dict[str, Ref] = {}
        removed: dict[str, Ref] = {}
        updated: dict[str, dict[str, Ref]] = {}
        for name in sorted(set(left) | set(right)):
            before = left.get(name)
            after = right.get(name)
            if before is None and after is not None:
                added[name] = after
            elif before is not None and after is None:
                removed[name] = before
            elif before is not None and after is not None and before != after:
                updated[name] = {"before": before, "after": after}
        return {"added": added, "removed": removed, "updated": updated}

    @staticmethod
    def _revision_payload(value: str, resolved) -> dict[str, Any]:
        return {
            "input": value,
            "kind": resolved.kind,
            "commit": resolved.commit,
            "branch": resolved.branch,
            "tag": resolved.tag,
        }

    def _remote_tracking_branches(self) -> list[str]:
        project_home = require_project_home(self._context.project_home)
        remote_root = Path(project_home) / ".dml" / "refs" / "remote"
        if not remote_root.exists():
            return []
        branches: list[str] = []
        for ref_path in sorted(remote_root.glob("*/*/heads/**/*")):
            if not ref_path.is_file() or ref_path.name.endswith(".lock"):
                continue
            relative = ref_path.relative_to(remote_root)
            parts = relative.parts
            if len(parts) < 4:
                continue
            owner = parts[0]
            project = parts[1]
            branch_name = "/".join(parts[3:])
            branches.append(f"dml://{owner}/{project}#{branch_name}")
        return branches

    def _dag_payload(self, dag_ref: Ref) -> dict[str, Any]:
        dag = self._dag_summary_payload(dag_ref)
        node_refs = list(dag["nodes"])
        dag["nodes"] = [self._node_ops().describe(node_ref) for node_ref in node_refs]
        return dag

    def _resolve_revision(self, value: str):
        return resolve_revision(
            value=value,
            commit_ops=self._commit_ops(),
            head_ops=self._head_ops(),
            project_dir=require_project_home(self._context.project_home),
        )

    def _resolve_revision_ref(self, value: str) -> Ref:
        return resolve_revision_ref(
            value=value,
            commit_ops=self._commit_ops(),
            head_ops=self._head_ops(),
            project_dir=require_project_home(self._context.project_home),
        )

    def _runtime_branch(self) -> str:
        return effective_project_branch(
            branch=None,
            head_ops=self._head_ops(),
            default_branch=self._context.default_branch,
        )

    def _create_s3_client(self):
        import boto3
        from botocore.config import Config

        return boto3.client("s3", config=Config(max_pool_connections=20))

    @property
    def ops(self) -> _OpsNamespace:
        with self._with_ops():
            pass
        return _OpsNamespace(self)

    @property
    def config(self) -> _ConfigNamespace:
        return _ConfigNamespace(self)

    @property
    def runtime(self) -> _RuntimeNamespace:
        return _RuntimeNamespace(self)

    @property
    def dag(self) -> _DagNamespace:
        return _DagNamespace(self)

    @property
    def admin(self) -> _AdminNamespace:
        return _AdminNamespace(self)

    def status(self) -> dict[str, object]:
        if not self._context.project_home or not project_config_exists(
            require_project_home(self._context.project_home)
        ):
            return {
                "head": None,
                "branches": [],
                "dags": {},
                "indexes": [],
            }
        head_state = current_head_state(self._head_ops())
        return {
            "head": {
                "mode": head_state.mode,
                "branch": head_state.branch,
                "commit": head_state.commit,
            },
            "branches": self._head_ops().list_branches(),
            "dags": self._dag_map_for_commit(head_state.commit),
            "indexes": self._head_ops().list_indexes(),
        }

    def branch(self, *, remote: bool = False) -> dict[str, object]:
        if remote:
            return {"branches": self._remote_tracking_branches(), "remote": True}
        head_ops = self._head_ops()
        return {
            "branches": head_ops.list_branches(),
            "head": current_head_branch(head_ops),
            "remote": False,
        }

    def log(self, revision: str = "HEAD", *, limit: int | None = None) -> dict[str, Any]:
        resolved = self._resolve_revision(revision)
        with self._with_ops() as ops:
            refs = list(ops.commit().list(resolved.commit, limit=limit))
        return {
            "revision": self._revision_payload(revision, resolved),
            "commits": [self._commit_ops().describe(ref) for ref in refs],
        }

    def show(self, revision: str = "HEAD") -> dict[str, Any]:
        resolved = self._resolve_revision(revision)
        commit = self._commit_ops().describe(resolved.commit)
        dags = self._dag_map_for_commit(resolved.commit)
        base_commit = commit["parents"][0] if commit["parents"] else None
        base_dags = self._dag_map_for_commit(base_commit) if base_commit is not None else {}
        return {
            "revision": self._revision_payload(revision, resolved),
            "commit": commit,
            "dags": dags,
            "change": {"base": base_commit, **self._dag_map_diff(base_dags, dags)},
        }

    def diff(self, left: str = "HEAD~1", right: str = "HEAD") -> dict[str, Any]:
        left_resolved = self._resolve_revision(left)
        right_resolved = self._resolve_revision(right)
        left_dags = self._dag_map_for_commit(left_resolved.commit)
        right_dags = self._dag_map_for_commit(right_resolved.commit)
        return {
            "left": self._revision_payload(left, left_resolved),
            "right": self._revision_payload(right, right_resolved),
            **self._dag_map_diff(left_dags, right_dags),
        }

    def checkout(self, revision: str) -> dict[str, str | None]:
        resolved = self._resolve_revision(revision)
        head_ops = self._head_ops()
        if resolved.kind == "branch" and resolved.branch is not None:
            head_ops.write_attached_head(resolved.branch)
            return {"mode": "attached", "branch": resolved.branch}
        head_ops.write_detached_head(resolved.commit)
        return {"mode": "detached", "branch": None}

    def fetch(self, remote_or_uri: str, branch: str | None, *, s3_client=None) -> Ref:
        project_home = require_project_home(self._context.project_home)
        uri = project_remote_uri(
            project_home=project_home,
            remote_or_uri=remote_or_uri,
            branch=branch,
            default_branch=self._context.default_branch,
        )
        client = s3_client or self._create_s3_client()
        with self._with_ops() as ops:
            return ops.remote(client=client).fetch_uri(uri)

    def pull(
        self, remote_or_uri: str, remote_branch: str | None, *, branch: str | None, user: str, s3_client=None
    ) -> Ref:
        project_home = require_project_home(self._context.project_home)
        target_branch = mutable_branch(branch=branch, head_ops=self._head_ops())
        uri = project_remote_uri(
            project_home=project_home,
            remote_or_uri=remote_or_uri,
            branch=remote_branch or target_branch,
            default_branch=self._context.default_branch,
        )
        client = s3_client or self._create_s3_client()
        with self._with_ops() as ops:
            return ops.remote(client=client).pull_uri_into_branch(uri, target_branch, user=user)

    def push(self, tag: str | None, *, branch: str | None, create: bool, force: bool, s3_client=None) -> str:
        project = load_project_config(require_project_home(self._context.project_home))
        source_branch = branch or self._head_ops().require_attached_head_branch()
        client = s3_client or self._create_s3_client()
        with self._with_ops() as ops:
            remote = ops.remote(client=client)
            if tag:
                return remote.push_project_tag(f"{project.uri}@{tag}", source_branch)
            return remote.push_project_branch(
                f"{project.uri}#{source_branch}", source_branch, create=create, force=force
            )

    def merge(self, revision: str, branch: str | None, user: str):
        target_branch = mutable_branch(branch=branch, head_ops=self._head_ops())
        return self._commit_ops().merge_into_head(target_branch, self._resolve_revision_ref(revision), user)

    def revert(self, revision: str, branch: str | None, user: str):
        target_branch = mutable_branch(branch=branch, head_ops=self._head_ops())
        return self._commit_ops().revert(target_branch, self._resolve_revision_ref(revision), user)

    def load(self, name_or_node):
        from daggerml.api import Dag, Node

        if isinstance(name_or_node, Node):
            desc = self._node_ops().describe(name_or_node.ref)
            dag_ref = desc.get("dag")
            if dag_ref is None:
                raise DmlRepoError("Node is not linked to a DAG")
            return Dag(dml=self, ref=dag_ref)
        commit = self._head_ops().resolve_head_commit()
        dag_ref = self._commit_ops().get_dag(commit, name_or_node)
        if dag_ref is None:
            raise DmlRepoError(f"DAG '{name_or_node}' not found")
        return Dag(dml=self, ref=dag_ref, name=name_or_node)

    @classmethod
    def temporary(
        cls,
        repo: str = "test",
        user: str = "user",
        branch: str = "main",
        remote_root: str | None = None,
    ):
        @contextmanager
        def _temporary_context():
            with TemporaryDirectory() as tmpdir:
                project_home = str(Path(tmpdir) / repo)
                Path(project_home).mkdir(parents=True, exist_ok=True)
                runtime = cls(
                    project_home=project_home, remote_uri=remote_root or "s3://test-bucket/test-prefix", user=user
                )
                init_project_layout(
                    project_home,
                    DmlProjectConfig(name=repo, owner=user.split("@", 1)[0], remote_uri=runtime._context.remote_uri),
                )
                with DmlOps.create(project_home, user=user, branch=branch, remote_root=runtime._context.remote_uri):
                    with runtime:
                        yield runtime

        return _temporary_context()

    @classmethod
    def init(
        cls,
        project_home: str | None = None,
        *,
        name: str | None = None,
        owner: str | None = None,
        branch: str | None = None,
        remote_uri: str | None = None,
        user: str | None = None,
        config_home: str | None = None,
        project_uri: str | None = None,
        no_hooks: bool = False,
    ) -> dict[str, object]:
        root = Path(project_home or ".").resolve()
        if not root.exists():
            raise FileNotFoundError(f"{root} does not exist")
        project_home = str(root)
        if name and project_uri:
            raise ValueError(
                "NAME and --project-uri are mutually exclusive; provide NAME to derive "
                "project URI or use --project-uri for an explicit URI"
            )

        global_context = resolve_global_context(project_home=project_home, user=user, config_home=config_home)
        resolved_branch = branch or global_context.default_branch or DEFAULT_HEAD

        recovering = project_config_exists(project_home) and not db_path_for_project(project_home).exists()
        if recovering:
            runtime = cls(project_home=project_home, remote_uri=remote_uri, user=user, config_home=config_home)
            with DmlOps.create(
                project_home,
                user=runtime._context.user,
                branch=resolved_branch,
                remote_root=runtime._context.remote_uri,
            ):
                if load_project_config(project_home).uri:
                    if not runtime._context.remote_uri:
                        raise DmlRepoError("remote.uri is required")
                    runtime.pull(
                        "origin",
                        None,
                        branch=None,
                        user=require_user(runtime._context.user, message="user is required"),
                    )
            return {"branch": resolved_branch, "project_home": project_home, "recovered": True}

        resolved_user = user or global_context.user
        cfg_owner = owner
        cfg_name = name
        if project_uri:
            project = DmlProjectConfig.load(project_home) if project_config_exists(project_home) else None
            if project is not None:
                cfg_owner = project.owner
                cfg_name = project.name
            else:
                from daggerml._internal.config import parse_dml_project_uri

                parsed = parse_dml_project_uri(project_uri)
                cfg_owner = parsed.owner
                cfg_name = parsed.project
        elif name:
            resolved_user = require_user(resolved_user, message="user is required to derive project URI from NAME")
            cfg_owner = resolved_user.split("@", 1)[0]
            cfg_name = name
        else:
            raise DmlRepoError("Either NAME or project_uri is required")

        project_cfg = DmlProjectConfig(name=cfg_name, owner=cfg_owner, remote_uri=remote_uri or "")
        init_project_layout(root, project_cfg)
        if not gitignore_exists(project_home):
            (root / ".dml" / ".gitignore").write_text("db\nHEAD\nrefs\n")
        with DmlOps.create(project_home, user=resolved_user, branch=resolved_branch, remote_root=remote_uri or ""):
            run_project_hooks(
                "post-init",
                global_context.config.hooks.post_init,
                project_dir=project_home,
                project=project_cfg,
                config_home=global_context.config.config_home,
                remote_name="origin" if remote_uri else None,
                no_hooks=no_hooks,
            )
        return {"branch": resolved_branch, "project_home": project_home, "project_uri": project_cfg.project_uri}
