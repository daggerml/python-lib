"""Read-only projections over DaggerML repository and execution state."""
# ruff: noqa: E501

from __future__ import annotations

import base64
import json
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urlsplit

from daggerml._core import Dml, DmlRepoError, Error, Ref, Runnable, Uri
from daggerml.dashboard.logs import read_cloudwatch_log
from daggerml.dashboard.serialization import bounded_json, project_runnable, redact


def _offset_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(str(offset).encode()).decode().rstrip("=")


def _parse_cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        value = int(base64.urlsafe_b64decode(padded).decode())
    except (ValueError, UnicodeError) as exc:
        raise ValueError("Invalid pagination cursor") from exc
    if value < 0:
        raise ValueError("Invalid pagination cursor")
    return value


def _ref(value: str, namespace: str) -> Ref:
    ref = Ref(value if ":" in value else f"{namespace}:{value}")
    if ref.nss()[0] != namespace:
        raise ValueError(f"Expected a {namespace} ref")
    return ref


def _ref_text(value: Ref | str) -> str:
    """Return the stable wire representation of a DaggerML reference."""
    return value.to if isinstance(value, Ref) else str(value)


def _uri_text(value: Uri | str | Mapping[str, Any] | Any) -> str | None:
    """Normalize persisted URI values without accepting arbitrary objects."""
    if isinstance(value, Uri):
        return value.uri
    if isinstance(value, Mapping) and isinstance(value.get("uri"), str):
        return value["uri"]
    return value if isinstance(value, str) else None


class RevisionError(ValueError):
    """A safe, stable failure while resolving a dashboard revision."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class ScriptReadError(Exception):
    """A safe, stable failure while reading runnable script evidence."""

    def __init__(self, code: str, message: str, *, status_code: int):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def _commit_id(value: Ref | str) -> str:
    return _ref_text(value).removeprefix("commit:")


def _datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        parsed = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("Timestamp is unavailable")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _rfc3339(value: Any) -> str:
    return _datetime(value).isoformat().replace("+00:00", "Z")


def _live_state(runtime: Mapping[str, Any], root_record: Mapping[str, Any] | None) -> tuple[str, str | None]:
    lifecycle = str((root_record or {}).get("lifecycle") or "")
    if lifecycle in {"cancel-requested", "cancel-ready"}:
        return "canceling", "Cancellation is in progress"
    if lifecycle == "canceled":
        return "canceled", "Canceled"
    frozen_message = runtime.get("frozen_message")
    if runtime.get("state") == "frozen":
        return "needs-attention", str(frozen_message or "Index is frozen")
    if lifecycle == "failed":
        return "needs-attention", "Execution failed"
    return "in-progress", None


def _timeline_record(execution_id: str, record: Mapping[str, Any], index_created: Any) -> dict[str, Any]:
    created = record.get("created_at")
    updated = record.get("updated_at")
    lifecycle = str(record.get("lifecycle") or "unknown")
    terminal = lifecycle in {"succeeded", "failed", "canceled"}
    predates = False
    if terminal and updated is not None:
        try:
            predates = _datetime(updated) < _datetime(index_created)
        except (TypeError, ValueError):
            pass
    timing = "predates-index" if predates else "open" if updated is None else "recorded"
    return {
        "execution_id": execution_id,
        "lifecycle": lifecycle,
        "created_at": _rfc3339(created) if created is not None else None,
        "updated_at": _rfc3339(updated) if updated is not None else None,
        "timing": timing,
        "predates_index": predates,
        "children": [str(value) for value in record.get("children", record.get("child_execution_ids", []))],
        "spawned": [str(value) for value in record.get("spawned", record.get("spawned_execution_ids", []))],
    }


def _timeline_records(graph: Mapping[str, Any], index_created: Any) -> list[dict[str, Any]]:
    """Flatten one reachable graph in stable, parent-before-child display order."""
    raw_nodes = graph.get("nodes")
    nodes = raw_nodes if isinstance(raw_nodes, Mapping) else {}
    raw_roots = graph.get("roots")
    roots = [str(value) for value in raw_roots] if isinstance(raw_roots, (list, tuple)) else []
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    pending: list[tuple[str, str | None, int]] = [(root, None, 0) for root in roots]
    while pending:
        execution_id, parent_id, depth = pending.pop(0)
        if execution_id in seen:
            continue
        seen.add(execution_id)
        raw = nodes.get(execution_id)
        record = raw if isinstance(raw, Mapping) else {}
        row = _timeline_record(execution_id, record, index_created)
        row["parent_execution_id"] = parent_id
        row["depth"] = depth
        rows.append(row)
        linked = [*row["spawned"], *row["children"]]
        pending[0:0] = [(child, execution_id, depth + 1) for child in linked if child not in seen]
    for execution_id, raw in nodes.items():
        if str(execution_id) in seen:
            continue
        record = raw if isinstance(raw, Mapping) else {}
        row = _timeline_record(str(execution_id), record, index_created)
        row["parent_execution_id"] = None
        row["depth"] = 0
        rows.append(row)
    return rows


class DashboardReadModel:
    """A fail-soft, bounded view of a local DaggerML project."""

    def __init__(
        self,
        project_home: str | Path | None,
        *,
        dml_factory: Callable[..., Dml] = Dml,
        cloudwatch_client_factory: Callable[[], Any] | None = None,
        s3_client_factory: Callable[[], Any] | None = None,
    ):
        self.project_home = Path(project_home).expanduser().resolve() if project_home is not None else None
        self._dml_factory = dml_factory
        self._cloudwatch_client_factory = cloudwatch_client_factory
        self._s3_client_factory = s3_client_factory
        self._dml: Dml | None = None
        self._remote_descriptor_verified = False
        self._revision_navigation: dict[str, dict[str, set[Ref]]] = {}

    @property
    def initialized(self) -> bool:
        return self.project_home is not None and (self.project_home / ".dml" / "HEAD").is_file()

    @property
    def dml(self) -> Dml:
        if not self.initialized:
            if self.project_home is None:
                raise DmlRepoError("Dashboard has no configured project")
            raise DmlRepoError(f"Not an initialized DaggerML project: {self.project_home}")
        if self._dml is None:
            self._dml = self._dml_factory(project_home=str(self.project_home))
        return self._dml

    def resolve_revision(self, revision: str) -> dict[str, Any]:
        """Resolve only ``HEAD`` or a concrete local commit, without fallback."""
        if not isinstance(revision, str) or (revision != "HEAD" and not re.fullmatch(r"[0-9a-f]{64}", revision)):
            raise RevisionError("invalid-revision", "Revision must be HEAD or a concrete commit ID")
        current = self.dml.status().get("commit")
        current_id = _commit_id(current) if current is not None else None
        if revision == "HEAD":
            if current_id is None:
                return {
                    "requested": revision,
                    "state": "unborn",
                    "is_current_head": False,
                }
            commit_id = current_id
        else:
            commit_id = revision
        try:
            self.dml.show(f"commit:{commit_id}")
        except (DmlRepoError, ValueError, KeyError) as exc:
            raise RevisionError("revision-not-found", "Revision is not available in this project") from exc
        return {
            "requested": revision,
            "state": "ready",
            "commit": commit_id,
            **({"current_head": current_id} if current_id is not None else {}),
            "is_current_head": commit_id == current_id,
        }

    def _scoped_href(self, path: str, project: str | None, revision: str | None) -> str:
        if project is None or revision is None:
            return path
        separator = "&" if "?" in path else "?"
        return f"{path}{separator}project={quote(project, safe='')}&revision={quote(revision, safe='')}"

    def overview(self, revision: str = "HEAD") -> dict[str, Any]:
        scope = self.resolve_revision(revision)
        status = bounded_json(self.dml.status())
        config = self.dml.config.show()
        safe_config = {
            "project_home": config.get("project_home"),
            "default": config.get("default", {}),
            "remote": config.get("remote", {}),
        }
        runtimes = self.runtimes()["items"]
        repository: dict[str, Any] = {}
        if scope["state"] == "ready":
            repository["commit"] = self.commit(scope["commit"])
            repository["recent_commits"] = self.history(scope["commit"], limit=8, visible_tips=False)["items"]
        checkout = {
            "mode": status.get("mode"),
            "branch": status.get("branch"),
            "state": "ready" if status.get("commit") is not None else "unborn",
        }
        return {
            "revision": scope,
            "repository": repository,
            "current": {
                "initialized": True,
                "project_home": str(self.project_home),
                "status": status,
                "config": bounded_json(redact(safe_config)),
                "checkout": checkout,
                "active_jobs": (active_jobs := sum(
                    1
                    for item in runtimes
                    if (item.get("execution") or {}).get("lifecycle")
                    in {"pending", "running", "cancel-requested", "cancel-ready"}
                )),
                "recent_runtimes": runtimes[:8],
            },
            # Retained for direct read-model callers covered by the original
            # dashboard contracts; API consumers use ``current.active_jobs``.
            "active_jobs": active_jobs,
        }

    def dml_api(self) -> dict[str, Any]:
        """Return DML namespace results with only transport-safe serialization.

        This is deliberately not a dashboard view model.  The browser owns
        labels, counts, duration formatting, and graph presentation.
        """
        if not self.initialized:
            return {
                "initialized": False,
                "project_home": str(self.project_home) if self.project_home is not None else None,
            }
        return {
            "initialized": True,
            "status": bounded_json(self.dml.status()),
            "config": bounded_json(redact(self.dml.config.show())),
            "runtime": {"list": bounded_json(self.dml.runtime.list())},
        }

    def dml_runtime(self, runtime_id: str) -> dict[str, Any]:
        index = _ref(runtime_id, "index")
        return {
            "describe": bounded_json(self.dml.runtime.describe(index)),
            "argv": _ref_text(self.dml.runtime.get_argv(index)),
        }

    def refs(self, revision: str = "HEAD", *, live: bool = True) -> dict[str, Any]:
        """Project bounded ref sources through public DML operations only."""
        scope = self.resolve_revision(revision)
        local = self._ref_source("branch"), self._ref_source("tag")
        remote = (self._ref_source("branch", remote=True), self._ref_source("tag", remote=True)) if live else ([], [])
        branches = self._group_refs("branch", local[0], remote[0])
        tags = self._group_refs("tag", local[1], remote[1])
        dependencies = self._dependency_refs(live=live)
        selected = scope.get("commit")
        selected_labels = []
        if selected is not None:
            for group in [*branches, *tags]:
                for source in ("local", "live"):
                    tip = group.get(source)
                    if isinstance(tip, Mapping) and tip.get("commit") == f"commit:{selected}":
                        selected_labels.append(f"{source}:{group['kind']}:{group['name']}")
        status = self.dml.status()
        return {
            "revision": scope,
            "checkout": {
                "mode": status.get("mode"),
                "branch": status.get("branch"),
                "state": "ready" if status.get("commit") is not None else "unborn",
            },
            **({"current_head": f"commit:{scope['current_head']}"} if "current_head" in scope else {}),
            "selected": {"commit": f"commit:{selected}", "labels": sorted(selected_labels)} if selected else {"labels": []},
            "branches": branches,
            "tags": tags,
            "sources": {
                "local": {"branch": {"truncated": False}, "tag": {"truncated": False}},
                "live": {"branch": {"truncated": False}, "tag": {"truncated": False}},
            },
            "dependencies": {"items": dependencies, "truncated": False},
        }

    def _ref_source(self, kind: str, *, remote: bool = False, dep: str | None = None) -> list[dict[str, Any]]:
        namespace = self.dml.branch if kind == "branch" else self.dml.tag
        return [
            {"name": str(item["name"]), **self._tip(_ref_text(item["commit"]))}
            for item in namespace.list(remote=remote, dep=dep)[:200]
        ]

    def _tip(self, commit: str) -> dict[str, Any]:
        try:
            self.dml.show(commit)
            inspectable = True
        except (DmlRepoError, ValueError, KeyError):
            inspectable = False
        return {"commit": commit, "inspectable": inspectable}

    def _group_refs(self, kind: str, local: list[dict[str, Any]], live: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for source, items in (("local", local), ("live", live)):
            for item in items:
                group = grouped.setdefault(item["name"], {"kind": kind, "name": item["name"]})
                group[source] = {key: value for key, value in item.items() if key != "name"}
        for name, group in grouped.items():
            if kind == "branch":
                upstream = self.dml.branch.get_upstream(name) if "local" in group else None
                if upstream is not None:
                    group["upstream"] = upstream["branch"]
                upstream_name = upstream["branch"] if upstream is not None else name
                live_tip = next((item for item in live if item["name"] == upstream_name), None)
                group["relation"] = self._branch_relation(group.get("local"), live_tip)
            else:
                commits = {tip["commit"] for source in ("local", "live") if isinstance((tip := group.get(source)), Mapping)}
                copies = sum(source in group for source in ("local", "live"))
                group["relation"] = "matching" if len(commits) == 1 and copies > 1 else "local-only" if "local" in group and len(commits) == 1 else "remote-only" if "local" not in group and commits else "conflicting"
        return [grouped[name] for name in sorted(grouped)]

    def _branch_relation(self, local: Any, tracking: Any) -> str:
        if not isinstance(local, Mapping) or not isinstance(tracking, Mapping):
            return "unknown"
        if not local.get("inspectable") or not tracking.get("inspectable"):
            return "unknown"
        if local["commit"] == tracking["commit"]:
            return "in-sync"
        return "unknown"

    def _dependency_refs(self, *, live: bool) -> list[dict[str, Any]]:
        names = sorted(self.dml.dep.list())
        dependencies = []
        for name in names[:50]:
            try:
                fetched_branches = self._ref_source("branch", dep=name)
                fetched_tags = self._ref_source("tag", dep=name)
                live_branches = self._ref_source("branch", remote=True, dep=name) if live else []
                live_tags = self._ref_source("tag", remote=True, dep=name) if live else []
                dependencies.append(
                    {
                        "name": name,
                        "root": redact({"root": self.dml.dep.list()[name]})["root"],
                        "branches": self._group_dependency_refs("branch", fetched_branches, live_branches),
                        "tags": self._group_dependency_refs("tag", fetched_tags, live_tags),
                        "sources": {
                            "fetched": {"branch": {"truncated": False}, "tag": {"truncated": False}},
                            "live": {"branch": {"truncated": False}, "tag": {"truncated": False}},
                        },
                    }
                )
            except Exception:
                dependencies.append({"name": name, "diagnostic": {"availability": "unavailable", "message": "Dependency refs are unavailable"}})
        return dependencies

    def _group_dependency_refs(
        self, kind: str, fetched: list[dict[str, Any]], live: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for source, items in (("fetched", fetched), ("live", live)):
            for item in items:
                group = grouped.setdefault(item["name"], {"kind": kind, "name": item["name"]})
                group[source] = {key: value for key, value in item.items() if key != "name"}
        for group in grouped.values():
            if kind == "branch":
                group["relation"] = self._branch_relation(group.get("fetched"), group.get("live"))
            else:
                commits = {
                    tip["commit"]
                    for source in ("fetched", "live")
                    if isinstance((tip := group.get(source)), Mapping)
                }
                copies = sum(source in group for source in ("fetched", "live"))
                group["relation"] = (
                    "matching"
                    if len(commits) == 1 and copies > 1
                    else "local-only"
                    if "fetched" in group and len(commits) == 1
                    else "remote-only"
                    if "fetched" not in group and commits
                    else "conflicting"
                )
        return [grouped[name] for name in sorted(grouped)]

    @staticmethod
    def _ref_items(payload: Mapping[str, Any], source: str) -> list[dict[str, Any]]:
        """Flatten the unified envelope for internal history and search reads."""
        if source in payload:  # Temporary tolerance for isolated legacy test doubles.
            return list(payload[source])
        items = []
        for group in [*payload.get("branches", []), *payload.get("tags", [])]:
            tip = group.get(source)
            if isinstance(tip, Mapping):
                items.append({"name": group["name"], "kind": group["kind"], "commit": tip["commit"]})
        return items

    def _local_refs_for_auxiliary_read(self) -> Mapping[str, Any]:
        try:
            return self.refs("HEAD", live=False)
        except TypeError:  # Isolated legacy read-model test doubles.
            return self.refs()  # type: ignore[call-arg]

    def history(
        self,
        revision: str = "HEAD",
        *,
        cursor: str | None = None,
        limit: int = 50,
        visible_tips: bool = True,
    ) -> dict[str, Any]:
        scope = self.resolve_revision(revision)
        if scope["state"] == "unborn":
            return {"items": [], "next_cursor": None, "revision": scope}
        limit = max(1, min(int(limit), 200))
        offset = _parse_cursor(cursor)
        labels: dict[str, list[dict[str, str]]] = {}
        if visible_tips:
            # The history visualisation is a bounded map of visible repository
            # topology, not merely the ancestry of the selected snapshot.
            roots: list[Ref] = []
            refs = self.refs(revision, live=False)
            for item in self._ref_items(refs, "local"):
                commit = Ref(item["commit"])
                roots.append(commit)
                labels.setdefault(_ref_text(commit), []).append({"name": str(item["name"]), "kind": str(item["kind"])})
            head_commit = self.dml.status().get("commit")
            if head_commit is not None:
                roots.insert(0, head_commit)
                head_branch = self.dml.status().get("branch")
                labels.setdefault(_ref_text(head_commit), []).append(
                    {"name": str(head_branch) if head_branch else "HEAD", "kind": "head"}
                )
            if not roots:
                roots = [Ref(f"commit:{scope['commit']}")]
        else:
            roots = [Ref(f"commit:{scope['commit']}")]
        to_walk = list(dict.fromkeys(roots))
        seen: set[Ref] = set()
        commits: list[Mapping[str, Any]] = []
        # Walk all visible tips once.  A hard cap proportional to the requested
        # page preserves bounded request cost while retaining graph parents.
        while to_walk and len(commits) < offset + limit + 1:
            current = to_walk.pop(0)
            if current in seen:
                continue
            seen.add(current)
            item = self.dml.show(current)
            commits.append(item)
            to_walk.extend(parent for parent in item["parents"] if parent not in seen)
        page = commits[offset : offset + limit]
        items = []
        for item in page:
            projected = bounded_json(item)
            projected["refs"] = sorted(
                { (label["kind"], label["name"]) for label in labels.get(f"commit:{item['id']}", []) }
            )
            projected["refs"] = [{"kind": kind, "name": name} for kind, name in projected["refs"]]
            projected["selected"] = item["id"] == scope["commit"]
            projected["is_current_head"] = item["id"] == scope.get("current_head")
            items.append(projected)
        return {
            "items": items,
            "next_cursor": _offset_cursor(offset + limit) if len(commits) > offset + limit else None,
            "revision": scope,
        }

    def commit(self, revision: str) -> dict[str, Any]:
        return bounded_json(self.dml.show(f"commit:{revision}" if revision != "HEAD" else revision))

    def commit_diff(self, revision: str, relative_to: str | None = None) -> dict[str, Any]:
        return bounded_json(self.dml.diff(revision, relative_to=relative_to))

    def dags(self, revision: str | None = None) -> dict[str, Any]:
        if revision is None:
            scope = None
            commit = self.dml.show("HEAD")
        else:
            scope = self.resolve_revision(revision)
            if scope["state"] == "unborn":
                return {"items": [], "revision": scope, "live_dags_eligible": False}
            commit = self.dml.show(f"commit:{scope['commit']}")
        items = []
        for name, dag_ref in sorted(commit["dags"].items()):
            if scope is not None:
                self._navigation(scope["commit"])["dags"].add(dag_ref)
            description = self.dml.dag.describe(dag_ref)
            items.append(
                {
                    "name": name,
                    "id": _ref_text(dag_ref),
                    "commit": _ref_text(commit["id"]),
                    "tags": list(description.get("tags", [])),
                    "node_count": len(description["nodes"]),
                    "status": "error" if description.get("error") is not None else "ready",
                }
            )
        if scope is None:
            return {"items": items}
        return {"items": items, "revision": scope, "live_dags_eligible": scope["is_current_head"]}

    def _navigation(self, revision: str) -> dict[str, set[Ref]]:
        return self._revision_navigation.setdefault(revision, {"dags": set(), "nodes": set()})

    def _remember_dag(self, revision: str, dag_ref: Ref, description: Mapping[str, Any]) -> None:
        """Record the lineage disclosed while navigating one revision."""
        navigation = self._navigation(revision)
        navigation["dags"].add(dag_ref)
        for node_ref in description["nodes"]:
            navigation["nodes"].add(node_ref)
            node = self.dml.dag.describe_node(node_ref)
            if node.get("type") == "FnNode" and isinstance(node.get("dag"), Ref):
                navigation["dags"].add(node["dag"])

    def _revision_contains_dag(self, revision: str, dag_ref: Ref) -> bool:
        scope = self.resolve_revision(revision)
        if scope["state"] != "ready":
            return False
        if dag_ref in self._navigation(scope["commit"])["dags"]:
            return True
        commit = self.dml.show(f"commit:{scope['commit']}")
        pending = list(commit["dags"].values())
        seen: set[Ref] = set()
        while pending and len(seen) < 1_000:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            description = self.dml.dag.describe(current)
            self._remember_dag(scope["commit"], current, description)
            if current == dag_ref:
                return True
            for node_ref in description["nodes"]:
                node = self.dml.dag.describe_node(node_ref)
                if node.get("type") == "FnNode" and isinstance(node.get("dag"), Ref):
                    pending.append(node["dag"])
        return False

    def _revision_contains_node(self, revision: str, node_ref: Ref) -> bool:
        scope = self.resolve_revision(revision)
        if scope["state"] != "ready":
            return False
        if node_ref in self._navigation(scope["commit"])["nodes"]:
            return True
        commit = self.dml.show(f"commit:{scope['commit']}")
        pending = list(commit["dags"].values())
        seen: set[Ref] = set()
        while pending and len(seen) < 1_000:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            description = self.dml.dag.describe(current)
            self._remember_dag(scope["commit"], current, description)
            if node_ref in description["nodes"]:
                return True
            for child_ref in description["nodes"]:
                node = self.dml.dag.describe_node(child_ref)
                if node.get("type") == "FnNode" and isinstance(node.get("dag"), Ref):
                    pending.append(node["dag"])
        return False

    def _current_live_contains_dag(self, scope: Mapping[str, Any] | None, dag_ref: Ref) -> bool:
        """Allow a partial DAG only in the current concrete HEAD scope."""
        if scope is None or not scope.get("is_current_head"):
            return False
        return any(runtime.get("dag") == dag_ref for runtime in self.dml.runtime.list())

    def _current_live_contains_node(self, scope: Mapping[str, Any] | None, node_ref: Ref) -> bool:
        """Allow nodes belonging to a current partial DAG, never a stale one."""
        if scope is None or not scope.get("is_current_head"):
            return False
        for runtime in self.dml.runtime.list():
            dag_ref = runtime.get("dag")
            if isinstance(dag_ref, Ref) and node_ref in self.dml.dag.describe(dag_ref)["nodes"]:
                return True
        return False

    def dag(self, dag_id: str, *, revision: str | None = None, project: str | None = None) -> dict[str, Any]:
        dag_ref = _ref(dag_id, "dag")
        scope = self.resolve_revision(revision) if revision is not None else None
        if revision is not None and not self._revision_contains_dag(revision, dag_ref) and not self._current_live_contains_dag(scope, dag_ref):
            raise RevisionError("resource-not-in-revision", "Resource is not available in this revision")
        desc = self.dml.dag.describe(dag_ref)
        if scope is not None and scope["state"] == "ready":
            self._remember_dag(scope["commit"], dag_ref, desc)
        names = {_ref_text(ref): name for name, ref in desc["names"].items()}
        nodes = []
        edges = []
        for node_ref in desc["nodes"]:
            node = self.dml.dag.describe_node(node_ref)
            item = bounded_json(node)
            item["name"] = names.get(_ref_text(node_ref))
            nodes.append(item)
            if node["type"] == "ImportNode":
                edges.append(
                    {
                        "source": _ref_text(node["node"]),
                        "target": _ref_text(node_ref),
                        "kind": "import",
                        "dag": _ref_text(node["dag"]),
                    }
                )
            elif node["type"] == "FnNode":
                item["context_dag"] = {
                    "ref": _ref_text(node["dag"]),
                    "href": self._scoped_href(f"/api/v1/dags/{_ref_text(node['dag'])}", project, revision),
                }
                edges.extend(
                    {"source": _ref_text(arg), "target": _ref_text(node_ref), "kind": "argument"}
                    for arg in node["argv"]
                )
        payload = {**bounded_json(desc), "nodes": nodes, "edges": edges}
        if scope is not None:
            payload["revision"] = scope
        function = self._function_context(dag_ref, description=desc, project=project, revision=revision)
        if function is not None:
            payload["function"] = function
        return payload

    def node(
        self,
        node_id: str,
        *,
        recursive: bool = False,
        revision: str | None = None,
        project: str | None = None,
    ) -> dict[str, Any]:
        node_ref = _ref(node_id, "node")
        scope = self.resolve_revision(revision) if revision is not None else None
        if revision is not None and not self._revision_contains_node(revision, node_ref) and not self._current_live_contains_node(scope, node_ref):
            raise RevisionError("resource-not-in-revision", "Resource is not available in this revision")
        description = self.dml.dag.describe_node(node_ref)
        value = self.dml.dag.get_node(node_ref, recursive=recursive)
        payload = {
            "description": bounded_json(description),
            "value": bounded_json(value, max_depth=6, max_items=100, max_string=16_384),
            "value_kind": "error" if isinstance(value, Error) else "runnable" if isinstance(value, Runnable) else "value",
            "value_type": type(value).__name__,
            "is_error": isinstance(value, Error),
        }
        if isinstance(value, Runnable):
            payload["value_runnable"] = self._runnable_inspection(
                value,
                script_href=self._scoped_href(
                    f"/api/v1/nodes/{_ref_text(node_ref)}/value/script", project, revision
                ),
            )
        if scope is not None:
            payload["revision"] = scope
        if description["type"] == "FnNode":
            context_ref = description["dag"]
            payload["context_dag"] = {
                "ref": _ref_text(context_ref),
                "href": self._scoped_href(f"/api/v1/dags/{_ref_text(context_ref)}", project, revision),
            }
            function = self._function_context(context_ref, project=project, revision=revision)
            if function is not None:
                payload["function"] = function
        return payload

    def _function_context(
        self,
        dag_ref: Ref,
        *,
        description: Mapping[str, Any] | None = None,
        project: str | None = None,
        revision: str | None = None,
    ) -> dict[str, Any] | None:
        """Project the persisted runnable owned by a function DAG."""
        desc = description or self.dml.dag.describe(dag_ref)
        argv_ref = desc.get("argv")
        if not isinstance(argv_ref, Ref):
            return None
        argv = self.dml.dag.get_node(argv_ref, recursive=True)
        applied = argv[0] if isinstance(argv, (list, tuple)) and argv else None
        runnable = (
            self._runnable_inspection(
                applied,
                script_href=self._scoped_href(
                    f"/api/v1/function-dags/{_ref_text(dag_ref)}/script", project, revision
                ),
                description=desc,
                project=project,
                revision=revision,
            )
            if isinstance(applied, Runnable)
            else {
                "state": "unavailable",
                "diagnostic": "The persisted context argv does not begin with a Runnable.",
            }
        )
        return {
            "dag": {"ref": _ref_text(dag_ref), "href": self._scoped_href(f"/api/v1/dags/{_ref_text(dag_ref)}", project, revision)},
            "argv": {"ref": _ref_text(argv_ref), "href": self._scoped_href(f"/api/v1/nodes/{_ref_text(argv_ref)}", project, revision)},
            "cache_key": desc.get("cache_key"),
            "runnable": runnable,
        }

    def _runnable_inspection(
        self,
        value: Runnable,
        *,
        script_href: str,
        description: Mapping[str, Any] | None = None,
        project: str | None = None,
        revision: str | None = None,
    ) -> dict[str, Any]:
        stack = project_runnable(value, max_depth=16)
        entrypoint = value
        truncated = False
        for _ in range(15):
            if not isinstance(entrypoint.sub, Runnable):
                break
            entrypoint = entrypoint.sub
        else:
            truncated = isinstance(entrypoint.sub, Runnable)
        projected_entrypoint = project_runnable(entrypoint, max_depth=1)
        details = projected_entrypoint.get("details")
        script_uri = _uri_text(details.get("script_uri")) if isinstance(details, Mapping) else None
        if projected_entrypoint.get("kind") != "script":
            script = {"state": "not-python-script", "message": "The innermost runnable is not a Python script executor."}
        elif not isinstance(script_uri, str) or not script_uri:
            script = {"state": "missing-script-uri", "message": "The Python script runnable has no script URI."}
        else:
            script = {"state": "available", "uri": script_uri, "href": script_href}
        raw_prepop = entrypoint.kwargs.get("prepop")
        names = description.get("names") if isinstance(description, Mapping) else None
        rows = []
        if isinstance(raw_prepop, Mapping):
            for name, item in list(raw_prepop.items())[:100]:
                node_ref = names.get(name) if isinstance(names, Mapping) else None
                node = None
                if isinstance(node_ref, Ref):
                    node = {
                        "ref": _ref_text(node_ref),
                        "href": self._scoped_href(f"/api/v1/nodes/{_ref_text(node_ref)}", project, revision),
                    }
                rows.append({"name": str(name), "type": type(item).__name__, "node": node})
        return {
            "state": "ready",
            "stack": stack,
            "entrypoint": projected_entrypoint,
            "script": script,
            "prepopulated": rows,
            "truncated": truncated or len(raw_prepop) > 100 if isinstance(raw_prepop, Mapping) else truncated,
        }

    def runtimes(self) -> dict[str, Any]:
        items = []
        runtimes = self.dml.runtime.list()
        if not runtimes:
            return {"items": items}
        remote_ready = True
        remote_diagnostic = None
        try:
            self._require_remote_descriptor()
        except Exception as exc:
            remote_ready = False
            remote_diagnostic = str(exc)
        for runtime in runtimes:
            item = bounded_json(runtime)
            try:
                if not remote_ready:
                    raise DmlRepoError(remote_diagnostic or "Remote execution state is unavailable")
                item["execution"] = bounded_json(self.dml.runtime.read_execution_record(runtime["id"]))
            except Exception as exc:
                item["execution"] = None
                item["execution_diagnostic"] = str(exc)
            items.append(item)
        return {"items": items}

    def status_live_indexes(
        self, project_id: str, project_name: str, *, head_ref: str | None = None
    ) -> list[dict[str, Any]]:
        """Project locally present indexes without requiring remote evidence."""
        items: list[dict[str, Any]] = []
        for runtime in self.dml.runtime.list():
            projected = bounded_json(runtime)
            index_ref = _ref_text(runtime["id"])
            root_record = None
            try:
                root_record = bounded_json(self.dml.runtime.read_execution_record(runtime["id"]))
            except Exception:
                pass
            group, reason = _live_state(projected, root_record)
            encoded_project = quote(project_id, safe="")
            encoded_index = quote(index_ref, safe="")
            project_href = self._status_project_href(encoded_project, head_ref)
            dag_ref = _ref_text(runtime["dag"])
            dag_href = (
                f"{project_href}/dags/{quote(dag_ref, safe='')}"
                if head_ref is not None
                else f"{project_href}?resource={quote(dag_ref, safe='')}&tab=summary"
            )
            item = {
                "project_id": project_id,
                "project_name": project_name,
                "index_ref": index_ref,
                "title": str(runtime.get("message") or index_ref),
                "group": group,
                "created_at": _rfc3339(runtime.get("created")),
                "links": {
                    "project": project_href,
                    "inspector": f"{project_href}?resource={encoded_index}&tab=summary",
                    "dag": dag_href,
                },
                "state": str(runtime.get("state") or "active"),
                "dag_ref": dag_ref,
            }
            if reason:
                item["reason"] = reason
            items.append(item)
        return items

    @staticmethod
    def _status_project_href(encoded_project: str, head_ref: str | None) -> str:
        """Return the only valid Home destination for current project state."""
        if head_ref is None:
            return f"/projects/{encoded_project}/unborn"
        return f"/projects/{encoded_project}/commits/{quote(_commit_id(head_ref), safe='')}"

    def status_checkout(self) -> dict[str, Any]:
        """Return the local checkout and sync facts used by one Status snapshot."""
        status = self.dml.status()
        head = status.get("commit")
        head_ref = _ref_text(head) if head is not None else None
        ahead = status.get("ahead")
        behind = status.get("behind")
        upstream = status.get("upstream")
        if upstream is None:
            sync_state = "unconfigured"
        elif ahead is None or behind is None:
            sync_state = "unknown"
        elif ahead == 0 and behind == 0:
            sync_state = "in-sync"
        elif ahead and behind:
            sync_state = "diverged"
        elif ahead:
            sync_state = "ahead"
        else:
            sync_state = "behind"
        return {
            "head_ref": head_ref,
            "checkout": {
                "mode": status.get("mode"),
                "branch": status.get("branch"),
                "state": "ready" if head_ref is not None else "unborn",
            },
            "sync": {
                "state": sync_state,
                **({"upstream": str(upstream)} if upstream is not None else {}),
                **({"ahead": ahead} if isinstance(ahead, int) else {}),
                **({"behind": behind} if isinstance(behind, int) else {}),
            },
        }

    def status_recent_commits(
        self,
        project_id: str,
        project_name: str,
        *,
        cutoff: datetime,
        scan_cap: int,
        head_ref: str | None = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Traverse every current-HEAD parent in deterministic breadth-first order."""
        head_commit = Ref(head_ref) if head_ref is not None else self.dml.status().get("commit")
        if head_commit is None:
            return [], False
        labels: dict[str, list[str]] = {}
        for ref in self._ref_items(self._local_refs_for_auxiliary_read(), "local"):
            labels.setdefault(str(ref["commit"]), []).append(str(ref["name"]))
        labels.setdefault(_ref_text(head_commit), []).append("HEAD")
        queue = [head_commit]
        seen: set[str] = set()
        included: list[dict[str, Any]] = []
        while queue and len(seen) < scan_cap:
            current = queue.pop(0)
            current_ref = _ref_text(current)
            if current_ref in seen:
                continue
            seen.add(current_ref)
            description = self.dml.show(current)
            queue.extend(description["parents"])
            timestamp = _datetime(description.get("created"))
            if timestamp < cutoff:
                continue
            dag_count = len(description["dags"])
            error_count = 0
            for dag_ref in description["dags"].values():
                if self.dml.dag.describe(dag_ref).get("error") is not None:
                    error_count += 1
            commit_ref = _ref_text(description["id"])
            if not commit_ref.startswith("commit:"):
                commit_ref = f"commit:{commit_ref}"
            encoded_project = quote(project_id, safe="")
            project_href = f"/projects/{encoded_project}/commits/{quote(_commit_id(commit_ref), safe='')}"
            inspector_href = f"{project_href}?resource={quote(commit_ref, safe='')}&tab=summary"
            included.append(
                {
                    "project_id": project_id,
                    "project_name": project_name,
                    "commit_ref": commit_ref,
                    "message": str(description.get("message") or ""),
                    "author": str(description.get("author") or ""),
                    "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
                    "refs": sorted(set(labels.get(commit_ref, []))),
                    "dag_count": dag_count,
                    "error_dag_count": error_count,
                    "links": {
                        "project": project_href,
                        "inspector": inspector_href,
                        "history": inspector_href,
                    },
                }
            )
        included.sort(key=lambda item: item["commit_ref"])
        included.sort(key=lambda item: item["timestamp"], reverse=True)
        return included, any(_ref_text(candidate) not in seen for candidate in queue)

    def live_index(self, index_id: str) -> dict[str, Any]:
        """Return bounded partial-DAG and reachable evidence for one live index."""
        index_ref = Ref(index_id) if index_id.startswith("frozenindex:") else _ref(index_id, "index")
        try:
            runtime = self.dml.runtime.describe(index_ref)
        except DmlRepoError:
            frozen_ref = Ref(f"frozenindex:{index_ref.id()}")
            runtime = self.dml.runtime.describe(frozen_ref)
            index_ref = frozen_ref
        dag_ref = runtime["dag"]
        diagnostics: list[dict[str, Any]] = []
        root_record = None
        try:
            root_record = bounded_json(self.dml.runtime.read_execution_record(index_ref))
        except Exception:
            diagnostics.append(
                {
                    "availability": "unconfigured",
                    "code": "execution-evidence-unavailable",
                    "message": "Remote execution evidence is unavailable",
                    "retryable": False,
                }
            )
        group, reason = _live_state(bounded_json(runtime), root_record)
        lineage: list[dict[str, Any]] = []
        try:
            graph = self.execution_graph(index_ref.id())
            lineage.extend(_timeline_records(graph, runtime.get("created")))
        except Exception:
            if root_record is not None:
                lineage.append(_timeline_record(index_ref.id(), root_record, runtime.get("created")))
        return bounded_json(
            {
                "index_ref": _ref_text(index_ref),
                "title": runtime.get("message") or _ref_text(index_ref),
                "state": runtime.get("state"),
                "group": group,
                "created_at": _rfc3339(runtime.get("created")),
                **({"reason": reason} if reason else {}),
                "dag": {
                    "ref": _ref_text(dag_ref),
                    "href": f"/api/v1/dags/{_ref_text(dag_ref)}",
                    "partial": True,
                },
                "execution": root_record,
                "lineage": lineage,
                "evidence": {
                    "logs": {
                        "stdout": {"href": f"/api/v1/executions/{index_ref.id()}/logs/stdout"},
                        "stderr": {"href": f"/api/v1/executions/{index_ref.id()}/logs/stderr"},
                    },
                    "runnable": {"href": f"/api/v1/executions/{index_ref.id()}"},
                },
                "identifiers": {"index": _ref_text(index_ref), "dag": _ref_text(dag_ref)},
                "diagnostics": diagnostics,
            }
        )

    def execution(self, execution_id: str) -> dict[str, Any]:
        self._require_remote_descriptor()
        index = _ref(execution_id, "index")
        record = self.dml.runtime.read_execution_record(index)
        record_execution_id = str(record.get("execution_id") or str(index).removeprefix("index:"))
        state = self._execution_state(record_execution_id)
        payload: dict[str, Any] = {
            "record": bounded_json(record),
            "launch_state": bounded_json(redact(state)),
        }
        try:
            argv_ref = self.dml.runtime.get_argv(index)
            argv = self.dml.dag.get_node(argv_ref, recursive=True)
            runnable = argv[0] if isinstance(argv, list) and argv else None
            if isinstance(runnable, Runnable):
                raw = {
                    "target": runnable.target,
                    "adapter": runnable.adapter,
                    "kwargs": runnable.kwargs,
                    "sub": runnable.sub,
                    "state": state,
                }
                payload["runnable"] = project_runnable(raw)
        except Exception:
            payload["runnable"] = None
        return payload

    def fndag(self, execution_id: str) -> dict[str, Any]:
        """Describe one function-DAG using the same objects as the Python API.

        The response intentionally retains DML object boundaries (runtime,
        argv node, result DAG, execution record, launch state).  Presentation
        conveniences such as labels and durations belong in the browser.
        """
        self._require_remote_descriptor()
        index = _ref(execution_id, "index")
        record = self.dml.runtime.read_execution_record(index)
        record_execution_id = str(record.get("execution_id") or str(index).removeprefix("index:"))
        runtime = None
        try:
            runtime = self.dml.runtime.describe(index)
            argv_ref = self.dml.runtime.get_argv(index)
            dag_ref = runtime.get("dag")
        except Exception:
            # Function executions are not themselves local runtime indexes once
            # complete.  Their completed DAG remains available via the public
            # cache API keyed by the persisted execution record.
            cache_key = record.get("cache_key")
            if not isinstance(cache_key, str) or not cache_key:
                raise DmlRepoError("Only function DAG executions with a cache key can be inspected") from None
            dag_ref = self.dml.cache.get(cache_key)
            if not isinstance(dag_ref, Ref):
                raise DmlRepoError(f"No completed function DAG found for cache key: {cache_key}") from None
            argv_ref = self.dml.dag.describe(dag_ref)["argv"]
        if not isinstance(argv_ref, Ref):
            raise DmlRepoError("Execution arguments are unavailable")
        argv_value = self.dml.dag.get_node(argv_ref, recursive=True)
        created_at = record.get("created_at")
        updated_at = record.get("updated_at")
        timing: dict[str, Any] = {"started_at": created_at, "ended_at": updated_at}
        if isinstance(created_at, (int, float)) and isinstance(updated_at, (int, float)):
            timing["duration_seconds"] = max(0, updated_at - created_at)
        inputs = []
        if isinstance(argv_value, list):
            for value in argv_value:
                if isinstance(value, Ref):
                    inputs.append({"ref": _ref_text(value), "href": f"/api/v1/nodes/{_ref_text(value)}"})
                else:
                    inputs.append({"value": bounded_json(value, max_depth=4, max_items=30, max_string=2_048)})
        output = None
        if isinstance(dag_ref, Ref):
            output = {"ref": _ref_text(dag_ref), "href": f"/api/v1/dags/{_ref_text(dag_ref)}"}
        execution = self.execution(record_execution_id)
        runnable = execution.get("runnable")
        root_runnable = argv_value[0] if isinstance(argv_value, list) and argv_value else None
        if runnable is None and isinstance(root_runnable, (Runnable, Mapping)):
            if isinstance(root_runnable, Runnable):
                raw_runnable = {
                    "target": root_runnable.target,
                    "adapter": root_runnable.adapter,
                    "kwargs": root_runnable.kwargs,
                    "sub": root_runnable.sub,
                }
            else:
                # Recursive node reads return a safe runnable representation
                # whose executor fields live under ``details``.
                raw_runnable = {
                    "target": root_runnable.get("target"),
                    "adapter": root_runnable.get("adapter"),
                    "kwargs": root_runnable.get("details"),
                    "sub": root_runnable.get("sub"),
                }
            runnable = project_runnable(
                {
                    **raw_runnable,
                    "state": execution.get("launch_state", {}),
                }
            )
        return {
            "execution": bounded_json(record),
            "runtime": bounded_json(runtime) if runtime is not None else None,
            "cache_key": record.get("cache_key"),
            "argv": {"ref": _ref_text(argv_ref), "href": f"/api/v1/nodes/{_ref_text(argv_ref)}", "inputs": inputs},
            "output": output,
            "timing": timing,
            "launch_state": execution.get("launch_state", {}),
            "runnable": runnable,
            "script": {"href": f"/api/v1/executions/{record_execution_id}/script"},
            "logs": {
                "stdout": {"href": f"/api/v1/executions/{record_execution_id}/logs/stdout"},
                "stderr": {"href": f"/api/v1/executions/{record_execution_id}/logs/stderr"},
            },
        }

    def execution_graph(self, *roots: str) -> dict[str, Any]:
        self._require_remote_descriptor()
        refs = tuple(Ref(root) if root.startswith(("index:", "frozenindex:")) else _ref(root, "index") for root in roots)
        return bounded_json(self.dml.runtime.describe_graph(*refs))

    def search(
        self, query: str, *, limit: int = 25, project: str | None = None, revision: str = "HEAD"
    ) -> dict[str, Any]:
        scope = self.resolve_revision(revision)
        needle = query.strip().lower()
        if not needle:
            return {"items": [], "revision": scope}
        limit = max(1, min(int(limit), 100))
        matches: list[dict[str, Any]] = []
        refs = self.refs(revision, live=False)
        for item in self._ref_items(refs, "local"):
            label = str(item["name"])
            if needle in label.lower() or needle in item["commit"].lower():
                matches.append({"kind": "ref", "label": label, "target": item["commit"], "preview": item})
        if len(matches) < limit:
            for item in self.history(revision, limit=100)["items"]:
                text = f"{item.get('id', '')} {item.get('message', '')} {item.get('author', '')}"
                if needle in text.lower():
                    matches.append(
                        {
                            "kind": "commit",
                            "label": item.get("message") or str(item["id"])[:12],
                            "target": f"commit:{item['id']}",
                            "preview": item,
                        }
                    )
                for name, dag_ref in item.get("dags", {}).items():
                    if needle in name.lower() or needle in _ref_text(dag_ref).lower():
                        matches.append(
                            {
                                "kind": "dag",
                                "label": name,
                                "target": _ref_text(dag_ref),
                                "preview": {"commit": item["id"]},
                            }
                        )
        items = []
        for item in matches[:limit]:
            preview = item.get("preview")
            commit = _commit_id(preview.get("commit", item["target"]) if isinstance(preview, dict) else item["target"])
            scoped = {**item, "commit": commit}
            if project is not None:
                scoped["project_id"] = project
                scoped["href"] = f"/projects/{quote(project, safe='')}/commits/{quote(commit, safe='')}"
            items.append(scoped)
        return {"items": items, "revision": scope}

    def _execution_state(self, execution_id: str) -> dict[str, Any]:
        record = self.dml.runtime.read_execution_record(_ref(execution_id, "index"))
        return record["driver"]["adapter_state"] or {}

    def _require_remote_descriptor(self) -> None:
        """Read-only preflight before constructing mutation-capable Remote."""
        if self._remote_descriptor_verified:
            return
        config = self.dml.config.show()["remote"]
        remote_root = config.get("root")
        if not isinstance(remote_root, str) or not remote_root:
            raise DmlRepoError("remote.root is required for execution state")
        parsed = urlsplit(remote_root)
        prefix = parsed.path.strip("/")
        key = "/".join(part for part in (prefix, "dml", "dml.json") if part)
        if self._s3_client_factory is not None:
            client = self._s3_client_factory()
        else:
            from daggerml.util import get_client

            client = get_client("s3")
        try:
            payload = json.loads(client.get_object(Bucket=parsed.netloc, Key=key)["Body"].read())
        except Exception as exc:
            raise DmlRepoError("Remote descriptor is unavailable; dashboard reads will not create it") from exc
        if payload != {
            "schema": 0,
            "hash": "sha256",
            "layout": "one-project-cas+refs+split-execution",
            "refs_prefix": "refs",
            "io_prefix": "io",
            "cas_prefix": "cas/sha256",
            "execution_prefix": "../exec",
        }:
            raise DmlRepoError("Remote descriptor is invalid")
        self._remote_descriptor_verified = True

    def remotes(self, *, live: bool = True) -> dict[str, Any]:
        result = self.refs()
        result["live"] = {"projects": [], "refs": [], "diagnostic": None}
        config = self.dml.config.show()["remote"]
        result["configured"] = bounded_json(redact(config))
        if not live or not config.get("root"):
            return result
        for kind, items in (("branch", self._ref_source("branch", remote=True)), ("tag", self._ref_source("tag", remote=True))):
            result["live"]["refs"].extend({"kind": kind, "name": item["name"]} for item in items)
        return result

    def script(self, execution_id: str, *, max_bytes: int = 128 * 1024) -> dict[str, Any]:
        """Read bounded source from a trusted script runnable resource."""
        inspection = self.fndag(execution_id)
        return self._read_script_resource(inspection.get("runnable"), max_bytes=max_bytes)

    def function_dag_script(
        self, dag_id: str, *, revision: str | None = None, max_bytes: int = 128 * 1024
    ) -> dict[str, Any]:
        """Read bounded source from a runnable persisted in a function DAG."""
        dag_ref = _ref(dag_id, "dag")
        if revision is not None:
            scope = self.resolve_revision(revision)
            if not self._revision_contains_dag(revision, dag_ref) and not self._current_live_contains_dag(scope, dag_ref):
                raise RevisionError("resource-not-in-revision", "Resource is not available in this revision")
        description = self.dml.dag.describe(dag_ref)
        argv_ref = description.get("argv")
        argv = self.dml.dag.get_node(argv_ref, recursive=True) if isinstance(argv_ref, Ref) else None
        applied = argv[0] if isinstance(argv, (list, tuple)) and argv else None
        if not isinstance(applied, Runnable):
            raise ScriptReadError(
                "applied-runnable-unavailable",
                "The persisted context argv does not begin with a Runnable.",
                status_code=404,
            )
        return self._read_script_runnable(applied, max_bytes=max_bytes)

    def node_value_script(
        self,
        node_id: str,
        *,
        revision: str,
        max_bytes: int = 128 * 1024,
    ) -> dict[str, Any]:
        """Read source only from the selected, revision-scoped node value."""
        node_ref = _ref(node_id, "node")
        scope = self.resolve_revision(revision)
        if not self._revision_contains_node(revision, node_ref) and not self._current_live_contains_node(scope, node_ref):
            raise RevisionError("resource-not-in-revision", "Resource is not available in this revision")
        value = self.dml.dag.get_node(node_ref, recursive=False)
        if not isinstance(value, Runnable):
            raise ScriptReadError(
                "node-value-not-runnable", "The selected node value is not a Runnable.", status_code=404
            )
        return self._read_script_runnable(value, max_bytes=max_bytes)

    def function_dag_logs(
        self,
        dag_id: str,
        stream: str,
        *,
        cursor: str | None = None,
        limit: int = 64 * 1024,
    ) -> dict[str, Any]:
        """Read durable logs using the cache identity persisted by a function DAG."""
        function = self._function_context(_ref(dag_id, "dag"))
        cache_key = function.get("cache_key") if function is not None else None
        if not isinstance(cache_key, str) or not cache_key:
            raise FileNotFoundError("This function DAG has no persisted log identity")
        return self._read_cloudwatch_log(cache_key, stream, cursor=cursor, limit=limit)

    def _read_script_resource(self, resource: Any, *, max_bytes: int) -> dict[str, Any]:
        while isinstance(resource, dict) and resource.get("kind") != "script":
            resource = resource.get("sub")
        details = resource.get("details") if isinstance(resource, dict) else None
        script_uri = details.get("script_uri") if isinstance(details, dict) else None
        if not isinstance(script_uri, str):
            raise ScriptReadError(
                "script-uri-unavailable", "This execution has no script resource.", status_code=404
            )
        return self._read_script_uri(script_uri, max_bytes=max_bytes)

    def _read_script_runnable(self, runnable: Runnable, *, max_bytes: int) -> dict[str, Any]:
        entrypoint = runnable.innermost()
        kind = project_runnable(entrypoint, max_depth=1).get("kind")
        if kind != "script":
            raise ScriptReadError(
                "not-python-script",
                "The innermost runnable is not a Python script executor.",
                status_code=404,
            )
        script_uri = _uri_text(entrypoint.kwargs.get("script_uri"))
        if not script_uri:
            raise ScriptReadError(
                "script-uri-unavailable",
                "The Python script runnable has no script URI.",
                status_code=404,
            )
        return self._read_script_uri(script_uri, max_bytes=max_bytes)

    def _read_script_uri(self, script_uri: str, *, max_bytes: int) -> dict[str, Any]:
        configured_root = self.dml.config.show()["remote"].get("root")
        if not isinstance(configured_root, str):
            raise ScriptReadError(
                "remote-unconfigured",
                "A configured remote root is required to read script resources.",
                status_code=403,
            )
        script = urlsplit(script_uri)
        root = urlsplit(configured_root)
        root_prefix = root.path.strip("/")
        key = script.path.strip("/")
        if (
            script.scheme != "s3"
            or script.netloc != root.netloc
            or (root_prefix and key != root_prefix and not key.startswith(f"{root_prefix}/"))
        ):
            raise ScriptReadError(
                "script-outside-remote-root",
                "Script resource is outside the configured remote root.",
                status_code=403,
            )
        self._require_remote_descriptor()
        max_bytes = max(1, min(int(max_bytes), 128 * 1024))
        from daggerml.util import get_client

        try:
            response = get_client("s3").get_object(
                Bucket=script.netloc,
                Key=key,
                Range=f"bytes=0-{max_bytes}",
            )
        except Exception as exc:
            raise ScriptReadError(
                "script-object-unavailable", "The script object is unavailable.", status_code=404
            ) from exc
        data = response["Body"].read(max_bytes + 1)
        return {
            "uri": script_uri,
            "source": data[:max_bytes].decode("utf-8", errors="replace"),
            "truncated": len(data) > max_bytes,
        }

    def logs(
        self,
        execution_id: str,
        stream: str,
        *,
        cursor: str | None = None,
        limit: int = 64 * 1024,
    ) -> dict[str, Any]:
        record = self.dml.runtime.read_execution_record(_ref(execution_id, "index"))
        cache_key = record.get("cache_key")
        if not cache_key:
            raise FileNotFoundError("No logs are available for this execution")
        return self._read_cloudwatch_log(cache_key, stream, cursor=cursor, limit=limit)

    def _read_cloudwatch_log(
        self,
        cache_key: str,
        stream: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any]:
        """Read the canonical durable stream for one trusted cache identity."""
        if self._cloudwatch_client_factory is None:
            from daggerml.util import get_client

            client = get_client("logs")
        else:
            client = self._cloudwatch_client_factory()
        return read_cloudwatch_log(client, cache_key, stream, cursor=cursor, limit=limit)
