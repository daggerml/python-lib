"""Cross-project Status projections and stable cursor snapshots."""

from __future__ import annotations

import base64
import json
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Literal, TypedDict, cast

from daggerml.dashboard.read_model import DashboardReadModel
from daggerml.dashboard.serialization import bounded_json

Availability = Literal["complete", "partial", "unavailable", "unauthorized", "unconfigured"]
LiveStateGroup = Literal["needs-attention", "in-progress", "canceling", "canceled"]


class EvidenceLinks(TypedDict, total=False):
    project: str
    inspector: str
    dag: str
    history: str


class StatusProject(TypedDict):
    id: str
    name: str
    path: str
    availability: Availability
    live_index_count: int
    recent_commit_count: int
    commit_truncated: bool
    local_available: bool
    path_context: dict[str, str]
    checkout: dict[str, Any]
    sync: dict[str, Any]
    last_activity: dict[str, Any]


class StatusLiveIndex(TypedDict, total=False):
    project_id: str
    project_name: str
    index_ref: str
    title: str
    group: LiveStateGroup
    created_at: str
    reason: str
    links: EvidenceLinks
    state: str
    dag_ref: str


class StatusCommit(TypedDict):
    project_id: str
    project_name: str
    commit_ref: str
    message: str
    author: str
    timestamp: str
    refs: list[str]
    dag_count: int
    error_dag_count: int
    links: EvidenceLinks


class StatusDiagnostic(TypedDict):
    project_id: str
    availability: Availability
    code: str
    message: str
    retryable: bool


@dataclass(frozen=True)
class _Snapshot:
    identifier: str
    generated_at: str
    created_monotonic: float
    projects: tuple[StatusProject, ...]
    live_indexes: tuple[StatusLiveIndex, ...]
    recent_commits: tuple[StatusCommit, ...]
    diagnostics: tuple[StatusDiagnostic, ...]
    truncated: bool


class StatusCursorError(ValueError):
    """Stable Status cursor failure with a machine-readable code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class DashboardStatus:
    """Build and page one bounded, failure-isolated cross-project snapshot."""

    retention_days = 365

    def __init__(
        self,
        projects: Any,
        model_factory: Callable[[str], DashboardReadModel],
        *,
        snapshot_ttl: float = 300.0,
        commit_scan_cap: int = 1_000,
        wall_clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.projects = projects
        self.model_factory = model_factory
        self.snapshot_ttl = snapshot_ttl
        self.commit_scan_cap = commit_scan_cap
        self.wall_clock = wall_clock or (lambda: datetime.now(timezone.utc))
        self.monotonic = monotonic
        self._snapshots: dict[str, _Snapshot] = {}

    def read(
        self,
        *,
        project_cursor: str | None = None,
        live_cursor: str | None = None,
        commit_cursor: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 200))
        cursors = {
            "projects": project_cursor,
            "live_indexes": live_cursor,
            "recent_commits": commit_cursor,
        }
        supplied = {name: value for name, value in cursors.items() if value is not None}
        if supplied:
            decoded = {name: self._decode_cursor(value, expected_collection=name) for name, value in supplied.items()}
            snapshot_ids = {value[0] for value in decoded.values()}
            if len(snapshot_ids) != 1:
                raise StatusCursorError("status-cursor-mismatch", "Status cursors must come from one snapshot")
            snapshot = self._get_snapshot(snapshot_ids.pop())
        else:
            snapshot = self._create_snapshot()
            decoded = {}

        initial = not supplied
        payload: dict[str, Any] = {
            "generated_at": snapshot.generated_at,
            "retention_days": self.retention_days,
            "diagnostics": list(snapshot.diagnostics),
            "truncated": snapshot.truncated,
        }
        for name in ("projects", "live_indexes", "recent_commits"):
            values = getattr(snapshot, name)
            if not initial and name not in decoded:
                payload[name] = {"items": [], "next_cursor": None}
                continue
            offset = decoded.get(name, (snapshot.identifier, 0))[1]
            page = list(values[offset : offset + limit])
            next_cursor = None
            if offset + limit < len(values):
                next_cursor = self._encode_cursor(snapshot.identifier, name, offset + limit)
            payload[name] = {"items": page, "next_cursor": next_cursor}
        return bounded_json(payload)

    def project_summaries(self) -> dict[str, StatusProject]:
        snapshot = self._create_snapshot()
        return {item["id"]: item for item in snapshot.projects}

    def _create_snapshot(self) -> _Snapshot:
        self._prune()
        now = self.wall_clock().astimezone(timezone.utc)
        projects: list[StatusProject] = []
        live_indexes: list[StatusLiveIndex] = []
        recent_commits: list[StatusCommit] = []
        diagnostics: list[StatusDiagnostic] = []
        truncated = False
        for registered in self.projects.list()["items"]:
            project_id = str(registered["id"])
            project_name = str(registered.get("name") or project_id)
            path = str(registered["path"])
            availability: Availability = "complete"
            project_live: list[StatusLiveIndex] = []
            project_commits: list[StatusCommit] = []
            commit_truncated = False
            checkout: dict[str, Any] = {"state": "unknown"}
            sync: dict[str, Any] = {"state": "unknown"}
            head_ref: str | None = None
            try:
                model = self.model_factory(project_id)
                if not model.initialized:
                    raise FileNotFoundError("Project repository is not initialized")
                checkout_info = self._checkout(model)
                checkout = checkout_info["checkout"]
                sync = checkout_info["sync"]
                head_ref = checkout_info["head_ref"]
                project_live = cast(
                    list[StatusLiveIndex],
                    model.status_live_indexes(project_id, project_name, head_ref=head_ref),
                )
                recent_kwargs: dict[str, Any] = {
                    "cutoff": now - timedelta(days=self.retention_days),
                    "scan_cap": self.commit_scan_cap,
                }
                if hasattr(model, "status_checkout"):
                    recent_kwargs["head_ref"] = head_ref
                recent, commit_truncated = model.status_recent_commits(project_id, project_name, **recent_kwargs)
                project_commits = cast(list[StatusCommit], recent)
            except PermissionError:
                availability = "unauthorized"
                checkout = {"state": "unavailable"}
                sync = {"state": "unknown"}
                diagnostics.append(
                    self._diagnostic(
                        project_id,
                        availability,
                        "project-unauthorized",
                        "Project state is not authorized",
                    )
                )
            except FileNotFoundError:
                availability = "unavailable"
                checkout = {"state": "unavailable"}
                sync = {"state": "unknown"}
                diagnostics.append(
                    self._diagnostic(project_id, availability, "project-unavailable", "Project state is unavailable")
                )
            except Exception:
                availability = "unavailable"
                checkout = {"state": "unavailable"}
                sync = {"state": "unknown"}
                diagnostics.append(
                    self._diagnostic(project_id, availability, "project-read-failed", "Project state could not be read")
                )
            live_indexes.extend(project_live)
            recent_commits.extend(project_commits)
            truncated = truncated or commit_truncated
            projects.append(
                cast(
                    StatusProject,
                    {
                        "id": project_id,
                        "name": project_name,
                        "path": path,
                        "availability": availability,
                        "live_index_count": len(project_live),
                        "recent_commit_count": len(project_commits),
                        "commit_truncated": commit_truncated,
                        "local_available": availability == "complete",
                        "path_context": self._path_context(path),
                        "checkout": checkout,
                        "sync": sync,
                        "last_activity": self._last_activity(
                            project_commits,
                            project_live,
                            commit_truncated=commit_truncated,
                            available=availability == "complete",
                        ),
                        **({"current_head": head_ref.removeprefix("commit:")} if head_ref is not None else {}),
                    },
                )
            )
        live_indexes.sort(key=lambda item: (item.get("created_at", ""), item.get("index_ref", "")), reverse=True)
        recent_commits.sort(key=lambda item: item["commit_ref"])
        recent_commits.sort(key=lambda item: item["timestamp"], reverse=True)
        identifier = secrets.token_urlsafe(18)
        snapshot = _Snapshot(
            identifier=identifier,
            generated_at=now.isoformat().replace("+00:00", "Z"),
            created_monotonic=self.monotonic(),
            projects=tuple(projects),
            live_indexes=tuple(live_indexes),
            recent_commits=tuple(recent_commits),
            diagnostics=tuple(diagnostics),
            truncated=truncated,
        )
        self._snapshots[identifier] = snapshot
        return snapshot

    @staticmethod
    def _checkout(model: DashboardReadModel) -> dict[str, Any]:
        if hasattr(model, "status_checkout"):
            return model.status_checkout()
        # Compatibility for lightweight read-model doubles. Production models
        # always provide the single-read checkout projection above.
        return {"head_ref": None, "checkout": {"state": "unknown"}, "sync": {"state": "unknown"}}

    @staticmethod
    def _path_context(path: str) -> dict[str, str]:
        normalized = path.rstrip("/") or path
        parent, _, leaf = normalized.rpartition("/")
        return {"parent": parent or "/", "leaf": leaf or normalized}

    @staticmethod
    def _last_activity(
        commits: list[StatusCommit],
        live_indexes: list[StatusLiveIndex],
        *,
        commit_truncated: bool,
        available: bool,
    ) -> dict[str, Any]:
        candidates = [(item["timestamp"], "commit") for item in commits]
        candidates.extend(
            (str(created_at), "live-index")
            for item in live_indexes
            if (created_at := item.get("created_at"))
        )
        if not candidates:
            return {"state": "unknown" if available else "unavailable", "truncated": commit_truncated}
        timestamp, source = max(candidates, key=lambda item: item[0])
        return {"state": "known", "timestamp": timestamp, "source": source, "truncated": commit_truncated}

    @staticmethod
    def _diagnostic(project_id: str, availability: Availability, code: str, message: str) -> StatusDiagnostic:
        return {
            "project_id": project_id,
            "availability": availability,
            "code": code,
            "message": message,
            "retryable": True,
        }

    def _get_snapshot(self, identifier: str) -> _Snapshot:
        self._prune()
        snapshot = self._snapshots.get(identifier)
        if snapshot is None:
            raise StatusCursorError(
                "status-cursor-expired",
                "Status cursor expired; restart traversal without a cursor",
            )
        return snapshot

    def _prune(self) -> None:
        now = self.monotonic()
        self._snapshots = {
            key: snapshot
            for key, snapshot in self._snapshots.items()
            if now - snapshot.created_monotonic <= self.snapshot_ttl
        }

    @staticmethod
    def _encode_cursor(snapshot: str, collection: str, offset: int) -> str:
        raw = json.dumps({"s": snapshot, "c": collection, "o": offset}, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @staticmethod
    def _decode_cursor(cursor: str, *, expected_collection: str) -> tuple[str, int]:
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded).decode())
            snapshot = payload["s"]
            collection = payload["c"]
            offset = payload["o"]
        except (KeyError, TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise StatusCursorError("status-cursor-invalid", "Invalid Status cursor") from exc
        if (
            collection != expected_collection
            or not isinstance(snapshot, str)
            or not isinstance(offset, int)
            or offset < 0
        ):
            raise StatusCursorError("status-cursor-invalid", "Invalid Status cursor")
        return snapshot, offset
__all__ = [
    "Availability",
    "DashboardStatus",
    "EvidenceLinks",
    "LiveStateGroup",
    "StatusCommit",
    "StatusCursorError",
    "StatusDiagnostic",
    "StatusLiveIndex",
    "StatusProject",
]
