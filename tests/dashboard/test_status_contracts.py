from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from daggerml._core.db import Ref
from daggerml.dashboard.read_model import (
    DashboardReadModel,
    _live_state,
    _timeline_record,
    _timeline_records,
)
from daggerml.dashboard.status import DashboardStatus, StatusCursorError


class _Projects:
    def __init__(self, items):
        self.items = items

    def list(self):
        return {"items": self.items, "default_project_id": self.items[0]["id"] if self.items else None}


class _StatusModel:
    initialized = True

    def __init__(self, project_id):
        self.project_id = project_id

    def status_checkout(self):
        return {
            "head_ref": f"commit:{self.project_id}",
            "checkout": {"state": "ready", "mode": "attached", "branch": "main"},
            "sync": {"state": "in-sync", "upstream": "main", "ahead": 0, "behind": 0},
        }

    def status_live_indexes(self, project_id, project_name, *, head_ref=None):
        del head_ref
        return [
            {
                "project_id": project_id,
                "project_name": project_name,
                "index_ref": f"index:{project_id}",
                "title": f"Live {project_name}",
                "group": "in-progress",
                "created_at": "2026-08-08T12:00:00Z",
                    "links": {"project": f"/projects/{project_id}/commits/{project_id}"},
            }
        ]

    def status_recent_commits(self, project_id, project_name, *, cutoff, scan_cap, head_ref=None):
        del cutoff, scan_cap, head_ref
        return (
            [
                {
                    "project_id": project_id,
                    "project_name": project_name,
                    "commit_ref": f"commit:{project_id}",
                    "message": "Complete",
                    "author": "researcher",
                    "timestamp": "2026-08-08T13:00:00Z",
                    "refs": ["HEAD"],
                    "dag_count": 2,
                    "error_dag_count": 1,
                    "links": {"history": f"/projects/{project_id}/commits/{project_id}"},
                }
            ],
            False,
        )


def _registered(count=3):
    return [
        {"id": f"p{number}", "name": f"Project {number}", "path": f"/registered/p{number}"}
        for number in range(count)
    ]


def test_dash_status_001__initial_and_continuation_pages_share_one_stable_snapshot():
    projects = _Projects(_registered())
    status = DashboardStatus(
        projects,
        lambda project_id: _StatusModel(project_id),
        wall_clock=lambda: datetime(2026, 8, 8, 14, tzinfo=timezone.utc),
    )

    first = status.read(limit=1)
    projects.items[1]["name"] = "Changed after snapshot"
    continued = status.read(project_cursor=first["projects"]["next_cursor"], limit=1)

    assert first["retention_days"] == 365
    assert first["projects"]["items"][0]["name"] == "Project 0"
    assert continued["projects"]["items"][0]["name"] == "Project 1"
    assert continued["generated_at"] == first["generated_at"]
    assert continued["live_indexes"] == {"items": [], "next_cursor": None}
    assert continued["recent_commits"] == {"items": [], "next_cursor": None}


def test_dash_status_001__status_uses_the_rolling_one_year_cutoff():
    cutoffs = []

    class CapturingStatusModel(_StatusModel):
        def status_recent_commits(self, project_id, project_name, *, cutoff, scan_cap, head_ref=None):
            cutoffs.append(cutoff)
            return super().status_recent_commits(
                project_id, project_name, cutoff=cutoff, scan_cap=scan_cap, head_ref=head_ref
            )

    status = DashboardStatus(
        _Projects(_registered(1)),
        lambda project_id: CapturingStatusModel(project_id),
        wall_clock=lambda: datetime(2026, 8, 8, 14, tzinfo=timezone.utc),
    )

    status.read()

    assert cutoffs == [datetime(2025, 8, 8, 14, tzinfo=timezone.utc)]


def test_dash_status_002__cursors_reject_collection_mismatch_snapshot_mismatch_and_expiry():
    now = [0.0]
    status = DashboardStatus(
        _Projects(_registered()),
        lambda project_id: _StatusModel(project_id),
        monotonic=lambda: now[0],
        snapshot_ttl=5,
    )
    first = status.read(limit=1)
    second = status.read(limit=1)

    with pytest.raises(StatusCursorError, match="one snapshot") as mismatch:
        status.read(
            project_cursor=first["projects"]["next_cursor"],
            live_cursor=second["live_indexes"]["next_cursor"],
        )
    assert mismatch.value.code == "status-cursor-mismatch"

    with pytest.raises(StatusCursorError, match="Invalid") as invalid:
        status.read(project_cursor=first["live_indexes"]["next_cursor"])
    assert invalid.value.code == "status-cursor-invalid"

    now[0] = 6
    with pytest.raises(StatusCursorError, match="restart traversal") as expired:
        status.read(project_cursor=first["projects"]["next_cursor"])
    assert expired.value.code == "status-cursor-expired"


def test_dash_status_003__project_failures_are_isolated_and_do_not_leak_exception_detail():
    projects = _Projects(_registered(2))

    def factory(project_id):
        if project_id == "p1":
            raise RuntimeError("Traceback: password=highly-secret /unsafe/internal/path")
        return _StatusModel(project_id)

    payload = DashboardStatus(projects, factory).read()

    assert len(payload["live_indexes"]["items"]) == 1
    assert payload["projects"]["items"][1]["availability"] == "unavailable"
    diagnostic = payload["diagnostics"][0]
    assert diagnostic["code"] == "project-read-failed"
    assert "secret" not in diagnostic["message"]
    assert "Traceback" not in diagnostic["message"]
    assert payload["projects"]["items"][1]["path"] == "/registered/p1"


def test_dash_status_009__project_envelopes_include_snapshot_activity_checkout_and_path_context():
    projects = _Projects(
        [
            {"id": "first", "name": "Research", "path": "/work/first/Research"},
            {"id": "second", "name": "Research", "path": "/work/second/Research"},
        ]
    )

    payload = DashboardStatus(projects, _StatusModel).read()

    first, second = payload["projects"]["items"]
    assert first["local_available"] is True
    assert first["current_head"] == "first"
    assert first["checkout"]["branch"] == "main"
    assert first["sync"]["state"] == "in-sync"
    assert first["last_activity"] == {
        "state": "known",
        "timestamp": "2026-08-08T13:00:00Z",
        "source": "commit",
        "truncated": False,
    }
    assert first["path_context"] == {"parent": "/work/first", "leaf": "Research"}
    assert second["path_context"] == {"parent": "/work/second", "leaf": "Research"}


def test_dash_status_010__unavailable_unborn_absent_and_truncated_activity_are_explicit():
    class Unavailable(_StatusModel):
        initialized = False

    class Unborn(_StatusModel):
        def status_checkout(self):
            return {"head_ref": None, "checkout": {"state": "unborn"}, "sync": {"state": "unconfigured"}}

        def status_live_indexes(self, *_args, **_kwargs):
            return []

        def status_recent_commits(self, *_args, **_kwargs):
            return [], False

    class Truncated(Unborn):
        def status_recent_commits(self, *_args, **_kwargs):
            return [], True

    models = {"bad": Unavailable, "unborn": Unborn, "cut": Truncated}
    payload = DashboardStatus(
        _Projects([
            {"id": "bad", "name": "Bad", "path": "/bad"},
            {"id": "unborn", "name": "Unborn", "path": "/unborn"},
            {"id": "cut", "name": "Cut", "path": "/cut"},
        ]),
        lambda project_id: models[project_id](project_id),
    ).read()

    bad, unborn, cut = payload["projects"]["items"]
    assert (bad["local_available"], bad["last_activity"]["state"]) == (False, "unavailable")
    assert "current_head" not in unborn
    assert unborn["checkout"]["state"] == "unborn"
    assert unborn["last_activity"] == {"state": "unknown", "truncated": False}
    assert cut["last_activity"] == {"state": "unknown", "truncated": True}


@pytest.mark.parametrize(
    ("runtime", "record", "expected"),
    [
        ({"state": "frozen", "frozen_message": "Wait"}, {"lifecycle": "cancel-requested"}, "canceling"),
        ({"state": "frozen"}, {"lifecycle": "canceled"}, "canceled"),
        ({"state": "frozen", "frozen_message": "Needs data"}, {"lifecycle": "failed"}, "needs-attention"),
        ({"state": "active"}, {"lifecycle": "failed"}, "needs-attention"),
        ({"state": "active"}, None, "in-progress"),
    ],
)
def test_dash_status_004__root_execution_state_has_deterministic_precedence(runtime, record, expected):
    assert _live_state(runtime, record)[0] == expected


def test_dash_status_005__predating_requires_a_terminal_record_updated_before_index_creation():
    index_created = "2026-08-08T12:00:00Z"

    predating = _timeline_record(
        "old", {"lifecycle": "succeeded", "created_at": 1, "updated_at": 2}, index_created
    )
    running = _timeline_record(
        "running", {"lifecycle": "running", "created_at": 1, "updated_at": 2}, index_created
    )
    unknown = _timeline_record("unknown", {"lifecycle": "running"}, index_created)

    assert predating["predates_index"] is True
    assert predating["timing"] == "predates-index"
    assert running["predates_index"] is False
    assert unknown["timing"] == "open"
    assert unknown["created_at"] is None


def test_dash_status_006__recent_commits_walk_all_parents_without_timestamp_pruning(tmp_path, monkeypatch):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")
    cutoff = datetime(2026, 7, 9, tzinfo=timezone.utc)
    descriptions = {
        "commit:a": {
            "id": Ref("commit:a"),
            "parents": [Ref("commit:b"), Ref("commit:c")],
            "dags": {"one": Ref("dag:1"), "two": Ref("dag:2")},
            "author": "A",
            "message": "merge",
            "created": cutoff.isoformat(),
        },
        "commit:b": {
            "id": Ref("commit:b"),
            "parents": [Ref("commit:d")],
            "dags": {},
            "author": "B",
            "message": "old parent",
            "created": (cutoff - timedelta(days=1)).isoformat(),
        },
        "commit:c": {
            "id": Ref("commit:c"),
            "parents": [Ref("commit:d")],
            "dags": {},
            "author": "C",
            "message": "same time",
            "created": cutoff.isoformat(),
        },
        "commit:d": {
            "id": Ref("commit:d"),
            "parents": [],
            "dags": {},
            "author": "D",
            "message": "reachable through old parent",
            "created": (cutoff + timedelta(hours=1)).isoformat(),
        },
    }
    visited = []

    class Dag:
        @staticmethod
        def describe(ref):
            return {"error": "failed" if ref.to == "dag:2" else None}

    class Dml:
        dag = Dag()

        @staticmethod
        def status():
            return {"commit": Ref("commit:a")}

        @staticmethod
        def show(commit):
            visited.append(commit.to)
            return descriptions[commit.to]

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())
    model.refs = lambda: {"local": [], "tracking": []}

    commits, truncated = model.status_recent_commits("p", "Project", cutoff=cutoff, scan_cap=10)

    assert visited == ["commit:a", "commit:b", "commit:c", "commit:d"]
    assert [item["commit_ref"] for item in commits] == ["commit:d", "commit:a", "commit:c"]
    assert commits[1]["dag_count"] == 2
    assert commits[1]["error_dag_count"] == 1
    assert commits[1]["links"] == {
        "project": "/projects/p/commits/a",
        "inspector": "/projects/p/commits/a?resource=commit%3Aa&tab=summary",
        "history": "/projects/p/commits/a?resource=commit%3Aa&tab=summary",
    }
    assert truncated is False


def test_dash_status_007__commit_scan_cap_reports_truncation(tmp_path, monkeypatch):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")

    class Dml:
        dag = object()

        @staticmethod
        def status():
            return {"commit": Ref("commit:a")}

        @staticmethod
        def show(commit):
            suffix = commit.to.split(":", 1)[1]
            parent = chr(ord(suffix) + 1)
            return {
                "id": commit,
                "parents": [Ref(f"commit:{parent}")],
                "dags": {},
                "author": "A",
                "message": suffix,
                "created": "2026-08-08T00:00:00+00:00",
            }

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())
    model.refs = lambda: {"local": [], "tracking": []}

    commits, truncated = model.status_recent_commits(
        "p", "Project", cutoff=datetime(2026, 7, 1, tzinfo=timezone.utc), scan_cap=2
    )

    assert len(commits) == 2
    assert truncated is True


def test_dash_status_008__one_live_row_does_not_expand_descendant_execution_state(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")

    class Runtime:
        @staticmethod
        def list():
            return [
                {
                    "id": Ref("index:root"),
                    "message": "Research",
                    "created": "2026-08-08T12:00:00Z",
                    "dag": Ref("dag:partial"),
                    "state": "active",
                    "frozen_message": None,
                }
            ]

        @staticmethod
        def read_execution_record(_execution):
            return {"lifecycle": "running"}

    class Dml:
        runtime = Runtime()

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())
    model.execution_graph = lambda *_roots: pytest.fail("aggregate must not read descendant lineage")

    rows = model.status_live_indexes("p", "Project", head_ref="commit:current")

    assert len(rows) == 1
    assert rows[0]["group"] == "in-progress"
    assert rows[0]["dag_ref"] == "dag:partial"
    assert rows[0]["links"] == {
        "project": "/projects/p/commits/current",
        "inspector": "/projects/p/commits/current?resource=index%3Aroot&tab=summary",
        "dag": "/projects/p/commits/current/dags/dag%3Apartial",
    }


def test_dash_status_008a__aggregate_links_use_concrete_head_or_unborn_context(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")

    class Runtime:
        @staticmethod
        def list():
            return [{"id": Ref("index:root"), "created": "2026-08-08T12:00:00Z", "dag": Ref("dag:partial")}]

    class Dml:
        runtime = Runtime()

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())

    links = model.status_live_indexes("project", "Project", head_ref=None)[0]["links"]

    assert links == {
        "project": "/projects/project/unborn",
        "inspector": "/projects/project/unborn?resource=index%3Aroot&tab=summary",
        "dag": "/projects/project/unborn?resource=dag%3Apartial&tab=summary",
    }


def test_dash_live_001__lineage_nests_and_retains_terminal_descendants():
    graph = {
        "roots": ["root"],
        "nodes": {
            "root": {
                "lifecycle": "running",
                "created_at": "2026-08-08T12:00:00Z",
                "updated_at": None,
                "spawned": ["active"],
                "children": ["done"],
            },
            "active": {
                "lifecycle": "running",
                "created_at": None,
                "updated_at": None,
                "children": [],
                "spawned": [],
            },
            "done": {
                "lifecycle": "succeeded",
                "created_at": "2026-08-08T12:01:00Z",
                "updated_at": "2026-08-08T12:02:00Z",
                "children": [],
                "spawned": [],
            },
        },
    }

    rows = _timeline_records(graph, "2026-08-08T12:00:00Z")

    assert [row["execution_id"] for row in rows] == ["root", "active", "done"]
    assert rows[1]["parent_execution_id"] == "root"
    assert rows[1]["depth"] == 1
    assert rows[1]["timing"] == "open"
    assert rows[2]["lifecycle"] == "succeeded"


def test_dash_live_002__live_index_exposes_expected_partial_dag_and_safe_missing_evidence(tmp_path):
    (tmp_path / ".dml").mkdir()
    (tmp_path / ".dml" / "HEAD").write_text("", encoding="utf-8")

    class Runtime:
        @staticmethod
        def describe(_index):
            return {
                "id": Ref("index:root"),
                "message": "Incomplete graph",
                "created": "2026-08-08T12:00:00Z",
                "dag": Ref("dag:partial"),
                "state": "active",
            }

        @staticmethod
        def read_execution_record(_execution):
            raise RuntimeError("Traceback: password=secret /internal/provider/path")

    class Dml:
        runtime = Runtime()

    model = DashboardReadModel(tmp_path, dml_factory=lambda **_kwargs: Dml())
    model.execution_graph = lambda *_roots: (_ for _ in ()).throw(RuntimeError("remote unavailable"))

    detail = model.live_index("index:root")

    assert detail["dag"] == {
        "ref": "dag:partial",
        "href": "/api/v1/dags/dag:partial",
        "partial": True,
    }
    assert "result" not in detail["dag"]
    assert detail["execution"] is None
    assert detail["lineage"] == []
    assert detail["identifiers"] == {"index": "index:root", "dag": "dag:partial"}
    assert detail["diagnostics"] == [
        {
            "availability": "unconfigured",
            "code": "execution-evidence-unavailable",
            "message": "Remote execution evidence is unavailable",
            "retryable": False,
        }
    ]
