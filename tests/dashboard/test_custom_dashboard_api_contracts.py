from __future__ import annotations

import pytest

pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from daggerml._core import Ref
from daggerml.dashboard import Dashboard, VegaLiteDashboardResult
from daggerml.dashboard.cache import DashboardResultCache
from daggerml.dashboard.plugins import RegisteredDashboard
from daggerml.dashboard.rendering import CustomDashboardService
from daggerml.dashboard.server import create_app

COMMIT = "a" * 64


class _Model:
    initialized = True
    project_home = None
    dml = object()

    @staticmethod
    def dag(dag_id, *, project, revision):
        assert project == "registered"
        assert revision == COMMIT
        return {"id": Ref(dag_id if ":" in dag_id else f"dag:{dag_id}"), "tags": ["metrics.v1"]}


def _app(tmp_path, render):
    app = create_app(tmp_path / "config")
    app.state.projects.get = lambda project: tmp_path if project == "registered" else None
    app.state.project_models[str(tmp_path.resolve())] = _Model()
    app.state.custom_dashboards.close()
    registered = RegisteredDashboard(
        Dashboard("acme.metrics", render, tags={"metrics.v1"}, eager=True),
        "fixture",
        "1",
        "fixture",
    )
    app.state.custom_dashboards = CustomDashboardService([registered], [], DashboardResultCache(tmp_path / "cache"))
    return app


def test_dash_custom_api_001__metadata_render_cache_and_refresh_are_revision_scoped(tmp_path):
    calls = []

    def render(dag):
        calls.append(dag.ref)
        return VegaLiteDashboardResult({"mark": "bar", "call": len(calls)})

    app = _app(tmp_path, render)
    with TestClient(app, base_url="http://127.0.0.1") as client:
        scope = f"project=registered&revision={COMMIT}"
        metadata = client.get(f"/api/v1/dags/dag:one/dashboards?{scope}")
        first = client.get(f"/api/v1/dags/dag:one/dashboard?name=acme.metrics&{scope}")
        cached = client.get(f"/api/v1/dags/dag:one/dashboard?name=acme.metrics&{scope}")
        refreshed = client.post(
            f"/api/v1/dags/dag:one/dashboard/refresh?{scope}",
            json={"name": "acme.metrics"},
        )
    assert metadata.json()["default"] == "acme.metrics"
    assert first.json()["cache_hit"] is False
    assert cached.json()["cache_hit"] is True
    assert refreshed.json()["spec"]["call"] == 2


def test_dash_custom_api_002__errors_and_refresh_content_type_are_safe(tmp_path):
    def render(_dag):
        raise RuntimeError("private traceback detail")

    app = _app(tmp_path, render)
    with TestClient(app, base_url="http://127.0.0.1") as client:
        scope = f"project=registered&revision={COMMIT}"
        failed = client.get(f"/api/v1/dags/dag:one/dashboard?name=acme.metrics&{scope}")
        unknown = client.get(f"/api/v1/dags/dag:one/dashboard?name=unknown&{scope}")
        content_type = client.post(
            f"/api/v1/dags/dag:one/dashboard/refresh?{scope}",
            content="{}",
            headers={"content-type": "text/plain"},
        )
    assert (failed.status_code, failed.json()["error"]["code"]) == (500, "dashboard-render-failed")
    assert "private" not in failed.text
    assert (unknown.status_code, unknown.json()["error"]["code"]) == (404, "dashboard-not-found")
    assert content_type.status_code == 415
