from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from daggerml._core import Ref
from daggerml.dashboard import Dashboard, PlotlyDashboardResult, VegaLiteDashboardResult
from daggerml.dashboard.cache import DashboardCacheIdentity, DashboardResultCache
from daggerml.dashboard.plugins import RegisteredDashboard
from daggerml.dashboard.rendering import CustomDashboardError, CustomDashboardService


class _Dml:
    class DagOps:
        @staticmethod
        def describe(_ref):
            return {"tags": ["metrics.v1"]}

    dag = DagOps()


def _registered(name, render, *, tags=frozenset(), eager=False, version="v1"):
    return RegisteredDashboard(
        Dashboard(name, render, tags=tags, eager=eager, cache_version=version),
        "test-dashboards",
        "1.0",
        "fixture",
    )


def test_dash_render_001__typed_results_are_cached_and_refresh_bypasses(tmp_path):
    calls = []

    def render(dag):
        calls.append(dag.ref)
        return PlotlyDashboardResult([{"x": [1, 2]}], {"title": "Metrics"})

    service = CustomDashboardService(
        [_registered("metrics", render, tags={"metrics.v1"})],
        [],
        DashboardResultCache(tmp_path),
    )
    try:
        first = service.render(dml=_Dml(), dag_ref=Ref("dag:one"), tags=["metrics.v1"], name="metrics")
        second = service.render(dml=_Dml(), dag_ref=Ref("dag:one"), tags=["metrics.v1"], name="metrics")
        refreshed = service.render(
            dml=_Dml(), dag_ref=Ref("dag:one"), tags=["metrics.v1"], name="metrics", refresh=True
        )
    finally:
        service.close()
    assert (first["kind"], first["cache_hit"]) == ("plotly", False)
    assert second["cache_hit"] is True
    assert refreshed["cache_hit"] is False
    assert len(calls) == 2


@pytest.mark.parametrize(
    ("result", "code"),
    [
        ({"kind": "plotly"}, "invalid-dashboard-result"),
        (VegaLiteDashboardResult({"value": float("nan")}), "invalid-dashboard-result"),
    ],
)
def test_dash_render_002__invalid_results_are_safe_and_uncached(tmp_path, result, code):
    service = CustomDashboardService([_registered("invalid", lambda _dag: result)], [], DashboardResultCache(tmp_path))
    try:
        with pytest.raises(CustomDashboardError) as raised:
            service.render(dml=_Dml(), dag_ref=Ref("dag:one"), tags=[], name="invalid")
    finally:
        service.close()
    assert raised.value.code == code
    assert not list(tmp_path.glob("*.json"))


def test_dash_render_003__oversized_and_raised_results_do_not_replace_cache(tmp_path):
    current = {"fail": False}

    def render(_dag):
        if current["fail"]:
            raise RuntimeError("private details")
        return VegaLiteDashboardResult({"mark": "point"})

    registered = _registered("metrics", render)
    cache = DashboardResultCache(tmp_path)
    service = CustomDashboardService([registered], [], cache)
    try:
        service.render(dml=_Dml(), dag_ref=Ref("dag:one"), tags=[], name="metrics")
        current["fail"] = True
        with pytest.raises(CustomDashboardError, match="rendering failed") as raised:
            service.render(dml=_Dml(), dag_ref=Ref("dag:one"), tags=[], name="metrics", refresh=True)
        identity = DashboardCacheIdentity("metrics", "dag:one", "test-dashboards", "1.0", "v1")
        assert cache.get(identity) == {"kind": "vega-lite", "spec": {"mark": "point"}}
    finally:
        service.close()
    assert raised.value.code == "dashboard-render-failed"
    assert "private" not in str(raised.value)


def test_dash_render_004__concurrency_is_two_and_duplicate_identity_shares_work(tmp_path):
    state = {"active": 0, "maximum": 0, "calls": 0}
    lock = threading.Lock()

    def render(_dag):
        with lock:
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
            state["calls"] += 1
        time.sleep(0.05)
        with lock:
            state["active"] -= 1
        return VegaLiteDashboardResult({"mark": "point"})

    service = CustomDashboardService([_registered("metrics", render)], [], DashboardResultCache(tmp_path))
    try:
        with ThreadPoolExecutor(max_workers=4) as callers:
            futures = [
                callers.submit(
                    service.render,
                    dml=_Dml(),
                    dag_ref=Ref("dag:shared" if index < 2 else f"dag:{index}"),
                    tags=[],
                    name="metrics",
                )
                for index in range(4)
            ]
            assert all(future.result()["kind"] == "vega-lite" for future in futures)
    finally:
        service.close()
    assert state == {"active": 0, "maximum": 2, "calls": 3}


def test_dash_render_005__unknown_and_incompatible_names_are_never_invoked(tmp_path):
    service = CustomDashboardService(
        [_registered("metrics", lambda _dag: VegaLiteDashboardResult({}), tags={"metrics.v1"})],
        [],
        DashboardResultCache(tmp_path),
    )
    try:
        with pytest.raises(CustomDashboardError) as incompatible:
            service.render(dml=_Dml(), dag_ref=Ref("dag:one"), tags=[], name="metrics")
        with pytest.raises(CustomDashboardError) as missing:
            service.render(dml=_Dml(), dag_ref=Ref("dag:one"), tags=[], name="missing")
    finally:
        service.close()
    assert incompatible.value.code == "dashboard-incompatible"
    assert missing.value.code == "dashboard-not-found"
