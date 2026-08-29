from __future__ import annotations

import json
import os
import time

import pytest

from daggerml.dashboard.cache import DashboardCacheIdentity, DashboardResultCache
from daggerml.dashboard.models import Dashboard, VegaLiteDashboardResult
from daggerml.dashboard.plugins import RegisteredDashboard, compatible_dashboard_metadata


def _identity(**changes):
    values = {
        "dashboard": "acme.metrics",
        "dag_ref": "dag:one",
        "distribution": "acme-dashboards",
        "distribution_version": "1.2.3",
        "cache_version": "v1",
    }
    values.update(changes)
    return DashboardCacheIdentity(**values)


def test_dash_cache_001__identity_changes_and_strict_cache_hits(tmp_path):
    cache = DashboardResultCache(tmp_path)
    identity = _identity()
    result = {"kind": "vega-lite", "spec": {"mark": "point"}}
    cache.put(identity, result)
    assert cache.get(identity) == result
    assert cache.get(_identity(dag_ref="dag:two")) is None
    assert cache.get(_identity(cache_version="v2")) is None


def test_dash_cache_002__malformed_expired_and_oversized_entries_are_removed(tmp_path):
    cache = DashboardResultCache(tmp_path, max_entry_bytes=180, max_age_seconds=1)
    malformed = tmp_path / "malformed.json"
    malformed.write_text("{", encoding="utf-8")
    identity = _identity()
    path = cache.path(identity)
    path.write_text(json.dumps({"schema": 1, "identity": {}, "result": {}}), encoding="utf-8")
    assert cache.get(identity) is None
    old = tmp_path / "old.json"
    old.write_text("{}", encoding="utf-8")
    os.utime(old, (time.time() - 5, time.time() - 5))
    cache.cleanup()
    assert not old.exists()
    with pytest.raises(ValueError, match="exceeds"):
        cache.put(identity, {"kind": "vega-lite", "spec": {"values": "x" * 500}})
    cache.cleanup()
    assert not malformed.exists()


def test_dash_cache_003__cleanup_evicts_least_recently_used(tmp_path):
    cache = DashboardResultCache(tmp_path, max_bytes=450)
    first = _identity(dag_ref="dag:first")
    second = _identity(dag_ref="dag:second")
    cache.put(first, {"kind": "vega-lite", "spec": {"value": "x" * 80}})
    first_path = cache.path(first)
    old = time.time() - 10
    os.utime(first_path, (old, old))
    cache.put(second, {"kind": "vega-lite", "spec": {"value": "y" * 80}})
    assert cache.get(first) is None
    assert cache.get(second) is not None


def test_dash_cache_004__compatibility_metadata_is_ordered_and_selects_first_eager():
    def render(_dag):
        return VegaLiteDashboardResult({"mark": "point"})

    registered = [
        RegisteredDashboard(Dashboard("all", render), "p", "1", "a"),
        RegisteredDashboard(Dashboard("first", render, tags={"x"}, eager=True), "p", "1", "a"),
        RegisteredDashboard(Dashboard("second", render, tags={"x"}, eager=True), "p", "1", "a"),
        RegisteredDashboard(Dashboard("missing", render, tags={"y"}), "p", "1", "a"),
    ]
    payload = compatible_dashboard_metadata(registered, {"x"})
    assert [item["name"] for item in payload["items"]] == ["all", "first", "second"]
    assert payload["default"] == "first"
