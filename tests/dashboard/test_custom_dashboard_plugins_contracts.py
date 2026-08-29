from __future__ import annotations

import subprocess
import sys
from dataclasses import FrozenInstanceError
from importlib.metadata import EntryPoint

import pytest

from daggerml.dashboard import Dashboard, PlotlyDashboardResult, VegaLiteDashboardResult
from daggerml.dashboard.plugins import compatible_dashboards, load_dashboard_plugins


def _render(_dag):
    return VegaLiteDashboardResult({"mark": "point"})


def _provider():
    return [Dashboard("z.last", _render), Dashboard("a.first", _render, tags={"metrics.v1"}, eager=True)]


def _bad_provider():
    raise RuntimeError("provider exploded with internal detail")


def _mixed_provider():
    return [Dashboard("a.first", _render), object()]


def _points(monkeypatch):
    module = sys.modules[__name__]
    monkeypatch.setitem(sys.modules, "dashboard_plugin_fixture", module)
    return [
        EntryPoint("z-provider", "dashboard_plugin_fixture:_bad_provider", "daggerml.dashboards"),
        EntryPoint("a-provider", "dashboard_plugin_fixture:_provider", "daggerml.dashboards"),
        EntryPoint("b-provider", "dashboard_plugin_fixture:_mixed_provider", "daggerml.dashboards"),
    ]


def test_dash_plugin_001__public_types_are_frozen_and_validate_fields():
    dashboard = Dashboard("acme.metrics", _render, tags=["metrics.v1", "metrics.v1"])
    assert dashboard.tags == frozenset({"metrics.v1"})
    assert PlotlyDashboardResult([{"x": [1]}]).layout == {}
    with pytest.raises(FrozenInstanceError):
        dashboard.name = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        Dashboard("", _render)
    with pytest.raises(TypeError):
        VegaLiteDashboardResult([])  # type: ignore[arg-type]


def test_dash_plugin_002__discovery_is_stable_isolated_and_first_name_wins(monkeypatch):
    registered, diagnostics = load_dashboard_plugins(_points(monkeypatch))
    assert [item.definition.name for item in registered] == ["z.last", "a.first"]
    assert {item.code for item in diagnostics} == {"duplicate-name", "invalid-definition", "plugin-load-failed"}


def test_dash_plugin_003__matching_requires_exact_tag_subset(monkeypatch):
    registered, _ = load_dashboard_plugins(_points(monkeypatch))
    assert [item.definition.name for item in compatible_dashboards(registered, {"metrics.v1"})] == [
        "z.last",
        "a.first",
    ]
    assert [item.definition.name for item in compatible_dashboards(registered, {"Metrics.v1"})] == ["z.last"]


def test_dash_plugin_004__public_import_does_not_load_server_or_read_model():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import daggerml.dashboard; "
            "assert 'daggerml.dashboard.server' not in sys.modules; "
            "assert 'daggerml.dashboard.read_model' not in sys.modules",
        ],
        check=True,
    )
