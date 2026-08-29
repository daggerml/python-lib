from __future__ import annotations

from importlib.metadata import EntryPoint
from pathlib import Path

from daggerml.dashboard import PlotlyDashboardResult, VegaLiteDashboardResult
from daggerml.dashboard.plugins import load_dashboard_plugins


class _Dag:
    @staticmethod
    def keys():
        return ["features", "score"]


def test_dash_example_001__installable_provider_returns_both_json_result_variants(monkeypatch):
    source = Path(__file__).parents[2] / "examples" / "dashboard-plugin" / "src"
    monkeypatch.syspath_prepend(source)
    point = EntryPoint("example", "example_dashboard_plugin:dashboards", "daggerml.dashboards")

    registered, diagnostics = load_dashboard_plugins([point])

    assert diagnostics == []
    assert [item.definition.name for item in registered] == [
        "example.nodes.plotly",
        "example.nodes.vega-lite",
    ]
    assert isinstance(registered[0].definition.render(_Dag()), PlotlyDashboardResult)
    assert isinstance(registered[1].definition.render(_Dag()), VegaLiteDashboardResult)
