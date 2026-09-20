"""Minimal custom-dashboard provider using only DaggerML public result types."""

from daggerml.dashboard import Dashboard, PlotlyDashboardResult, VegaLiteDashboardResult


def _node_names(dag):
    return list(dag.keys())


def plotly_nodes(dag):
    """Render DAG node names as a declarative Plotly bar chart."""
    names = _node_names(dag)
    return PlotlyDashboardResult(
        data=[{"type": "bar", "x": names, "y": list(range(1, len(names) + 1))}],
        layout={"title": {"text": "DAG nodes"}},
    )


def vega_nodes(dag):
    """Render DAG node names as a declarative Vega-Lite bar chart."""
    values = [{"node": name, "order": index + 1} for index, name in enumerate(_node_names(dag))]
    return VegaLiteDashboardResult(
        {
            "data": {"values": values},
            "mark": "bar",
            "encoding": {
                "x": {"field": "node", "type": "nominal"},
                "y": {"field": "order", "type": "quantitative"},
            },
        }
    )


def dashboards():
    """Return ordered example definitions discovered from package metadata."""
    tags = {"example.metrics.v1"}
    return [
        Dashboard("example.nodes.plotly", plotly_nodes, tags=tags, eager=True),
        Dashboard("example.nodes.vega-lite", vega_nodes, tags=tags),
    ]
