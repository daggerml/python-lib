# Custom DAG Dashboards

Status: implemented

Purpose: Explain how an installed Python package contributes declarative custom visualizations to the local DAG dashboard.

Authority: User-facing registration, tag matching, eager selection, result types, trusted execution, and cache invalidation for custom DAG dashboards.

Scope: Python dashboard providers rendered by `dml-dashboard`; DAG authoring and plotting-library APIs remain documented elsewhere.

## Content

Register a zero-argument provider in an installable package:

```toml
[project.entry-points."daggerml.dashboards"]
research = "research_dashboards:dashboards"
```

The provider returns ordered definitions:

```python
from daggerml.dashboard import Dashboard, VegaLiteDashboardResult


def render_metrics(dag):
    values = [{"node": name, "order": index} for index, name in enumerate(dag.keys())]
    return VegaLiteDashboardResult({
        "data": {"values": values},
        "mark": "bar",
        "encoding": {
            "x": {"field": "node", "type": "nominal"},
            "y": {"field": "order", "type": "quantitative"},
        },
    })


def dashboards():
    return [Dashboard(
        name="acme.metrics.nodes",
        tags={"metrics.v1"},
        eager=True,
        cache_version="1",
        render=render_metrics,
    )]
```

A definition is compatible when all its exact, case-sensitive required tags
occur in the selected immutable DAG's intrinsic tags. The DAG page lists every
compatible definition. It runs only the selected definition; when the URL has
no selection, the first compatible eager definition is selected automatically.

Return `PlotlyDashboardResult(data, layout, config)` or
`VegaLiteDashboardResult(spec)`. Both contain plain JSON, so the provider does
not need Plotly or Altair. The browser supplies the rendering libraries.

Providers are trusted installed Python and run in the dashboard process with a
committed public `Dag`; they are not sandboxed. At most two renders run at once.
Successful results are cached locally for up to 30 days with defaults of 10 MiB
per result and 512 MiB total. The key includes the dashboard name, immutable DAG
ref, provider distribution version, `cache_version`, and result schema. Bump
`cache_version` when editable source or dependencies change without a package
version change, or use the dashboard's refresh control.

## References

- [Plugin API](../../extend/reference/plugin-api.md)
- [DAG storage and types](../../develop/architecture/dag-storage-and-types.md)
- [Local research dashboard](../../develop/architecture/dashboard.md)
