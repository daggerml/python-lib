# DaggerML Dashboard Server

This package serves the local dashboard, including its HTTP API, read model,
execution inspection, cancellation delegation, plugin results, and packaged
frontend assets. It consumes established DaggerML interfaces and remains
read-only except for confirmed cancellation through the runtime API.

The public dashboard plugin surface is exposed from `daggerml.dashboard`.
Normative dashboard behavior is owned by the dashboard OpenSpec capabilities in
[`openspec/spec-overview.md`](../../../openspec/spec-overview.md); the browser
source lives in [`dashboard-ui/`](../../../dashboard-ui/).

## Custom dashboard providers

An installed package registers a zero-argument provider through the
`daggerml.dashboards` entry-point group:

```toml
[project.entry-points."daggerml.dashboards"]
research = "research_dashboards:dashboards"
```

The provider returns ordered `daggerml.dashboard.Dashboard` definitions. Each
definition has a unique namespaced name, exact case-sensitive required DAG tags,
an `eager` flag, an optional `cache_version`, and a render function that receives
a committed public `Dag`. Render functions return `PlotlyDashboardResult` or
`VegaLiteDashboardResult`; both hold plain JSON and do not require plotting
libraries in the provider environment.

Definitions are compatible when all required tags occur in the selected DAG's
intrinsic tags. Discovery sorts entry points by name and value, preserves
provider order, isolates provider failures, and keeps the first definition when
names collide. The DAG page executes only the selected compatible definition;
without an explicit selection, it selects the first compatible eager definition.

Providers are trusted installed Python running inside the dashboard process and
are not sandboxed. At most two renders run concurrently. Successful results are
cached locally for up to 30 days, with defaults of 10 MiB per result and 512 MiB
total. Cache identity includes the dashboard name, immutable DAG ref, provider
distribution version, `cache_version`, and result schema. Bump `cache_version`
when editable source or dependencies change without a distribution version
change, or use the dashboard refresh control.
