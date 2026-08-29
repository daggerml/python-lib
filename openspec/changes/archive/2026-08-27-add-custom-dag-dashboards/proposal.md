## Why

The DAG explorer can inspect persisted graph structure and values, but project-specific research schemas still require users to leave DaggerML to understand their results. Installed Python packages should be able to contribute declarative visualizations that the dashboard discovers, matches against intrinsic DAG tags, and runs locally on demand.

## What Changes

- Add a Python entry-point group through which plugins provide named custom dashboard definitions.
- Define dashboard definitions with a unique user-managed name, required DAG tags, an `eager` default-selection flag, and a Python render function accepting a loaded public `Dag`.
- Define typed Plotly and Vega-Lite results containing bounded declarative JSON payloads.
- Show every tag-compatible custom dashboard in a DAG-page selector; automatically select and run the first eagerly enabled compatible definition in stable registration order.
- Execute only the selected custom dashboard, expose safe loading and failure states, preserve selection in the DAG-page URL, and allow explicit refresh.
- Cache successful results locally with bounded entry size, total size, age, concurrency, atomic replacement, and automatic least-recently-used cleanup.

## Capabilities

### New Capabilities

- `custom-dag-dashboards`: Plugin discovery, definition and result contracts, intrinsic DAG-tag matching, eager default selection, execution, local caching, and declarative rendering.

### Modified Capabilities

- `dashboard-revision-navigation`: Extend DAG-page addressable state and interaction requirements with custom-dashboard selection and rendering.

## Impact

- Adds a `daggerml.dashboards` Python entry-point surface and public dashboard definition/result types.
- Extends the dashboard read model and versioned HTTP API with DAG tags, compatible dashboard metadata, render results, cache state, and refresh behavior.
- Extends the DAG explorer UI and URL query state with a custom-dashboard selector and Plotly/Vega-Lite renderers.
- Adds frontend renderer dependencies and local dashboard-cache files below the selected DaggerML configuration home; repository and remote state remain unchanged.
