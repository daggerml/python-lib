# custom-dag-dashboards Specification

## Purpose

Allow installed Python plugins to provide tag-compatible declarative visualizations that researchers can select and run locally for an immutable DAG.

## Requirements

### Requirement: Python plugins register custom dashboard definitions
The system SHALL discover custom dashboard providers from the `daggerml.dashboards` Python entry-point group. Each entry point SHALL load a zero-argument provider whose ordered result contains dashboard definitions with a non-empty `name`, a set of required intrinsic DAG `tags`, an `eager` boolean, a Python render function, and an optional `cache_version`. Dashboard names SHALL be the definition identity and SHALL be unique across the discovered registry. Discovery SHALL use entry-point name and value order followed by provider result order, preserve other valid definitions when one provider fails, and report bounded plugin diagnostics.

#### Scenario: Plugin contributes multiple definitions
- **WHEN** an installed dashboard provider returns multiple valid named definitions
- **THEN** the registry exposes them in provider result order after definitions from earlier entry points

#### Scenario: Plugin provider fails
- **WHEN** one dashboard entry point fails to load or its provider raises
- **THEN** the registry reports a bounded diagnostic for that entry point
- **AND** definitions from other valid providers remain available

#### Scenario: Dashboard names clash
- **WHEN** two discovered definitions use the same dashboard name
- **THEN** the later definition is rejected with a bounded duplicate-name diagnostic
- **AND** the first definition in stable registration order remains available

### Requirement: Intrinsic DAG tags determine compatible dashboards
A dashboard definition SHALL be compatible with a DAG when every required dashboard tag occurs in the DAG's current intrinsic tags. Matching SHALL be exact and case-sensitive, and a definition with no required tags SHALL match every DAG. The dashboard SHALL evaluate compatibility from the revision-reachable DAG itself rather than from commit, tree, or revision tags.

#### Scenario: DAG satisfies all required tags
- **WHEN** a definition requires `metrics.v1` and `candidate` and the selected DAG contains both intrinsic tags
- **THEN** the definition is included in the DAG's compatible dashboards

#### Scenario: DAG lacks one required tag
- **WHEN** a definition requires two tags and the selected DAG contains only one
- **THEN** the definition is not included in the DAG's compatible dashboards

#### Scenario: DAG tags change
- **WHEN** active tag mutation produces and selects a new immutable DAG identity
- **THEN** compatibility is evaluated from that newly selected DAG's intrinsic tags

### Requirement: Selection controls custom dashboard execution
The DAG page SHALL list every compatible dashboard in stable registration order and SHALL execute at most the selected definition. If compatible definitions with `eager = true` exist and no dashboard is selected in the URL, the first such definition in stable registration order SHALL become the selected default and execute automatically. If no compatible definition is eager, the page SHALL execute no custom dashboard until the researcher selects one. Changing selection SHALL execute or load only the newly selected definition.

#### Scenario: One of several compatible definitions is eager
- **WHEN** a DAG has several compatible definitions and at least one is eager
- **THEN** the first compatible eager definition is selected and executed by default
- **AND** every other compatible definition remains available in the selector

#### Scenario: No compatible definition is eager
- **WHEN** compatible definitions exist but none is eager and the URL has no selection
- **THEN** the page presents the selector without executing a custom dashboard

#### Scenario: Researcher selects another compatible dashboard
- **WHEN** the researcher selects a different compatible definition
- **THEN** only that definition becomes active and its result is loaded or rendered

### Requirement: Render functions receive loaded public DAGs and typed results
The server SHALL call the selected definition's render function with a loaded, committed public `Dag` for the validated project, revision, and DAG identity. A successful function SHALL return either a `PlotlyDashboardResult` containing Plotly data, layout, and configuration JSON or a `VegaLiteDashboardResult` containing a Vega-Lite specification JSON. The server SHALL reject other return types, non-JSON values, and results exceeding 10 MiB with a bounded safe diagnostic and SHALL NOT expose a traceback to the browser.

#### Scenario: Render function returns Plotly JSON
- **WHEN** a selected render function returns a valid bounded `PlotlyDashboardResult`
- **THEN** the browser renders its declarative Plotly payload

#### Scenario: Render function returns Vega-Lite JSON
- **WHEN** a selected render function returns a valid bounded `VegaLiteDashboardResult`
- **THEN** the browser renders its declarative Vega-Lite payload

#### Scenario: Render function fails
- **WHEN** a selected render function raises or returns an invalid or oversized value
- **THEN** the page presents a bounded dashboard-specific failure state without a traceback
- **AND** other compatible definitions remain selectable

### Requirement: Successful custom dashboard results use a bounded local cache
The dashboard SHALL cache only successful validated results below the selected DaggerML configuration home. A cache identity SHALL include dashboard name, immutable DAG ref, plugin distribution version, definition `cache_version`, and dashboard-result schema version. The default cache limits SHALL be 512 MiB total, 10 MiB per result, and 30 days maximum age. Cache replacement SHALL be atomic. Startup and every successful cache write SHALL remove expired entries and then evict least-recently-used entries until the total-size limit is satisfied. Cache state SHALL NOT modify a DaggerML repository, remote, DAG, commit, ref, or execution record.

#### Scenario: Valid cached result exists
- **WHEN** the selected dashboard has an unexpired result with the exact current cache identity
- **THEN** the server returns that result without invoking the render function

#### Scenario: Cache exceeds its limits
- **WHEN** startup or a successful write finds expired entries or more than 512 MiB of cached results
- **THEN** expired entries are removed and remaining entries are evicted in least-recently-used order until the cache is within its limits

#### Scenario: Render fails
- **WHEN** a render function raises or produces an invalid result
- **THEN** the failure is not stored as a cached dashboard result

#### Scenario: Researcher refreshes a dashboard
- **WHEN** the researcher explicitly refreshes the selected custom dashboard
- **THEN** the server bypasses its existing cache entry and atomically replaces that entry only after a successful render

### Requirement: Custom dashboard rendering has bounded concurrency
One dashboard server process SHALL run at most two custom dashboard render functions concurrently. Additional render requests SHALL wait for capacity without starting duplicate concurrent work for the same cache identity.

#### Scenario: More than two dashboards are requested
- **WHEN** two render functions are active and another uncached render is requested
- **THEN** the additional render waits until execution capacity is available

#### Scenario: Concurrent requests share one identity
- **WHEN** concurrent requests target the same uncached dashboard and DAG cache identity
- **THEN** one render function runs and the waiting requests reuse its successful result or receive its bounded failure
