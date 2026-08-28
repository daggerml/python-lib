## 1. Public Plugin Contract

- [x] 1.1 Add lightweight frozen `Dashboard`, `PlotlyDashboardResult`, and `VegaLiteDashboardResult` public types with normalization, strict field validation, optional `cache_version`, and exports that do not import server or renderer dependencies.
- [x] 1.2 Implement deterministic `daggerml.dashboards` entry-point discovery, provider-order preservation, distribution metadata capture, isolated load diagnostics, and first-definition-wins duplicate-name handling.
- [x] 1.3 Add contract tests for valid multi-definition providers, invalid definitions, provider failures, stable ordering, duplicate names, and lightweight public imports.

## 2. Compatibility and Local Result Cache

- [x] 2.1 Add intrinsic normalized tags to revision-scoped DAG summary/detail projections and tests for named and reachable function-context DAGs without reading commit/tree tags.
- [x] 2.2 Implement exact all-required-tags compatibility, stable compatible-definition metadata, and first-eager default selection with unit tests for empty, partial, multiple, and changed DAG tag sets.
- [x] 2.3 Implement the versioned local JSON cache with canonical hashed identities, strict reads, atomic writes, access tracking, 30-day expiry, 512 MiB LRU cleanup, 10 MiB entry rejection, and startup/write maintenance.
- [x] 2.4 Add cache contract tests covering hits, identity changes, malformed entries, expiry, LRU eviction, atomic refresh replacement, retained entries after failed refresh, and no repository-state mutation.

## 3. Rendering Service and HTTP API

- [x] 3.1 Implement the two-worker trusted in-process rendering service with revision-reachable committed public `Dag` construction, per-identity in-flight deduplication, strict JSON/result validation, and traceback-free failures.
- [x] 3.2 Add rendering-service tests for Plotly and Vega-Lite results, cache hits, explicit bypass, invalid return types, NaN/infinity, oversized payloads, exceptions, two-worker concurrency, and duplicate-request sharing.
- [x] 3.3 Add revision-scoped compatible-dashboard, selected-result, and JSON refresh endpoints under `/api/v1/dags/{dag_id}`, including existing project/revision/reachability, authentication, same-origin, and error-envelope behavior.
- [x] 3.4 Wire registry, cache cleanup, rendering executor, and shutdown lifecycle into application creation, and add API tests for unknown, incompatible, eager, cached, refreshed, and failed dashboards.

## 4. DAG Explorer User Interface

- [x] 4.1 Add Plotly and Vega-Lite renderer dependencies, TypeScript result/metadata types, and API-client methods while preserving separate lazy production bundles.
- [x] 4.2 Extend route parsing and generation with the `dashboard` query field, compatible-dashboard selection, browser history restoration, and eager-default route replacement.
- [x] 4.3 Build the DAG-page dashboard selector and bounded panel states so no non-eager definition runs before selection and changing selection requests only the active definition.
- [x] 4.4 Implement Plotly and Vega-Lite mounting/disposal, responsive presentation, stale-response suppression, cache-hit indication, bounded failures, and explicit refresh behavior.
- [x] 4.5 Add frontend tests for zero/one/multiple compatible definitions, multiple eager definitions, explicit and restored selections, incompatible links, lazy renderer choice, errors, navigation races, and refresh.

## 5. Documentation, Packaging, and Verification

- [x] 5.1 Update the dashboard architecture, public-boundary language, plugin reference, extension guidance, and user guidance for intrinsic tag matching, trusted plugin execution, eager selection, typed results, cache defaults, and invalidation.
- [x] 5.2 Add a minimal packaged example/test provider for both result variants and verify installed entry-point discovery without requiring Plotly or Altair in the Python plugin environment.
- [x] 5.3 Rebuild packaged frontend assets and verify wheel/source-distribution contents include lazy Plotly/Vega bundles but exclude local cache and development artifacts.
- [x] 5.4 Run focused Python contract/integration tests, frontend typecheck/tests/build, repository lint, and strict OpenSpec validation; resolve every failure before marking the change complete.
