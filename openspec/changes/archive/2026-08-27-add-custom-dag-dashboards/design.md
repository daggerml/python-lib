## Context

The local dashboard already validates project, concrete revision, DAG, and node reachability before returning bounded projections. Its Python package is installed with optional server dependencies, its frontend is packaged into the wheel, and its only persistent dashboard-owned data is project registration. See `proposal.md` for motivation.

DAG tags are now normalized intrinsic fields on immutable DAG objects. They are available from `Dml.dag.describe()` and public `Dag.tags`, including for execution-result and function-context DAGs that have no named tree entry. The current dashboard DAG projections do not yet return those tags.

The existing dashboard architecture calls the implementation internal and read-only except for project registration and cancellation. This change introduces a deliberately small public plugin/type surface and a local derived-result cache, while retaining the stronger invariant that custom dashboards never mutate repository, remote, or execution state.

## Goals / Non-Goals

**Goals:**

- Make custom visualization registration a Python packaging concern with no project-file registration format.
- Use intrinsic DAG tags as exact compatibility declarations for every revision-reachable committed DAG.
- Keep execution lazy except for one compatible eagerly enabled default.
- Return declarative, typed, bounded JSON that the browser renders without server-side plotting libraries.
- Make repeated rendering fast while automatically bounding local disk use.
- Keep selection deterministic, addressable, and isolated from inspector state.

**Non-Goals:**

- Sandboxing trusted local plugin code or defending against an adversarial installed Python package.
- Executing custom dashboards through DaggerML adapters, executors, or persistent execution records.
- Supporting arbitrary HTML, JavaScript callbacks, raster images, server-rendered plots, or result types beyond Plotly and Vega-Lite.
- Running every compatible dashboard eagerly or precomputing results across projects and revisions.
- Coordinating cache state across multiple dashboard server processes.

## Decisions

### Public definitions and results live in the lightweight dashboard package

`daggerml.dashboard` will export frozen `Dashboard`, `PlotlyDashboardResult`, and `VegaLiteDashboardResult` value types from a module that imports neither FastAPI nor rendering libraries. `Dashboard` contains `name`, normalized required `tags`, `render`, `eager=False`, and `cache_version=""`. The result variant determines the output renderer; definitions do not repeat an output-type field.

This intentionally changes the previous fully internal dashboard boundary. Keeping the types in `daggerml.dashboard` gives plugin authors one direct namespace while preserving optional server dependencies. Placing them in generic authoring APIs would make a UI-only extension contract look like core DAG behavior; accepting untyped dictionaries would defer basic errors until browser rendering.

### Entry points load ordered providers

The `daggerml.dashboards` entry-point group will use `importlib.metadata`. Each entry point loads and calls one zero-argument provider returning an iterable of `Dashboard` values. Entry points are sorted by `(name, value)` and provider order is retained. The registry records distribution name/version with each definition, isolates provider failures, and rejects later duplicate dashboard names with a diagnostic while preserving the first.

A provider supports several related dashboards without repetitive package metadata and gives eager selection a stable order. One-definition-per-entry-point was rejected as cumbersome; import-time decorators were rejected because discovery should not depend on incidental module import order.

### Compatibility is an all-required-tags subset test

The server adds intrinsic tags to DAG summary/detail projections and computes compatibility as `definition.tags <= dag.tags`, using exact case-sensitive strings. Empty requirements match all DAGs. Matching occurs after normal revision reachability validation and therefore works for named DAGs and reachable committed function-context DAGs. Live partial DAGs are excluded until they become committed DAG identities because rendering accepts a committed public `Dag`.

Tags are not copied into registration or cache metadata as an independent identity input. They already participate in immutable DAG identity, so a tag change selects a different DAG ref and naturally recomputes compatibility and cache identity.

### The selector is lazy with one deterministic eager default

The compatible-definitions response preserves registry order and identifies the first compatible `eager=True` definition as the default. The browser writes an automatically selected eager name into the `dashboard` query field using route replacement, then requests only that result. Without an eager compatible definition, no result request occurs until explicit selection. Selecting another definition updates the query and requests only that definition.

The server remains authoritative for compatibility. An unknown or incompatible requested name returns a bounded unavailable response and is never invoked. A selector was chosen over stacked rendering because custom render functions may be expensive and an unbounded number of matching panels would make DAG navigation unpredictable.

### Rendering runs trusted plugin code in a bounded in-process service

After validating project, revision, DAG reachability, compatibility, and cache identity, the render service constructs a committed public `Dag` wrapper over the selected public `Dml` and DAG `Ref`. It invokes the selected callable in a server-owned `ThreadPoolExecutor(max_workers=2)`. A per-cache-key in-flight map lets concurrent callers await one future.

The service accepts only the two public result classes. It serializes with strict JSON semantics, including rejection of NaN and infinity, and enforces the 10 MiB encoded-result limit before cache publication or response. Exceptions become dashboard-specific error envelopes without tracebacks. Request cancellation does not terminate an already running callable.

In-process execution is appropriate for trusted installed plugins and allows the public `Dag` wrapper to retain normal lazy node access. Subprocess execution was rejected for this version because reconstructing the repository context, serializing the wrapper, propagating plugin environments, and terminating descendants would add a separate worker protocol. DaggerML runtime execution was rejected because visualization would create repository execution state and inherit remote runtime latency.

### The HTTP API separates discovery, rendering, and refresh

Revision-scoped routes will be added under the existing `/api/v1/dags/{dag_id}` family:

- `GET /dashboards` returns compatible definition metadata, plugin diagnostics, and the eager default name.
- `GET /dashboard?name=...` returns a validated cached or newly rendered discriminated result and cache-hit metadata.
- `POST /dashboard/refresh` with JSON `{ "name": "..." }` bypasses the existing entry and replaces it only after a successful render.

Every route requires registered `project` and concrete `revision` scope and reuses existing DAG reachability validation. Definition callables, Python objects, plugin paths, and tracebacks never enter responses. Refresh mutates only the local derived cache and uses the existing same-origin or bearer-token protections.

### Cache entries are atomic bounded JSON files

The cache root will be `<config_home>/<daggerml-version>/dashboard/custom-dashboard-cache/v1`. Each entry filename is the SHA-256 digest of canonical identity JSON containing dashboard name, DAG ref, plugin distribution name/version, definition cache version, and result schema version. Its JSON envelope contains the discriminated result and creation metadata; file modification time records recent access for LRU ordering.

Reads validate the envelope and bounds before use. Writes serialize to a sibling temporary file, fsync, and atomically replace the target. A process lock protects scans, access updates, writes, and cleanup. Startup and each successful write delete malformed temporary files, entries older than 30 days, then least-recently-used entries until total regular-file size is at most 512 MiB. A result larger than 10 MiB is rejected rather than cached. Failed refresh retains the prior valid entry.

SQLite was rejected because the cache needs only opaque key lookup and bounded LRU scanning at a small default size. Persisting results in DaggerML storage was rejected because visualizations are local derived UI state, not research DAG data or repository history.

### Browser rendering uses packaged declarative runtimes

The frontend will dynamically load `plotly.js-dist-min` for Plotly results and `vega-embed` for Vega-Lite results, render into a bounded responsive panel, and dispose the prior renderer when selection or DAG scope changes. It will show independent loading, unavailable, execution-error, and invalid-result states plus a refresh control. Dynamic imports avoid loading both renderer runtimes for every DAG visit, although both remain packaged wheel assets.

## Risks / Trade-offs

- [A trusted render function can hang, consume memory, or mutate through its public `Dml` handle] → Document the trusted-plugin boundary, cap concurrency at two, and keep subprocess isolation as a future protocol rather than implying protection this version does not provide.
- [Editable plugin source can change without a distribution-version change] → Include explicit `cache_version`; plugin authors bump it when output logic or dependencies change, and researchers can force refresh.
- [Full Plotly and Vega runtimes increase packaged frontend size] → Split each renderer into a lazy bundle and load only the selected result kind.
- [Filesystem LRU access updates add small local writes] → Touch only cache hits and serialize cache maintenance under one process lock.
- [Multiple server processes can render or clean the same identity concurrently] → Atomic replacement keeps entries valid; cross-process deduplication is explicitly outside this version.
- [A refresh request may finish after navigation changes] → Key every request by immutable DAG ref and dashboard name, and discard stale browser responses when route scope changes.

## Migration Plan

1. Add the public definition/result types and plugin registry without installed providers; existing dashboard behavior remains unchanged.
2. Add DAG-tag projections, compatibility and rendering APIs, and the local cache behind routes unused by the current frontend.
3. Add renderer dependencies, selector state, eager default routing, rendering, and refresh UI.
4. Update dashboard/plugin documentation and packaged assets, then test with example providers for both result variants.

Rollback removes the selector and routes first, then the registry/types. Existing local cache files are derived and may be deleted automatically or left for age-based cleanup; no repository migration or rollback is required.
