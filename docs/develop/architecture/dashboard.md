# Local Research Dashboard

Status: specified

## Purpose

Define the local dashboard's product boundary, read model, HTTP interface,
security model, and relationship to the DaggerML repository and execution
services.

## Authority

This document is normative for the `dml-dashboard` command, Home and canonical
project-and-commit dashboard routes, the versioned local dashboard API,
revision-versus-current response boundaries, dashboard read-only behavior,
cancellation confirmation, redaction, executor introspection, and packaged
frontend assets.

## Scope

The local dashboard server and browser application, including local repository
inspection, optional remote inspection, live execution observation, and
user-requested cancellation. Existing repository, remote, and execution
semantics remain owned by their subsystem architecture documents.

The dashboard is an experimental appendage to DaggerML rather than part of the
core repository architecture. Treat it like a separate application package
that consumes established DaggerML interfaces: its inspection paths are
read-only and do not extend or redefine core repository, remote, or execution
contracts. Confirmed cancellation is the narrow exception; the dashboard only
delegates it to the existing runtime API and does not own lifecycle state.

## Content

### Boundary and startup

`dml-dashboard` starts a same-origin HTTP server and serves a packaged
React/TypeScript application. It opens the browser unless `--no-open` is set.
The command accepts:

```text
dml-dashboard [--config-home PATH] [--host 127.0.0.1] [--port 8765]
              [--no-open] [--allow-remote]
```

`--config-home` is also accepted as `--config-dir`. The dashboard does not
accept a project-directory argument. It discovers the default and registered
project directories from the selected DaggerML configuration directory, then
resolves each project's local configuration from that project directory.

The default server binds only to `127.0.0.1`. A non-loopback host is rejected
unless `--allow-remote` is present. An allowed non-loopback server generates an
ephemeral bearer token, prints a warning and the token-bearing launch URL, and
requires the token for all API and event-stream requests. The token is held in
memory for that server process and is not persisted.

`--config-home` follows normal DaggerML configuration-home resolution. A
configuration directory without a configured or registered project still
starts successfully and presents a diagnostic empty state. Missing optional
remote configuration or unavailable external services degrade the
corresponding panels; they do not prevent local inspection.

The dashboard implementation is internal except for the lightweight public
`Dashboard`, `PlotlyDashboardResult`, and `VegaLiteDashboardResult` plugin
contract in `daggerml.dashboard`. The production frontend is built ahead of publication and
included in the Python wheel and source distribution. Runtime startup never
requires Node.js or a package-manager installation.

### Product organization

The browser application uses a persistent workbench shell whose primary
navigation collapses to a current-page-aware icon rail, plus global search,
breadcrumbs, a command palette, keyboard navigation, responsive layouts, and
equivalent light and dark themes. The DaggerML brand links to the sole global
destination, **Home**. Home combines the cross-project status content and
project directory. Selecting a locally available project resolves its `HEAD` to
a concrete commit and opens its project workspace, whose navigation contains
only **Overview**, **DAG Explorer**, and **Tags and refs**:

- **Home** groups every locally present index once as Needs attention, In
  progress, Canceling, or Canceled. Recent reachable commits feed its rolling
  one-year calendar rather than a duplicate Recently completed queue or
  expandable activity list. The calendar uses seven
  weekday rows and month-labeled compact week columns, preserves source-project
  identity through accessible per-cell labels, and reports bounded-history truncation.
- **Home** lists registered repositories with shortened, safe path context,
  availability, checkout and activity summaries, live-work and sync counts,
  and recent-commit context. The full registered path is disclosed on pointer
  hover, keyboard focus, and through an accessible description; it is never
  supplied by an aggregate request.
- **Overview** summarizes the selected commit's checkout and repository
  content alongside clearly labeled current health, live-index, and execution
  information. It does not display a project URI or Infrastructure card.
- **DAG Explorer** renders committed DAGs and partial DAGs belonging to live
  indexes through the same canvas. Node silhouette identifies persisted node
  type; border color identifies structural role such as DAG arguments, result,
  or error, and an on-canvas legend defines both encodings. For a committed DAG,
  it lists installed custom dashboards whose required exact tags are a subset
  of that DAG's intrinsic tags. It runs only the selected definition; the first
  compatible eager definition becomes the addressable default.

Status, Projects, History, Runs, Remotes, and Activity are not primary
destinations. Relevant execution,
remote-health, log, resource, and cancellation evidence is available in the
project pages and contextual inspector.

A shared inspector presents applicable Summary, Value, Runnable, DAG, Lineage,
and Logs tabs for commits, live indexes, DAGs, nodes, errors,
executions, and external resources. Every node has Value. FnNodes and their
function-context DAGs also have Runnable. Value means the persisted node value;
Runnable means the function-applied runnable stored at context argv position
zero. When an FnNode returns a Runnable, its Value and Runnable tabs therefore
show separate runnable inspections with separate meanings. Logs appears only when the selected resource resolves to an
execution log source, contains only bounded stdout/stderr output, and never
duplicates Summary as a fallback. Routes, filters, inspector selections, and inspector tabs
are addressable so users can follow provenance and use browser history without
losing their working context. On desktop the inspector is a resizable workspace
column; on tablet it is a drawer; on mobile it is a dedicated detail surface.
Large graphs, values, scripts, stacks, and logs use explicit bounds or
pagination rather than unbounded eager responses. This is a v0 organization:
the removed destinations, implicit-project routes, and their response or route
compatibility adapters are not retained.

Canonical browser scope is encoded in these routes:

```text
/
/projects/:project/commits/:commit
/projects/:project/commits/:commit/dags
/projects/:project/commits/:commit/dags/:dag
/projects/:project/commits/:commit/refs
/projects/:project/unborn
```

Project and commit segments are percent encoded. A commit route contains a
concrete bare commit ID, not `HEAD`, a branch, or a tag. Home, search, and the
project switcher resolve `HEAD` once before navigating; a concrete selection
does not move when `HEAD` later changes. Query state for an owning route may
name an inspector resource, resource type, tab, graph filter, or selected custom
dashboard. It is not an
alternative source of project or revision scope.

Function nodes expose their context DAG as a navigable relationship. Function
node inspection and function-context DAG inspection both expose the exact
function-applied runnable stack. Runnable node values use the same presentation
in Value. Summary retains concise properties and context navigation but does
not duplicate value previews, runnable stacks, script source, or prepopulation.
When execution state for the
function DAG's persisted cache key is available, both surfaces resolve its
bounded stdout and stderr streams even after the completed execution leaves
execution enumeration. The browser supplies the function DAG ref, not an
arbitrary cache key; the server derives the canonical stream identity from the
persisted DAG.

### Read model

The server has a dashboard-specific read-model layer over `Dml`, `Head`,
`CommitOps`, `DagOps`, runtime and execution records, remote refs, and executor
state. Read handlers use the established core operations or read-only storage
transactions. They do not write refs, materialize arbitrary remote artifacts,
refresh caches, fetch branches, or otherwise alter repository state.

Installed `daggerml.dashboards` entry points may provide trusted Python render
functions. The server passes one validated revision-reachable committed public
`Dag` and accepts only bounded Plotly or Vega-Lite result values. It runs at
most two render functions concurrently and coalesces work for one result
identity. Plugin code runs in-process without an adversarial sandbox.

Successful results use a local derived JSON cache below the selected
configuration home. Its default bounds are 10 MiB per result, 512 MiB total,
and 30 days. Startup and successful writes remove expired entries and evict
least-recently-used entries. Cache writes are atomic and never modify a
repository, remote, execution record, or persisted DAG.

Remote-backed panels query configured S3 state and CloudWatch only. Each
external lookup is bounded and failure-isolated. The response distinguishes
unavailable, unconfigured, unauthorized, and empty state.

The read model projects an execution's persisted runnable chain from outermost
executor to its nested runnable. Every layer is a discriminated object with a
`kind` and only fields pertinent to that executor:

- `script`: function name, trusted script URI, bounded rendered source, process
  identifier when recorded, and available log streams.
- `docker`: image, safe flags, container identifier and status, temporary-image
  state, and its nested runnable.
- `ssh`: host, safe flags, environment-file paths, and its nested runnable.
  Environment-file contents are never read or returned.
- `batch`: Lambda target, image, CPU, memory, GPU, queue, job identifier,
  definition, status, attempts, CloudWatch streams, and nested runnable.
- `cfn`: stack identity, operation/status, and relevant rollback context.
- Unknown extensions: a generic, redacted view of the runnable type, URI, safe
  scalar metadata, persisted launch state, and nested runnable when recognizable.

Fields that do not apply to a layer are omitted rather than returned as null.
Artifact inspection returns metadata and sanitized URIs and does not
automatically download artifact content.

Runnable inspection follows only the explicit `sub` chain, from outermost layer
to innermost entrypoint, with a fixed depth bound and explicit truncation. A
Python script preview is available only when that innermost entrypoint is a
script executor with a persisted script URI. Other entrypoint kinds, missing
URIs, unavailable objects, absent remote configuration, and URIs outside the
configured remote root produce distinct safe availability codes. Script source
is loaded lazily and bounded.

The innermost script runnable's prepopulation is projected as bounded rows of
name, safe value type, and optional node link; raw values are not returned. A
link is present only when the name resolves to a persisted node in the same
applied context and revision. Returned Runnable values may declare
prepopulation that has not been instantiated, in which case the row remains
unlinked.

The Home aggregate read opens each registered project independently. It reads
local index summaries and current-HEAD-reachable commits without enumerating
historical remote executions or eagerly resolving execution graphs and
resources. One failure produces a safe project diagnostic while other projects
remain available. Current-HEAD ancestry follows every parent breadth-first in
persisted order, counts unique commits against a per-project cap, includes the
exact 365-day cutoff, and orders included commits by timestamp descending then
ref ascending.

Project workspace responses separate immutable repository snapshot data from
present-day operations. They identify the requested revision, resolved commit,
current `HEAD`, and whether the selection is current; commit-derived checkout,
history, DAGs, and nodes belong to the selected revision, while live indexes,
executions, remote availability, sync, and executor health are explicitly
current. Historical selection never falls back to `HEAD` or attributes current
operations to the historical commit. DAG explorer includes live or partial DAGs
only while its concrete selection remains current `HEAD`.

### Local HTTP API

All JSON routes are same-origin and live below `/api/v1`. Successful collection
responses use:

```json
{
  "items": [],
  "next_cursor": null
}
```

Cursors are opaque. Detail responses return the named resource object directly.
Errors use:

```json
{
  "error": {
    "code": "stable-machine-code",
    "message": "safe user-facing message",
    "retryable": false
  }
}
```

The API provides these resource families:

| Resource | Required behavior |
| --- | --- |
| `/status` | Home's failure-isolated cross-project project, live-index, recent-commit, diagnostic, and truncation envelopes. Its three collections have independent cursors into one five-minute snapshot; aggregate requests accept no project path. |
| `/projects` | Registered-project collection used for Home selection; Home displays its enriched aggregate status fields rather than treating this as a standalone destination. |
| `/health`, `/overview` | Server readiness; and, for `/overview` with required `project` and `revision` workspace scope, revision metadata, commit-scoped repository content, and separately current local, remote, runtime, and health state. |
| `/history`, `/commits`, `/dags`, `/dags/{dag_id}`, `/nodes/{node_id}` | Required `project` and `revision` workspace scope; cursor-paginated ancestry and commit-derived DAG, server-classified node value, error, provenance, and exact function-applied runnable reads validate resource reachability in that revision. |
| `/dags/{dag_id}/dashboards`, `/dags/{dag_id}/dashboard`, `/dags/{dag_id}/dashboard/refresh` | Compatible custom-dashboard metadata, lazy cached or rendered declarative results, and explicit cache-bypassing refresh for one revision-reachable committed DAG. Refresh requires JSON and replaces a cache entry only after successful rendering. |
| `/refs` | Required `project` and `revision` scope; selected-commit and checkout labels plus grouped local, fetched tracking, bounded live main-remote, and dependency branch and tag comparisons. Unmaterialized live tips remain visible but cannot select or fetch a revision. |
| `/runtimes` | Open runtime summaries and mutable DAG structure. |
| `/live-indexes/{id}` | Partial-DAG link, root execution when available, nested reachable lineage, logs, resources, safe identifiers, and bounded diagnostics. |
| `/executions/graph`, `/executions/{id}` | Root-selected lineage, execution records, lifecycle, launch state, and resolved runnable/resource projections. |
| `/executions/{id}/script`, `/function-dags/{dag_id}/script`, `/nodes/{node_id}/value/script`, `/executions/{id}/logs/{stdout\|stderr}`, `/function-dags/{dag_id}/logs/{stdout\|stderr}` | Bounded rendered source derived from trusted execution, revision-scoped function-context argv zero, or revision-scoped node value state; plus cursor-based log chunks resolved from execution identity or a function DAG's trusted persisted cache key, with origin and truncation metadata. Callers cannot supply an arbitrary script URI. |
| `/search` | Bounded global ref and identifier search for command-palette navigation, returning project-scoped canonical hrefs with concrete commit IDs. |
| `/events`, `/executions/{id}/events`, `/executions/{id}/logs/{stream}/events`, `/function-dags/{dag_id}/logs/{stream}/events` | Server-Sent Events for repository, selected-execution, cancellation, and execution- or function-DAG-resolved log changes, with keepalive and reconnect support. |

List routes accept an opaque `cursor` and a server-capped `limit`. Graph and
detail routes validate registered project, revision, and DaggerML refs before
reading. Unknown or malformed values return stable validation or not-found
errors without exposing internal paths or tracebacks; a missing concrete
revision never falls back to `HEAD`.

`/status` accepts `project_cursor`, `live_cursor`, and `commit_cursor`. An
initial request snapshots and returns a page from all collections. A
continuation returns pages only for supplied cursors, rejects mixed-snapshot
cursors, and asks the client to restart after snapshot expiry. Aggregate routes
accept no project path; their path values come only from the registered-project
configuration.

### Cancellation

Cancellation is the API's only repository or execution mutation. Custom
dashboard refresh mutates only the bounded local derived-result cache. A client first
requests `POST /api/v1/executions/{id}/cancel-confirmation`, which validates the
target and returns a 60-second, single-use nonce associated with that execution.
It then sends:

```http
POST /api/v1/executions/{id}/cancel
Authorization: Bearer <token>  # only for --allow-remote
Content-Type: application/json

{"mode": "full", "nonce": "..."}
```

The server rejects a missing, expired, reused, or differently scoped nonce.
Only `mode = "full"` is accepted by the dashboard. Acceptance records a
dashboard audit event, plans cancellation once through the existing runtime
cancellation API, and starts a bounded background drive loop. Repeated client
requests or server drive iterations must be safe when planning already occurred,
the execution became terminal, or another runtime advanced cancellation.

The drive loop stops when the execution reaches a terminal cancellation state,
the configured timeout expires, or an actionable error occurs. Progress and
failure are reported through execution reads and events. The dashboard does not
write lifecycle values itself, call executors directly, or make adapter results
authoritative; existing execution coordination remains the sole lifecycle
authority.

### Redaction and trusted reads

Before serialization, the server recursively removes credentials, tokens,
authorization and cookie values, environment values, secret-like configuration
keys, and presigned URL query parameters. It may expose a sanitized URI
consisting of scheme, host/bucket, and path. Redaction applies to known response
models, generic extension metadata, errors, logs, and event payloads.

Local script and log reads are allowed only for canonical paths derived from
trusted persisted execution state and contained by the expected DaggerML
execution roots. S3 script and log reads are allowed only for configured
DaggerML buckets and keys derived from persisted state. Path traversal,
symlink escape, arbitrary user-supplied paths, and arbitrary URL fetching are
rejected.

CloudWatch is the dashboard's only log backend. It uses log group `dml` and
streams `/run/{cache_key}/{stdout|stderr}` when a trusted persisted cache key
belongs to the selected execution or function DAG. Missing log identity,
CloudWatch configuration, streams, or access produce bounded unavailable
evidence; the dashboard never reads executor-local logs. All reads enforce byte,
line, and request-duration bounds.

### Availability and compatibility

Local repository views remain usable when no remote is configured. Remote,
CloudWatch-log, and persisted-runnable panels identify missing optional
dependencies and permissions without
converting the whole application into an error state. The server closes
repository handles and background tasks on shutdown.

`/api/v1` is versioned independently of the public Python API. Clients must
ignore unspecified additive fields. This internal v0 dashboard does not retain
old route or response-shape compatibility when its scope model changes.
Persisted DaggerML object and execution schemas are not changed for the
dashboard.

## References

- [System overview](system-overview.md)
- [DAG storage and types](dag-storage-and-types.md)
- [Execution and runtime state](execution-and-runtime-state.md)
- [Remotes and sync](remotes-and-sync.md)
- [Public API and CLI](public-api-and-cli.md)
- [Runtime state reference](../../use/reference/runtime-state.md)
- [Built-in integrations](../../extend/reference/built-in-integrations.md)
- [Sharp bits and security](../../sharp-bits-and-security.md)
