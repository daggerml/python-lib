## MODIFIED Requirements

### Requirement: Shared `Dml` exposes the fixed method namespaces
The shared `Dml` class SHALL expose this caller-facing method surface:

- top level: `status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, `revert`
- `dag`: `list`, `get`, `checkout`, `delete`
- `admin.index`: `list`, `get`, `delete`
- `admin.cache`: `invalidate`
- `admin.remote`: `list`, `gc`
- `admin`: `gc`
- `runtime`: `create`, `describe`, `describe_graph`, `put_literal`, `put_import`, `start_fn`, `cancel`, `commit`
- `config`: `get`, `set`, `show`
- `ops`: `commit`, `head`, `dag`, `node`, `index`, `cache`, `remote`, `gc`, `config`

#### Scenario: Top-level repository methods are present
- **WHEN** a caller inspects the shared `Dml` class
- **THEN** the repository porcelain workflows are available on the top level rather than through raw subsystem factories

#### Scenario: DAG, admin, runtime, and config methods remain namespaced
- **WHEN** a caller needs DAG inspection, admin maintenance, runtime staging behavior, or config access
- **THEN** the shared `Dml` exposes those methods under `dag`, `admin`, `runtime`, and `config` namespaces respectively

#### Scenario: Runtime namespace exposes cancel
- **WHEN** a caller needs to cancel work rooted at an index
- **THEN** the shared `Dml` exposes that workflow as `dml.runtime.cancel(index_id)`

#### Scenario: Runtime namespace exposes describe_graph
- **WHEN** a caller needs execution-lineage inspection rooted at one or more runtime executions
- **THEN** the shared `Dml` exposes that workflow as `dml.runtime.describe_graph(...)`

#### Scenario: Exact subsystem objects are grouped under ops
- **WHEN** a caller needs direct exact-input subsystem behavior such as `CommitOps`, `HeadOps`, or `IndexOps`
- **THEN** the shared `Dml` exposes those objects under `dml.ops.*` rather than as direct top-level `Dml` attributes

## ADDED Requirements

### Requirement: Shared `Dml` runtime namespace SHALL normalize roots for execution graph inspection
The shared `Dml` runtime namespace SHALL expose `describe_graph(*roots: Ref | str)` for execution-lineage inspection. If the caller provides no roots, the runtime namespace SHALL use all currently open local runtime indexes as roots. Before delegating to execution-state graph extraction, the runtime namespace SHALL normalize the selected roots to execution-id strings.

#### Scenario: Explicit roots are normalized and delegated
- **WHEN** a caller invokes `dml.runtime.describe_graph(idx1, "exec-2")`
- **THEN** the runtime namespace SHALL normalize those roots to execution-id strings
- **AND** it SHALL delegate the graph extraction using only those normalized root ids

#### Scenario: Empty input defaults to open local indexes
- **WHEN** a caller invokes `dml.runtime.describe_graph()` with no explicit roots
- **THEN** the runtime namespace SHALL read the currently open local runtime indexes
- **AND** it SHALL use those index ids as the root execution ids for graph extraction
