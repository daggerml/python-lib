## MODIFIED Requirements

### Requirement: Shared `Dml` class and public namespaces SHALL expose the supported workflow surface
The shared `Dml` class SHALL expose this caller-facing method surface:

- top level: `status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, `revert`
- `dag`: `list`, `get`, `checkout`, `delete`
- `admin.index`: `list`, `get`, `delete`
- `admin.cache`: `invalidate`
- `admin.remote`: `list`, `gc`
- `admin`: `gc`
- `runtime`: `create`, `describe`, `read_execution_record`, `describe_graph`, `put_literal`, `put_import`, `start_fn`, `cancel`, `commit`
- `config`: `get`, `set`, `show`
- `ops`: `commit`, `head`, `dag`, `node`, `index`, `cache`, `remote`, `gc`, `config`

#### Scenario: Top-level repository methods are present
- **WHEN** a caller inspects the shared `Dml` class
- **THEN** the repository porcelain workflows are available on the top level rather than through raw subsystem factories

#### Scenario: DAG, admin, runtime, and config methods remain namespaced
- **WHEN** a caller needs DAG inspection, admin maintenance, runtime staging behavior, or config access
- **THEN** the shared `Dml` exposes those methods under `dag`, `admin`, `runtime`, and `config` namespaces respectively

#### Scenario: Runtime namespace exposes describe_graph
- **WHEN** a caller needs execution-lineage inspection rooted at one or more runtime executions
- **THEN** the shared `Dml` exposes that workflow as `dml.runtime.describe_graph(...)`

#### Scenario: Exact subsystem objects are grouped under ops
- **WHEN** a caller needs direct exact-input subsystem behavior such as `CommitOps`, `HeadOps`, or `IndexOps`
- **THEN** the shared `Dml` exposes those objects under `dml.ops.*` rather than as direct top-level `Dml` attributes

## ADDED Requirements

### Requirement: Shared `Dml` runtime namespace SHALL expose direct execution-record inspection
The shared `Dml` runtime namespace SHALL expose `read_execution_record(execution: Ref | str)` for direct execution-state inspection. The runtime namespace SHALL normalize either input form to an execution-id string before delegation. When the read succeeds, the method SHALL return the raw execution record typed dict without reshaping or enrichment.

#### Scenario: Runtime namespace reads an execution record from a runtime ref
- **WHEN** a caller invokes `dml.runtime.read_execution_record(idx1)`
- **THEN** the runtime namespace SHALL normalize `idx1` to `idx1.id()`
- **AND** it SHALL delegate the read using that execution id

#### Scenario: Runtime namespace reads an execution record from an execution id string
- **WHEN** a caller invokes `dml.runtime.read_execution_record("exec-2")`
- **THEN** the runtime namespace SHALL use `"exec-2"` as the delegated execution id without additional reshaping

#### Scenario: Runtime namespace preserves underlying read failures
- **WHEN** a caller invokes `dml.runtime.read_execution_record("missing")`
- **AND** no execution record exists for `"missing"`
- **THEN** the runtime namespace SHALL surface the same missing-record failure from the underlying execution-state reader
