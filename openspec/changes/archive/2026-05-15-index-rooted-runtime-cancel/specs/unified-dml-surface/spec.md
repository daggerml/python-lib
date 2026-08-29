## MODIFIED Requirements

### Requirement: Shared `Dml` exposes the fixed method namespaces
The shared `Dml` class SHALL expose this caller-facing method surface:

- top level: `status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, `revert`
- `dag`: `list`, `get`, `checkout`, `delete`
- `admin.index`: `list`, `get`, `delete`
- `admin.cache`: `invalidate`
- `admin.remote`: `list`, `gc`
- `admin`: `gc`
- `runtime`: `create`, `describe`, `put_literal`, `put_import`, `start_fn`, `cancel`, `commit`
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

#### Scenario: Exact subsystem objects are grouped under ops
- **WHEN** a caller needs direct exact-input subsystem behavior such as `CommitOps`, `HeadOps`, or `IndexOps`
- **THEN** the shared `Dml` exposes those objects under `dml.ops.*` rather than as direct top-level `Dml` attributes
