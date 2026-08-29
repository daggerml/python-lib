## MODIFIED Requirements

### Requirement: Shared `Dml` exposes the fixed method namespaces
The shared `Dml` class SHALL expose this caller-facing method surface:

- top level: `status`, `show`, `log`, `diff`, `checkout`, `fetch`, `pull`, `push`, `merge`, `revert`
- `branch`: `list`, `create`, `move`, `rename`, `delete`
- `tag`: `list`, `create`, `delete`
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

#### Scenario: Branch and tag lifecycle methods are namespaced
- **WHEN** a caller needs local branch or local tag lifecycle behavior
- **THEN** the shared `Dml` exposes those workflows under `dml.branch` and `dml.tag` rather than as one top-level `branch` method

#### Scenario: DAG, admin, runtime, and config methods remain namespaced
- **WHEN** a caller needs DAG inspection, admin maintenance, runtime staging behavior, or config access
- **THEN** the shared `Dml` exposes those methods under `dag`, `admin`, `runtime`, and `config` namespaces respectively

#### Scenario: Runtime namespace exposes cancel
- **WHEN** a caller needs to cancel work rooted at an index
- **THEN** the shared `Dml` exposes that workflow as `dml.runtime.cancel(index_id)`

#### Scenario: Exact subsystem objects are grouped under ops
- **WHEN** a caller needs direct exact-input subsystem behavior such as `CommitOps`, `HeadOps`, or `IndexOps`
- **THEN** the shared `Dml` exposes those objects under `dml.ops.*` rather than as direct top-level `Dml` attributes

### Requirement: `Dml` is the only fuzzy-selector boundary
The shared `Dml` class SHALL accept fuzzy selector strings only for workflows whose contract is lookup or repository navigation, and it SHALL require exact `Ref` objects for workflows whose contract is direct dereference or mutation of DB-backed objects.

#### Scenario: Revision selector resolves inside Dml
- **WHEN** a caller passes a supported revision string such as `HEAD~1`, `@v1`, or `dml://alice/demo#main` to a shared `Dml` repository method
- **THEN** the `Dml` method resolves it through the selector-resolution submodule and lower-level ops receive only exact values

#### Scenario: Unsupported named-remote grammar is rejected at Dml boundary
- **WHEN** a caller passes a named-remote selector such as `origin/main`
- **THEN** the shared `Dml` method fails rather than inventing named-remote support
