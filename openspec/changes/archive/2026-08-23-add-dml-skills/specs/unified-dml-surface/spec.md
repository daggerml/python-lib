## MODIFIED Requirements

### Requirement: Shared `Dml` exposes the fixed method namespaces
The shared `Dml` SHALL expose repository methods including `status`, `show`, `log`, `diff`, `checkout`, `fetch`, `pull`, `push`, `merge`, `rebase`, `revert`, and `gc`; dependency lifecycle under `dep.add|list|delete`; cache control under `cache.get|describe|invalidate`; and the existing `branch`, `tag`, `dag`, `skills`, `runtime`, `config`, and `ops` namespaces. The shared surface SHALL NOT expose an `admin` namespace, an `admin.remote` namespace, or admin-owned cache and GC entrypoints.

- `branch`: `list`, `create`, `set_upstream`, `get_upstream`, `move`, `rename`, `delete`
- `tag`: `list`, `create`, `delete`
- `cache`: `get`, `describe`, `invalidate`
- `dag`: `list`, `get`, `checkout`, `delete`
- `skills`: `authoring`, `repository`, `inspection`
- `runtime`: `create`, `describe`, `read_execution_record`, `read_launch_state`, `describe_graph`, `put_literal`, `put_import`, `start_fn`, `cancel`, `commit`
- `config`: `get`, `set`, `show`
- `ops`: `commit`, `head`, `dag`, `node`, `index`, `cache`, `remote`, `gc`, `config`

#### Scenario: Repository and dependency methods are present
- **WHEN** a caller inspects the shared `Dml` class
- **THEN** repository porcelain and `gc` are top-level, dependency endpoint lifecycle is under `dep`, and cache control is under `cache`

#### Scenario: DAG, runtime, configuration, and skills methods are namespaced
- **WHEN** a caller needs DAG inspection, runtime behavior, configuration, or bundled agent guidance
- **THEN** the shared `Dml` exposes those methods under `dag`, `runtime`, `config`, and `skills` respectively

#### Scenario: Runtime namespace exposes describe_graph
- **WHEN** a caller needs execution-lineage inspection rooted at one or more runtime executions
- **THEN** the shared `Dml` exposes that workflow as `dml.runtime.describe_graph(...)`

#### Scenario: Runtime namespace exposes launch-state inspection
- **WHEN** a caller needs the persisted executor resume state for one execution
- **THEN** the shared `Dml` exposes that workflow as `dml.runtime.read_launch_state(...)`

#### Scenario: Branch namespace exposes arbitrary upstream inspection
- **WHEN** a caller needs the configured upstream of a local branch other than the attached branch
- **THEN** the shared `Dml` exposes that workflow as `dml.branch.get_upstream(...)`

#### Scenario: Administrative namespace is absent
- **WHEN** a caller attempts to access `dml.admin`
- **THEN** the shared `Dml` surface does not provide that namespace
