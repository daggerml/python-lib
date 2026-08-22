## MODIFIED Requirements

### Requirement: Shared `Dml` exact DB object contracts use `Ref`
The shared `Dml` surface SHALL require `Ref` objects for exact DB-backed object inputs and for runtime or execution identity inputs, including execution roots supplied to cache invalidation, and SHALL return `Ref` objects as canonical runtime and DB identity. Revision selectors, DAG and node names, branch and tag names, endpoint roots, dependency names, cache keys, and lower-level execution-state IDs SHALL remain strings. Public methods accepting execution `Ref` values SHALL convert them to string execution IDs only when delegating below the shared surface and SHALL NOT convert caller strings into refs.

#### Scenario: Exact DAG access requires `Ref`
- **WHEN** a caller invokes a `Dml` method whose contract is to dereference an exact DAG object
- **THEN** the method requires a `Ref`
- **AND** it does not accept a plain `"dag:..."` string as a substitute

#### Scenario: Exact node access requires `Ref`
- **WHEN** a caller invokes a `Dml` method whose contract is to dereference an exact node object
- **THEN** the method requires a `Ref`
- **AND** it does not accept a plain `"node:..."` string as a substitute

#### Scenario: Runtime execution identity requires `Ref`
- **WHEN** a caller supplies an execution identity to `Dml.runtime.create`, `read_launch_state`, `read_execution_record`, `describe_graph`, or `cancel`
- **THEN** the shared `Dml` method requires an `index` or otherwise method-supported runtime `Ref`
- **AND** it does not accept a bare execution ID or plain `"index:..."` string as a substitute

#### Scenario: Cache invalidation execution identity requires Ref
- **WHEN** a caller supplies an execution identity to `Dml.cache.invalidate`
- **THEN** the method requires an `index` or `frozenindex` `Ref`
- **AND** it does not accept a cache key, bare execution ID, or plain ref-shaped string

#### Scenario: Non-identity selectors remain strings
- **WHEN** a caller provides revision text, a symbolic object name, an endpoint root, a dependency name, or a cache key
- **THEN** the shared `Dml` surface continues to accept that value as a string

#### Scenario: Lower-level execution state remains string addressed
- **WHEN** a public `Dml` method delegates a validated runtime ref to `IndexOps` or `ExecutionState`
- **THEN** it passes the ref's exact id as the lower-level execution ID string
- **AND** the lower-level persisted and protocol representation does not change

#### Scenario: DB-backed payloads use ref identity
- **WHEN** a shared `Dml` payload includes the identity of a commit, DAG, node, runtime, or other DB-backed object
- **THEN** that identity is represented by `Ref`
- **AND** the payload does not duplicate the same DB identity as a separate raw id string

### Requirement: Shared `Dml` exposes the fixed method namespaces
The shared `Dml` SHALL expose repository methods including `status`, `show`, `log`, `diff`, `checkout`, `fetch`, `pull`, `push`, `merge`, `rebase`, `revert`, and `gc`; dependency lifecycle under `dep.add|list|delete`; cache control under `cache.get|describe|invalidate`; and the existing `branch`, `tag`, `dag`, `admin`, `runtime`, `config`, and `ops` namespaces. The shared surface SHALL NOT expose an `admin.remote` namespace or admin-owned cache and GC entrypoints.

- `branch`: `list`, `create`, `set_upstream`, `get_upstream`, `move`, `rename`, `delete`
- `tag`: `list`, `create`, `delete`
- `cache`: `get`, `describe`, `invalidate`
- `dag`: `list`, `get`, `checkout`, `delete`
- `admin.index`: `list`, `get`, `delete`
- `admin`: `agent_skill`
- `runtime`: `create`, `describe`, `read_execution_record`, `read_launch_state`, `describe_graph`, `put_literal`, `put_import`, `start_fn`, `cancel`, `commit`
- `config`: `get`, `set`, `show`
- `ops`: `commit`, `head`, `dag`, `node`, `index`, `cache`, `remote`, `gc`, `config`

#### Scenario: Repository and dependency methods are present
- **WHEN** a caller inspects the shared `Dml` class
- **THEN** repository porcelain and `gc` are top-level, dependency endpoint lifecycle is under `dep`, and cache control is under `cache`

#### Scenario: DAG, runtime, config, and remaining admin methods remain namespaced
- **WHEN** a caller needs DAG inspection, runtime behavior, configuration, or remaining administration behavior
- **THEN** the shared `Dml` exposes those methods under `dag`, `runtime`, `config`, and `admin` respectively

#### Scenario: Runtime namespace exposes describe_graph
- **WHEN** a caller needs execution-lineage inspection rooted at one or more runtime executions
- **THEN** the shared `Dml` exposes that workflow as `dml.runtime.describe_graph(...)`

#### Scenario: Runtime namespace exposes launch-state inspection
- **WHEN** a caller needs the persisted executor resume state for one execution
- **THEN** the shared `Dml` exposes that workflow as `dml.runtime.read_launch_state(...)`

#### Scenario: Branch namespace exposes arbitrary upstream inspection
- **WHEN** a caller needs the configured upstream of a local branch other than the attached branch
- **THEN** the shared `Dml` exposes that workflow as `dml.branch.get_upstream(...)`

#### Scenario: Remote administration namespace is absent
- **WHEN** a caller inspects `dml.admin`
- **THEN** no `remote` namespace, cache method, or GC method is exposed there

#### Scenario: Exact subsystem objects are grouped under ops
- **WHEN** a caller needs direct exact-input subsystem behavior such as `CommitOps`, `HeadOps`, or `IndexOps`
- **THEN** the shared `Dml` exposes those objects under `dml.ops.*` rather than as direct top-level `Dml` attributes

### Requirement: Shared `Dml` SHALL expose direct cache control
The shared `Dml` surface SHALL expose `dml.cache.get(cache_key: str) -> Ref | None`, `dml.cache.describe(cache_key: str) -> CacheDescription | None`, and `dml.cache.invalidate(*executions: Ref) -> InvalidationResponse`. `CacheDescription` SHALL contain `execution: Ref`, `dag: Ref | None`, and `lifecycle: EXECUTION_LIFECYCLES`. Cache lookup SHALL return the reusable cached DAG when present. Cache description SHALL report the exact execution named by the cache pointer and SHALL include its DAG only when that execution is an unmarked reusable terminal result. Cache invalidation SHALL require one or more exact execution refs.

#### Scenario: Cache get returns a cached DAG ref
- **WHEN** cache key `ck1` names a current reusable terminal DAG
- **THEN** `dml.cache.get("ck1")` returns that DAG `Ref`

#### Scenario: Cache get preserves absence
- **WHEN** no reusable terminal result exists for `ck1`
- **THEN** `dml.cache.get("ck1")` returns `None`

#### Scenario: Cache describe reports running execution
- **WHEN** `cache/ck1` names running execution `e1`
- **THEN** `dml.cache.describe("ck1")` returns execution `Ref("index:e1")`, lifecycle `running`, and `dag = None`

#### Scenario: Cache describe reports reusable terminal identities
- **WHEN** `cache/ck1` names unmarked terminal execution `e1` with result `dag:d1`
- **THEN** `dml.cache.describe("ck1")` returns execution `Ref("index:e1")`, the exact DAG ref `Ref("dag:d1")`, and the terminal lifecycle

#### Scenario: Cache describe rejects marked result reuse
- **WHEN** `cache/ck1` names a canceled or invalidated terminal execution
- **THEN** `dml.cache.describe("ck1")` reports that execution and lifecycle with `dag = None`

#### Scenario: Cache describe preserves absence
- **WHEN** `cache/ck1` is absent or names a missing execution record
- **THEN** `dml.cache.describe("ck1")` returns `None`

#### Scenario: Cache describe retains selected execution identity during rebound
- **WHEN** cache description reads `e1` from `cache/ck1` and that pointer is rebound to `e2` before the execution record is read
- **THEN** the operation SHALL describe `e1` if its execution record exists
- **AND** it SHALL NOT substitute `e2`

#### Scenario: Cache invalidate accepts exact executions
- **WHEN** a caller invokes `dml.cache.invalidate(Ref("index:e1"), Ref("frozenindex:e2"))`
- **THEN** the invalidation workflow receives exactly execution IDs `e1` and `e2`

#### Scenario: Cache invalidate requires at least one execution
- **WHEN** a caller invokes `dml.cache.invalidate()` without an execution ref
- **THEN** the call fails before running invalidation

### Requirement: Shared `Dml` exposes runtime cancel with explicit mode selection
The shared `Dml` runtime namespace SHALL expose cancellation as `dml.runtime.cancel(execution: Ref, mode="full")`. It SHALL validate the runtime ref, delegate its id to execution state, and preserve the supplied `Ref` as the requested identity in the returned summary. `mode` SHALL accept `"full"` and `"drive"`.

- `mode = "full"` SHALL run the full root-facing cancellation workflow.
- `mode = "drive"` SHALL run only the cancellation driver needed by an already-canceling execution.

#### Scenario: Runtime namespace exposes full cancellation by default
- **WHEN** a caller invokes `dml.runtime.cancel(Ref("index:idx1"))` without an explicit mode
- **THEN** the runtime namespace SHALL use `mode = "full"`
- **AND** it SHALL delegate execution ID `idx1`

#### Scenario: Runtime namespace exposes drive mode for internal cancellation progress
- **WHEN** a caller invokes `dml.runtime.cancel(Ref("index:e1"), mode="drive")`
- **THEN** the runtime namespace SHALL expose the driver-only cancellation behavior for execution `e1`

#### Scenario: Runtime cancellation rejects string identity
- **WHEN** a Python caller invokes `dml.runtime.cancel("e1")` or `dml.runtime.cancel("index:e1")`
- **THEN** the runtime namespace SHALL fail before cancellation state is changed
