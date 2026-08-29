## MODIFIED Requirements

### Requirement: Shared `Dml` exposes the fixed method namespaces
The shared `Dml` SHALL expose repository methods including `status`, `show`, `log`, `diff`, `checkout`, `fetch`, `pull`, `push`, `merge`, `rebase`, `revert`, and `gc`; dependency lifecycle under `dep.add|list|delete`; cache control under `cache.get|invalidate`; and the existing `branch`, `tag`, `dag`, `admin`, `runtime`, `config`, and `ops` namespaces. The shared surface SHALL NOT expose an `admin.remote` namespace or admin-owned cache and GC entrypoints.

- `branch`: `list`, `create`, `set_upstream`, `get_upstream`, `move`, `rename`, `delete`
- `tag`: `list`, `create`, `delete`
- `cache`: `get`, `invalidate`
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

### Requirement: `Dml` delegates repository behavior to the relevant ops classes
The shared `Dml` class SHALL orchestrate workflows by delegating repository actions to the relevant subsystem ops classes rather than re-implementing those mechanics inline. Module-level helper functions in `daggerml._internal.dml` SHALL construct the owning concrete ops classes directly and SHALL NOT route calls through a facade object or string-dispatch proxy layer.

#### Scenario: Commit-oriented workflow delegates to CommitOps
- **WHEN** a caller invokes `dml.show`, `dml.log`, `dml.diff`, `dml.merge`, or `dml.revert`
- **THEN** `Dml` delegates the relevant repository operations to `CommitOps` after preparing resolved inputs

#### Scenario: Runtime workflow delegates to IndexOps
- **WHEN** a caller invokes `dml.runtime.create`, `dml.runtime.put_literal`, `dml.runtime.start_fn`, or `dml.runtime.commit`
- **THEN** `Dml` delegates the relevant repository operations to `IndexOps` after preparing resolved inputs

#### Scenario: Runtime workflow passes explicit execution identity to IndexOps
- **WHEN** a shared `Dml` runtime workflow needs execution-aware behavior such as runnable DAG publication or nested execution lineage
- **THEN** the `Dml` runtime layer passes explicit execution identity into `IndexOps`
- **AND** `IndexOps` does not read that identity from a process-local ambient execution context

#### Scenario: Runtime start_fn falls back to root index identity
- **WHEN** `dml.runtime.start_fn(index_id, ...)` runs without resolved `config.execution.id`
- **THEN** the runtime layer passes `caller_execution_id = index_id`
- **AND** `IndexOps.start_fn` treats that root execution record as the caller identity

#### Scenario: Cache workflows delegate to execution and remote state owners
- **WHEN** a caller invokes `dml.cache.get` or `dml.cache.invalidate`
- **THEN** `Dml` delegates cache lookup or invalidation to the existing remote or execution-state owner after preparing exact cache-key inputs

#### Scenario: GC delegates according to selected source
- **WHEN** a caller invokes `dml.gc(remote=False)` or `dml.gc(remote=True)`
- **THEN** `Dml` delegates to local database GC or configured remote GC respectively

#### Scenario: Helper construction instantiates concrete ops directly
- **WHEN** a shared `Dml` workflow needs an ops object such as `CommitOps`, `HeadOps`, `IndexOps`, or `RemoteOps`
- **THEN** the helper logic in `daggerml._internal.dml` constructs that concrete ops class directly against the active DB handle
- **AND** it does not dispatch through a `DmlOps` facade or `_OpsProxy`-style string factory

## ADDED Requirements

### Requirement: Shared `Dml` SHALL expose direct cache control
The shared `Dml` surface SHALL expose `dml.cache.get(cache_key: str) -> Ref | None` and `dml.cache.invalidate(*cache_keys: str) -> InvalidationResponse`. Cache lookup SHALL return the cached DAG ref when present and `None` when absent. Cache invalidation SHALL preserve the existing execution-graph invalidation behavior and SHALL require one or more exact string cache keys.

#### Scenario: Cache get returns a cached DAG ref
- **WHEN** cache key `ck1` names a current cached DAG
- **THEN** `dml.cache.get("ck1")` returns that DAG `Ref`

#### Scenario: Cache get preserves absence
- **WHEN** no current cache ref exists for `ck1`
- **THEN** `dml.cache.get("ck1")` returns `None`

#### Scenario: Cache invalidate accepts exact keys
- **WHEN** a caller invokes `dml.cache.invalidate("ck1", "ck2")`
- **THEN** the existing invalidation workflow receives exactly those cache keys

#### Scenario: Cache invalidate requires at least one key
- **WHEN** a caller invokes `dml.cache.invalidate()` without a cache key
- **THEN** the call fails before running invalidation

### Requirement: Shared `Dml` SHALL expose source-selectable garbage collection
The shared `Dml` surface SHALL expose `gc(*, remote: bool = False) -> LocalGCSummary | RemoteGCSummary`. With `remote=False`, GC SHALL collect unreachable objects from the local repository and return `LocalGCSummary`. With `remote=True`, GC SHALL require configured `remote.root`, collect that endpoint's remote CAS/ref maintenance scope, and return `RemoteGCSummary`. The method SHALL NOT accept a dependency selector.

`LocalGCSummary` SHALL contain `deleted: dict[str, int]`, `ref-enumeration-time: int`, and `gc-time: int`.

`RemoteGCSummary` SHALL contain `tombstones-deleted: int`, `cas-deleted: int`, `cas-retained: int`, `total-refs: int`, `gc-time: int`, `ref-enumeration-time: int`, and `cas-enumeration-time: int`.

#### Scenario: GC defaults to local collection
- **WHEN** a caller invokes `dml.gc()`
- **THEN** local GC runs and returns `LocalGCSummary`
- **AND** no remote capability is required or accessed

#### Scenario: Remote flag selects remote collection
- **WHEN** a caller invokes `dml.gc(remote=True)` with configured `remote.root`
- **THEN** remote GC runs against that endpoint and returns `RemoteGCSummary`

#### Scenario: Remote GC requires remote root
- **WHEN** a caller invokes `dml.gc(remote=True)` without configured `remote.root`
- **THEN** the call fails with the established remote configuration error before maintenance begins

#### Scenario: Dependency GC is unavailable
- **WHEN** a caller inspects or invokes `dml.gc`
- **THEN** no dependency selector is exposed or accepted
