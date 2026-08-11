## Purpose
Define the canonical shared `_internal.Dml` boundary, its fixed caller-facing surface, and the delegation constraints between that surface and lower-level ops/config-resolution helpers.

## Requirements

### Requirement: Revision-consuming Dml methods expose exact source signatures
The shared public surface SHALL expose `show`, `log`, `diff`, and `rev_parse` with keyword-only mutually exclusive `remote: bool = False` and `dep: str | None = None`; `checkout`, `merge`, `rebase`, `revert`, `branch.create`, and `tag.create` with keyword-only `remote: bool = False` only; `dag.checkout` with both mutually exclusive source selectors; `branch.list` and `tag.list` with both independent source selectors; and `fetch(revision: str | None = None, /, *, dep: str | None = None)`.

#### Scenario: Inspection exposes both source selectors
- **WHEN** callers inspect show, log, diff, rev-parse, or DAG checkout
- **THEN** their signatures expose keyword-only `remote` and `dep`

#### Scenario: History mutation exposes remote only
- **WHEN** callers inspect repository checkout, merge, rebase, revert, branch creation, or tag creation
- **THEN** their signatures expose keyword-only `remote` and no dependency selector

#### Scenario: Fetch selects dependency separately from revision
- **WHEN** callers inspect fetch
- **THEN** its positional-only optional revision contains only a branch or `@tag` and optional keyword-only `dep` selects the endpoint

#### Scenario: Revision source mutual exclusion is validated by Dml
- **WHEN** a revision-consuming public method receives `remote=True` with non-null `dep`
- **THEN** the shared method fails before revision lookup, regardless of whether it was called from Python or the generated CLI

#### Scenario: Ref listing source selectors remain independent
- **WHEN** `branch.list` or `tag.list` receives `remote=True` with non-null `dep`
- **THEN** the shared method accepts both selectors and delegates dependency-endpoint listing

### Requirement: Dml construction owns remote-root configuration
`Dml`, `Dml.init`, and `Dml.clone` SHALL accept `remote_root` through the shared configuration surface. Resolved `remote.root` SHALL affect a method only when that method performs remote-backed synchronization, storage, cache, execution, or administration behavior. Local-only methods SHALL not add special handling for this or other unrelated construction arguments.

#### Scenario: Local method ignores unused remote capability
- **WHEN** a `Dml` instance has `remote_root` and executes a method requiring no remote behavior
- **THEN** that method behaves through its normal local path without remote access

#### Scenario: Remote-backed method receives resolved root
- **WHEN** the same instance executes remote-backed behavior
- **THEN** the shared orchestration passes resolved `remote.root` to the relevant remote-aware component

#### Scenario: CLI derives constructor option behavior
- **WHEN** public construction parameters generate CLI options
- **THEN** their effects follow shared `Dml` configuration and method usage without command-specific logic in `daggerml._cli`

### Requirement: One shared `_internal.Dml` class is the canonical orchestration boundary
The system SHALL expose one shared `_internal.Dml` class for repository, DAG, admin, and runtime workflows.

#### Scenario: CLI delegates through shared Dml
- **WHEN** a CLI command executes a repository, DAG, admin, or runtime workflow
- **THEN** the handler instantiates or receives a `Dml` instance and delegates through that class instead of orchestrating lower-level ops classes directly

#### Scenario: API wrappers delegate through shared Dml
- **WHEN** `Dag` or `Node` wrappers need repository/runtime behavior
- **THEN** they delegate through the shared internal `Dml` implementation, whether by direct use or by a thin compatibility wrapper in `daggerml.api`

### Requirement: Public node wrappers SHALL expose provenance traversal through `context(root=...)`
The public `daggerml.api` node-wrapper surface SHALL expose `context(root: bool = True)` as the provenance-oriented way to resolve the DAG behind an imported, function-produced, builtin-derived, or projected value.

#### Scenario: Public node wrapper resolves nearest context
- **WHEN** a caller invokes `node.context(root=False)` on a public `Node`
- **THEN** the wrapper uses the shared API/runtime inspection surfaces to resolve the nearest non-builtin import/function DAG context for that value

#### Scenario: Public node wrapper resolves rooted context
- **WHEN** a caller invokes `node.context(root=True)` on a public `Node`
- **THEN** the wrapper recursively follows provenance until it no longer crosses a non-builtin import/function boundary and returns the resulting DAG

### Requirement: Public committed collection reads SHALL expose `Projection` wrappers for interrogation
The public `daggerml.api` collection-wrapper surface SHALL allow committed dict/list reads to return `Projection` wrappers for ex-post interrogation without mutating repository state.

#### Scenario: Committed collection read returns projection wrapper
- **WHEN** a caller reads a projected subvalue from a committed collection-valued `Node`
- **THEN** the public API may return a `Projection` wrapper instead of a real `Node` when the selected subvalue has no standalone persisted node identity

#### Scenario: Projection remains outside mutation and execution entrypoints
- **WHEN** a caller receives a public `Projection` wrapper
- **THEN** that wrapper is limited to read-only interrogation helpers and is not accepted as a staging, mutation, or callable-runtime input

### Requirement: `Dml` delegates fuzzy and config resolution to dedicated submodules
The shared `Dml` class SHALL remain the sole caller-facing boundary for fuzzy selector and config-derived context behavior, but it SHALL farm fuzzy selector resolution to a dedicated fuzzy-resolution submodule and config-derived context lookup to a dedicated config submodule.

#### Scenario: Revision parsing delegates to fuzzy-resolution submodule
- **WHEN** a caller passes a supported revision string to a `Dml` repository method
- **THEN** `Dml` delegates the fuzzy parsing and resolution step to the fuzzy-resolution submodule before invoking lower-level ops

#### Scenario: Current head and remote context delegate to config submodule
- **WHEN** a `Dml` workflow needs current head state, default branch behavior, or remote-uri context
- **THEN** `Dml` obtains that config-derived context through the config submodule before invoking lower-level ops

### Requirement: Shared `Dml` constructor uses root runtime override inputs
The shared `Dml` constructor SHALL accept the full supported runtime configuration surface through Python-friendly keyword parameters, including project, database, remote, default, and user/config-home overrides. `Dml.init(...)` SHALL accept the same configuration kwargs plus bootstrap-only parameters. The shared surface SHALL also expose `Dml.from_config_vars(...)` for constructing `Dml` from flattened canonical config-var dictionaries.

#### Scenario: Python kwargs cover the supported config surface
- **WHEN** a caller provides explicit configuration overrides supported by the shared resolver
- **THEN** those values can be passed directly to the shared `Dml` constructor using Python-friendly parameter names

#### Scenario: Init reuses constructor config kwargs
- **WHEN** a caller provides supported configuration overrides to `Dml.init(...)`
- **THEN** the init workflow accepts the same config kwargs as `Dml.__init__` in addition to bootstrap-only args

#### Scenario: Canonical config vars use dedicated classmethod
- **WHEN** a caller already has a flattened config-var dictionary such as `{"remote.root": "s3://bucket/root"}`
- **THEN** it can construct a `Dml` instance through `Dml.from_config_vars(...)` without translating those keys to Python kwargs first

### Requirement: Shared `Dml` exact DB object contracts use `Ref`
The shared `Dml` surface SHALL require `Ref` objects for exact DB-backed object inputs and return `Ref` objects as canonical DB identity. Revision selectors, DAG and node names, branch and tag names, endpoint roots, dependency names, and runtime index IDs SHALL remain strings.

#### Scenario: Exact DAG access requires `Ref`
- **WHEN** a caller invokes a `Dml` method whose contract is to dereference an exact DAG object
- **THEN** the method requires a `Ref`
- **AND** it does not accept a plain `"dag:..."` string as a substitute

#### Scenario: Exact node access requires `Ref`
- **WHEN** a caller invokes a `Dml` method whose contract is to dereference an exact node object
- **THEN** the method requires a `Ref`
- **AND** it does not accept a plain `"node:..."` string as a substitute

#### Scenario: Non-DB selectors remain strings
- **WHEN** a caller provides revision text, a symbolic object name, an endpoint root, a dependency name, or an index ID
- **THEN** the shared `Dml` surface continues to accept that value as a string

#### Scenario: DB-backed payloads use ref identity
- **WHEN** a shared `Dml` payload includes the identity of a commit, DAG, node, or other DB-backed object
- **THEN** that identity is represented by `Ref`
- **AND** the payload does not duplicate the same DB identity as a separate raw `id` string

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

### Requirement: Shared `Dml` branch and tag listings SHALL expose source-selected commit tips
The shared `Dml` branch and tag namespaces SHALL expose `list(*, remote: bool = False, dep: str | None = None) -> list[RefListItem]`, where `RefListItem` has exact shape `{"name": str, "commit": Ref}`. The methods SHALL pass both selectors to the ref-enumeration workflow and shape its name/tip records as this item list. The public item shape is owned by this capability; source selection, tip identity, ordering, endpoint side effects, and ref validity are owned by `remote-project-refs`.

#### Scenario: Default listing selects local refs
- **WHEN** a caller invokes `dml.branch.list()` or `dml.tag.list()` without source selectors
- **THEN** the shared method delegates with `remote = False` and `dep = None` and returns the resulting item list

#### Scenario: Remote listing selects the main remote
- **WHEN** a caller invokes a listing method with `remote = True` and no dependency
- **THEN** the shared method delegates with `remote = True` and `dep = None` and returns the resulting item list

#### Scenario: Dependency listing selects fetched refs
- **WHEN** a caller invokes a listing method with `dep = "models"` and `remote = False`
- **THEN** the shared method delegates with `remote = False` and `dep = "models"` and returns the resulting item list

#### Scenario: Remote dependency listing selects the dependency endpoint
- **WHEN** a caller invokes a listing method with `remote = True` and `dep = "models"`
- **THEN** the shared method delegates with `remote = True` and `dep = "models"` and returns the resulting item list

### Requirement: Shared `Dml` runtime namespace SHALL expose launch-state inspection
The shared `Dml` runtime namespace SHALL expose `read_launch_state(execution_id: str) -> dict | None`, delegate that execution id to the execution-state launch reader, and return its result without reshaping it. Launch-state content and absence semantics are owned by `runtime-execution-records`.

#### Scenario: Runtime namespace returns persisted resume state
- **WHEN** a caller invokes `dml.runtime.read_launch_state("e1")` and launch state exists for execution `e1`
- **THEN** the method returns the persisted executor resume-state JSON object

#### Scenario: Runtime namespace preserves an absent launch state
- **WHEN** a caller invokes `dml.runtime.read_launch_state("missing")` and no launch state exists
- **THEN** the method returns `None`

### Requirement: Shared `Dml` branch namespace SHALL expose arbitrary upstream lookup
The shared `Dml` branch namespace SHALL expose `get_upstream(branch: str) -> UpstreamInfo | None`, delegate the branch name to branch upstream storage, and return its result without reshaping it. Upstream payload and failure semantics are owned by `named-remote-branch-tracking`.

#### Scenario: Upstream lookup is not limited to the attached branch
- **WHEN** local branch `feature` tracks remote-root branch `main` and a caller invokes `dml.branch.get_upstream("feature")`
- **THEN** the result identifies upstream branch `main` regardless of the current checkout

#### Scenario: Unconfigured branch has no upstream
- **WHEN** a caller invokes `dml.branch.get_upstream("feature")` and `feature` has no configured upstream
- **THEN** the method returns `None`

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

### Requirement: Shared `Dml` runtime namespace SHALL normalize roots for execution graph inspection
The shared `Dml` runtime namespace SHALL expose `describe_graph(*roots: Ref | str, visual: bool = False)` for execution-lineage inspection. If the caller provides no roots, the runtime namespace SHALL use all currently open local runtime indexes as roots. Before delegating to execution-state graph extraction, the runtime namespace SHALL normalize the selected roots to execution-id strings. When `visual` is `False`, the method SHALL return the extracted `ExecutionGraph`. When `visual` is `True`, the method SHALL render a human-friendly graph view and return `None`.

#### Scenario: Explicit roots are normalized and delegated
- **WHEN** a caller invokes `dml.runtime.describe_graph(idx1, "exec-2")`
- **THEN** the runtime namespace SHALL normalize those roots to execution-id strings
- **AND** it SHALL delegate the graph extraction using only those normalized root ids

#### Scenario: Empty input defaults to open local indexes
- **WHEN** a caller invokes `dml.runtime.describe_graph()` with no explicit roots
- **THEN** the runtime namespace SHALL read the currently open local runtime indexes
- **AND** it SHALL use those index ids as the root execution ids for graph extraction

#### Scenario: Visual mode renders instead of returning the raw graph
- **WHEN** a caller invokes `dml.runtime.describe_graph(idx1, visual=True)`
- **THEN** the runtime namespace SHALL fetch the same execution graph data it would use for raw inspection
- **AND** it SHALL render a human-friendly execution graph view
- **AND** it SHALL return `None`

### Requirement: Shared `Dml` exposes runtime cancel with explicit mode selection
The shared `Dml` runtime namespace SHALL expose cancellation as `dml.runtime.cancel(index_or_execution, mode="full")`. `mode` SHALL accept `"full"` and `"drive"`.

- `mode = "full"` SHALL run the full root-facing cancellation workflow.
- `mode = "drive"` SHALL run only the cancellation driver needed by an already-canceling execution.

#### Scenario: Runtime namespace exposes full cancellation by default
- **WHEN** a caller invokes `dml.runtime.cancel(idx1)` without an explicit mode
- **THEN** the runtime namespace SHALL use `mode = "full"`

#### Scenario: Runtime namespace exposes drive mode for internal cancellation progress
- **WHEN** a caller invokes `dml.runtime.cancel(e1, mode="drive")`
- **THEN** the runtime namespace SHALL expose the driver-only cancellation behavior for that execution

### Requirement: Shared `Dml` surface SHALL be introspection-ready
The shared `Dml` boundary and its public namespaces SHALL expose runtime documentation that explains class purpose, method behavior, and parameter meaning without changing workflow semantics, and that metadata SHALL be sufficient for generated CLI help.

#### Scenario: Namespace objects describe their purpose
- **WHEN** a caller inspects `Dml` or any namespace reachable through `dml.config`, `dml.runtime`, `dml.dag`, or `dml.admin`
- **THEN** the class exposes a docstring that describes the purpose of that boundary or namespace

#### Scenario: Public methods describe behavior
- **WHEN** a caller inspects a public top-level or namespaced `Dml` method
- **THEN** the method exposes a docstring that describes the operation behavior and any notable constraints or side effects

#### Scenario: Generated CLI help can use runtime docs
- **WHEN** the CLI generator inspects `Dml` or one of its public namespace methods
- **THEN** it can derive command descriptions and parameter help from runtime docstrings and annotation metadata without a separate command-specific help registry

### Requirement: Shared `Dml` parameters SHALL expose machine-readable help metadata
Public parameters on the shared `Dml` surface and its public namespace methods SHALL use `typing.Annotated` metadata to describe parameter meaning, while Python signature defaults remain the source of truth for default values.

#### Scenario: Parameter meaning is available from annotations
- **WHEN** a caller inspects annotations for a public `Dml` method or a public method on a `Dml` namespace object with extras included
- **THEN** the parameter annotations include `Annotated` metadata that describes what each user-facing parameter means

#### Scenario: Defaults remain in the signature
- **WHEN** a public `Dml` or namespaced method has a defaulted parameter
- **THEN** the default value remains represented by the Python signature
- **AND** the `Annotated` metadata does not become the source of truth for that default

#### Scenario: Ambiguous selector parameters may include examples
- **WHEN** a public `Dml` parameter accepts potentially confusing selector or URI forms such as revision selectors or remote project identifiers
- **THEN** the `Annotated` metadata MAY include concise examples that clarify accepted forms without redefining the underlying grammar

#### Scenario: Non-generatable CLI parameters are not part of the public method surface
- **WHEN** a public workflow depends on helper state that cannot be generated from CLI input such as an S3 client object
- **THEN** that helper state is provided through `Dml` instance construction or private instance state rather than through a public method parameter

### Requirement: `Dml` stores runtime context, S3 client state, and temporary-directory bookkeeping
The shared `Dml` class SHALL keep only `_context`, `_s3_client`, and `_tempdirs` as private instance attributes. Helper behavior that supports `Dml` public methods SHALL live in module-level functions within `daggerml._internal.dml` rather than in private `Dml` instance methods.

#### Scenario: Namespace and helper access do not require extra Dml instance fields
- **WHEN** a caller uses any public namespace on `Dml`
- **THEN** the namespace behavior is derived from `_context`, `_s3_client`, `_tempdirs`, and delegated helper logic without introducing additional private `Dml` instance attributes

#### Scenario: Dml public workflows do not depend on private helper methods
- **WHEN** a `Dml` repository, runtime, DAG, admin, or config workflow needs helper behavior such as ops dispatch, payload shaping, or revision binding
- **THEN** that helper behavior executes through module-level functions in `daggerml._internal.dml` rather than through `Dml._...` instance methods

#### Scenario: Namespace objects keep only Dml as private state
- **WHEN** a caller inspects the namespace objects exposed by `Dml`
- **THEN** each namespace object keeps only `._dml` as private instance state
- **AND** namespace helper behavior does not rely on additional private attrs or private helper methods on the namespace object

#### Scenario: Remote sync workflows reuse the Dml-owned S3 client
- **WHEN** a caller invokes `dml.fetch`, `dml.pull`, or `dml.push`
- **THEN** the workflow uses the `Dml` instance's private `_s3_client` instead of requiring a public `s3_client` method parameter

### Requirement: `Dml` is the only fuzzy-selector boundary
The shared `Dml` class SHALL accept fuzzy selector strings only for workflows whose contract is lookup or repository navigation, and it SHALL require exact `Ref` objects for workflows whose contract is direct dereference or mutation of DB-backed objects.

#### Scenario: Revision selector resolves inside Dml
- **WHEN** a caller passes a supported revision string such as `HEAD~1` to a shared `Dml` repository method
- **THEN** the `Dml` method resolves it through the fuzzy-resolution submodule and lower-level ops receive only exact values

#### Scenario: DAG-name lookup resolves inside Dml
- **WHEN** a caller passes a DAG name to a shared `Dml` lookup workflow that documents name-based selection
- **THEN** the shared `Dml` method performs that selector resolution through the fuzzy-resolution submodule and lower-level ops do not parse that caller-facing form

#### Scenario: Exact DB-object workflow rejects fuzzy string grammar
- **WHEN** a caller passes a ref-like string such as `dag:abc123`, `node-literal:abc123`, or `commit:abc123` to a shared `Dml` workflow whose contract is for an exact DB-backed object
- **THEN** the method fails rather than coercing that string into a `Ref`

#### Scenario: Unsupported fuzzy grammar is rejected at Dml boundary
- **WHEN** a caller passes a selector form that is not documented by the redesigned CLI contracts
- **THEN** the shared `Dml` method fails rather than inventing additional grammar

### Requirement: Lower-level ops classes accept resolved values only
Lower-level ops classes used by `Dml` SHALL accept exact refs, exact branch names, exact ids, and other resolved repository values rather than caller-facing fuzzy selectors or config-shaped overrides.

#### Scenario: Commit workflow uses exact values below Dml
- **WHEN** a shared `Dml` method invokes commit/head workflow behavior
- **THEN** the lower-level ops calls receive already-resolved commits, branches, or ids instead of revision grammar strings

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

### Requirement: Shared `Dml` returns JSON-ready payloads
Shared `Dml` methods SHALL return JSON-ready dict/list payloads for container structure, while allowing typed leaves such as `Ref`, `Uri`, `Error`, and `Runnable`.

#### Scenario: CLI-ready result shape comes from Dml
- **WHEN** a caller invokes a shared `Dml` repository or admin workflow
- **THEN** the returned payload is ready for JSON serialization without CLI-owned result reshaping beyond standard typed-leaf encoding

### Requirement: Repository bootstrap and recovery are available through shared `Dml`
Repository bootstrap and recovery workflows SHALL be available through the shared `Dml` boundary.

#### Scenario: Init and recovery use Dml-owned entrypoint
- **WHEN** a caller invokes repository bootstrap or recovery behavior
- **THEN** the workflow executes through a `Dml` entrypoint and preserves the documented config-first recovery semantics

### Requirement: Shared `Dml` class exposes clone bootstrap
The shared `Dml` surface SHALL expose `clone` as a classmethod bootstrap workflow alongside `init`.

#### Scenario: Caller discovers clone on shared Dml surface
- **WHEN** a caller inspects the shared `Dml` class
- **THEN** clone bootstrap is available as `Dml.clone(...)` rather than as an instance method or external helper

### Requirement: Direct user cancellation SHALL use configured user identity
When `dml.runtime.cancel(index_id)` is invoked without an active runtime execution context, the workflow SHALL still proceed as an out-of-band cancellation operation. In that case, the runtime SHALL record `cancellation_requested_by` from the configured user identity.

#### Scenario: User-triggered cancel records configured user without active execution
- **WHEN** a user directly invokes `dml.runtime.cancel("idx1")`
- **AND** there is no active caller `execution_id`
- **THEN** the runtime SHALL set `cancellation_requested_by` to `config.user`

#### Scenario: Missing configured user still fails cancel
- **WHEN** a user invokes `dml.runtime.cancel("idx1")`
- **AND** no configured user identity is available
- **THEN** the runtime SHALL fail the request rather than persisting an empty cancellation requester

### Requirement: Runtime cancellation SHALL be out-of-band control-plane behavior
`dml.runtime.cancel(index_id)` SHALL operate as an out-of-band control-plane workflow rather than as a continuation of a running execution. The workflow SHALL freeze the target index, remove caller-owned live edges, orphan eligible callees, and request detached cancellation without requiring an active caller execution context.

#### Scenario: Direct cancel freezes index before cancellation traversal
- **WHEN** a user invokes `dml.runtime.cancel("idx1")`
- **THEN** the runtime SHALL freeze the index before removing live caller edges or requesting callee cancellation
