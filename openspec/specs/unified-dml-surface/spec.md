## Purpose
Define the canonical shared `_internal.Dml` boundary, its fixed caller-facing surface, and the delegation constraints between that surface and lower-level ops/config-resolution helpers.

## Requirements

### Requirement: One shared `_internal.Dml` class is the canonical orchestration boundary
The system SHALL expose one shared `_internal.Dml` class for repository, DAG, admin, and runtime workflows.

#### Scenario: CLI delegates through shared Dml
- **WHEN** a CLI command executes a repository, DAG, admin, or runtime workflow
- **THEN** the handler instantiates or receives a `Dml` instance and delegates through that class instead of orchestrating lower-level ops classes directly

#### Scenario: API wrappers delegate through shared Dml
- **WHEN** `Dag` or `Node` wrappers need repository/runtime behavior
- **THEN** they delegate through the shared internal `Dml` implementation, whether by direct use or by a thin compatibility wrapper in `daggerml.api`

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
The shared `Dml` surface SHALL require `Ref` objects for caller inputs that represent exact DB-backed objects, and it SHALL return `Ref` objects as the canonical identity for DB-backed objects in its payloads.

#### Scenario: Exact DAG access requires `Ref`
- **WHEN** a caller invokes a `Dml` method whose contract is to dereference an exact DAG object
- **THEN** the method requires a `Ref`
- **AND** it does not accept a plain `"dag:..."` string as a substitute

#### Scenario: Exact node access requires `Ref`
- **WHEN** a caller invokes a `Dml` method whose contract is to dereference an exact node object
- **THEN** the method requires a `Ref`
- **AND** it does not accept a plain `"node:..."` string as a substitute

#### Scenario: Non-DB selectors remain strings
- **WHEN** a caller provides a revision selector, DAG name, node name, branch, tag, remote URI, or `index_id`
- **THEN** the shared `Dml` surface continues to accept that value as a string

#### Scenario: DB-backed payloads use ref identity
- **WHEN** a shared `Dml` payload includes the identity of a commit, DAG, node, or other DB-backed object
- **THEN** that identity is represented by `Ref`
- **AND** the payload does not duplicate the same DB identity as a separate raw `id` string

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

#### Scenario: Admin workflow delegates to the owning subsystem
- **WHEN** a caller invokes an admin cache, remote, or gc workflow
- **THEN** `Dml` delegates the repository action to `CacheOps`, `RemoteOps`, or `GcOps` respectively after preparing resolved inputs

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
