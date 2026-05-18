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
The shared `Dml` constructor SHALL accept the root runtime override inputs already threaded through callers for project-home, remote-uri, user, and config-home context.

#### Scenario: CLI globals map directly to constructor
- **WHEN** a caller provides explicit project-home, remote-uri, user, or config-home runtime overrides
- **THEN** those values can be passed directly to the shared `Dml` constructor without a separate caller-specific context adapter

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
The shared `Dml` boundary and its public namespaces SHALL expose runtime documentation that explains class purpose, method behavior, and parameter meaning without changing workflow semantics.

#### Scenario: Namespace objects describe their purpose
- **WHEN** a caller inspects `Dml` or any namespace reachable through `dml.config`, `dml.runtime`, `dml.dag`, or `dml.admin`
- **THEN** the class exposes a docstring that describes the purpose of that boundary or namespace

#### Scenario: Public methods describe behavior
- **WHEN** a caller inspects a public top-level or namespaced `Dml` method
- **THEN** the method exposes a docstring that describes the operation behavior and any notable constraints or side effects

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

### Requirement: `Dml` stores only runtime context and temporary-directory bookkeeping
The shared `Dml` class SHALL keep only `_context` and `_tempdirs` as private instance attributes. Helper behavior that supports `Dml` public methods SHALL live in module-level functions within `daggerml._internal.dml` rather than in private `Dml` instance methods.

#### Scenario: Namespace and helper access do not require extra Dml instance fields
- **WHEN** a caller uses any public namespace on `Dml`
- **THEN** the namespace behavior is derived from `_context`, `_tempdirs`, and delegated helper logic without introducing additional private `Dml` instance attributes

#### Scenario: Dml public workflows do not depend on private helper methods
- **WHEN** a `Dml` repository, runtime, DAG, admin, or config workflow needs helper behavior such as ops dispatch, payload shaping, or revision binding
- **THEN** that helper behavior executes through module-level functions in `daggerml._internal.dml` rather than through `Dml._...` instance methods

#### Scenario: Namespace objects keep only Dml as private state
- **WHEN** a caller inspects the namespace objects exposed by `Dml`
- **THEN** each namespace object keeps only `._dml` as private instance state
- **AND** namespace helper behavior does not rely on additional private attrs or private helper methods on the namespace object

### Requirement: `Dml` is the only fuzzy-selector boundary
The shared `Dml` class SHALL accept only the fuzzy selector forms already specified by the redesigned CLI contracts and SHALL resolve those forms internally before invoking lower-level operations.

#### Scenario: Revision selector resolves inside Dml
- **WHEN** a caller passes a supported revision string such as `HEAD~1` to a shared `Dml` repository method
- **THEN** the `Dml` method resolves it through the fuzzy-resolution submodule and lower-level ops receive only exact values

#### Scenario: DAG selector resolves inside Dml
- **WHEN** a caller passes `train` or `dag:abc123` to `dml.dag.get`
- **THEN** the shared `Dml` method performs the selector-mode handling through the fuzzy-resolution submodule and lower-level ops do not parse that caller-facing form

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
