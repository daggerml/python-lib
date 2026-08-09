## ADDED Requirements

### Requirement: Revision-consuming Dml methods expose exact source signatures
The shared public surface SHALL expose the following signatures, with `remote` and `dep` keyword-only and mutually exclusive wherever both appear:

- `show(revision: Ref | str = "HEAD", *, remote: bool = False, dep: str | None = None)`
- `log(revision: Ref | str = "HEAD", limit: int = 10, *, remote: bool = False, dep: str | None = None)`
- `diff(revision: Ref | str = "HEAD", relative_to: Ref | str | None = None, *, remote: bool = False, dep: str | None = None)`
- `rev_parse(revision: str, *, remote: bool = False, dep: str | None = None)`
- `checkout(revision: Ref | str, *, remote: bool = False)`
- `merge(revision: Ref | str, ff_only: bool = True, *, remote: bool = False)`
- `rebase(revision: Ref | str, *, remote: bool = False)`
- `revert(revision: Ref | str, message: str | None = None, *, remote: bool = False)`
- `branch.create(name: str, *, revision: Ref | str | None = None, remote: bool = False)`
- `tag.create(name: str, revision: Ref | str = "HEAD", *, remote: bool = False)`
- `dag.checkout(revision: Ref | str, dag: str, *, remote: bool = False, dep: str | None = None, name: str | None = None, replace: bool = False)`
- `fetch(revision: str | None = None, /, *, dep: str | None = None)`

#### Scenario: Inspection exposes both source selectors
- **WHEN** callers inspect show, log, diff, rev-parse, or DAG checkout
- **THEN** their signatures expose keyword-only `remote` and `dep`

#### Scenario: History mutation exposes remote only
- **WHEN** callers inspect repository checkout, merge, rebase, revert, branch creation, or tag creation
- **THEN** their signatures expose keyword-only `remote` and no dependency selector

#### Scenario: Fetch selects dependency separately from revision
- **WHEN** callers inspect fetch
- **THEN** its positional-only optional revision contains only a branch or `@tag` and optional keyword-only `dep` selects the endpoint

#### Scenario: Source mutual exclusion is validated by Dml
- **WHEN** any public method receives `remote=True` with non-null `dep`
- **THEN** the shared method fails before revision lookup, regardless of whether it was called from Python or the generated CLI

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

## MODIFIED Requirements

### Requirement: Shared `Dml` exact DB object contracts use `Ref`
The shared `Dml` surface SHALL require `Ref` objects for exact DB-backed object inputs and return `Ref` objects as canonical DB identity. Revision selectors, DAG and node names, branch and tag names, endpoint roots, dependency names, and runtime index IDs SHALL remain strings.

#### Scenario: Exact DAG and node access requires Ref
- **WHEN** a caller dereferences an exact DAG or node object
- **THEN** the method requires `Ref` and rejects a plain typed-ref string substitute

#### Scenario: Non-DB selectors remain strings
- **WHEN** a caller provides revision text, a symbolic object name, an endpoint root, a dependency name, or an index ID
- **THEN** the shared surface accepts the value as a string

#### Scenario: DB-backed payloads use ref identity
- **WHEN** a payload contains DB-backed object identity
- **THEN** it uses `Ref` without duplicating a raw ID string

### Requirement: Shared `Dml` exposes the fixed method namespaces
The shared `Dml` SHALL expose repository methods including `status`, `show`, `log`, `diff`, `checkout`, `branch`, `tag`, `fetch`, `pull`, `push`, `merge`, `rebase`, and `revert`; dependency lifecycle under `dep.add|list|delete`; and the existing `dag`, `admin`, `runtime`, `config`, and `ops` namespaces.

#### Scenario: Repository and dependency methods are present
- **WHEN** a caller inspects the shared `Dml` surface
- **THEN** repository porcelain is top-level and dependency endpoint lifecycle is under `dep`

#### Scenario: Other subsystem methods remain namespaced
- **WHEN** a caller needs DAG, administration, runtime, configuration, or lower-level ops behavior
- **THEN** those methods remain under their established namespaces

### Requirement: Shared `Dml` parameters SHALL expose machine-readable help metadata
Public parameters on the shared `Dml` surface and namespace methods SHALL use `typing.Annotated` metadata to describe parameter meaning, while signature defaults remain authoritative. Revision-consuming methods SHALL describe `remote` and `dep` source selection without embedding endpoint identity in revision strings.

#### Scenario: Parameter meaning is available from annotations
- **WHEN** a caller inspects public method annotations with extras included
- **THEN** each user-facing parameter includes concise meaning metadata

#### Scenario: Defaults remain in the signature
- **WHEN** a public method has a defaulted parameter
- **THEN** the signature, not annotation text, remains the source of truth

#### Scenario: Revision source parameters include useful examples
- **WHEN** a method accepts revision plus remote or dependency source selection
- **THEN** metadata MAY show namespace-independent branch or `@tag` examples and source flags

#### Scenario: Non-generatable CLI parameters remain internal
- **WHEN** helper state such as an S3 client cannot be generated from CLI input
- **THEN** it remains instance or private state rather than a public method parameter
