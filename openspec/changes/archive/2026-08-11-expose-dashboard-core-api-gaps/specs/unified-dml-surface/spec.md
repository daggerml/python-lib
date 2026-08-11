## MODIFIED Requirements

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

### Requirement: Shared `Dml` exposes the fixed method namespaces
The shared `Dml` SHALL expose repository methods including `status`, `show`, `log`, `diff`, `checkout`, `branch`, `tag`, `fetch`, `pull`, `push`, `merge`, `rebase`, and `revert`; dependency lifecycle under `dep.add|list|delete`; and the existing `dag`, `admin`, `runtime`, `config`, and `ops` namespaces.

- `branch`: `list`, `create`, `set_upstream`, `get_upstream`, `move`, `rename`, `delete`
- `tag`: `list`, `create`, `delete`
- `dag`: `list`, `get`, `checkout`, `delete`
- `admin.index`: `list`, `get`, `delete`
- `admin.cache`: `invalidate`
- `admin.remote`: `list`, `gc`
- `admin`: `gc`
- `runtime`: `create`, `describe`, `read_execution_record`, `read_launch_state`, `describe_graph`, `put_literal`, `put_import`, `start_fn`, `cancel`, `commit`
- `config`: `get`, `set`, `show`
- `ops`: `commit`, `head`, `dag`, `node`, `index`, `cache`, `remote`, `gc`, `config`

#### Scenario: Repository and dependency methods are present
- **WHEN** a caller inspects the shared `Dml` class
- **THEN** repository porcelain is top-level and dependency endpoint lifecycle is under `dep`

#### Scenario: DAG, admin, runtime, and config methods remain namespaced
- **WHEN** a caller needs DAG inspection, admin maintenance, runtime staging behavior, or config access
- **THEN** the shared `Dml` exposes those methods under `dag`, `admin`, `runtime`, and `config` namespaces respectively

#### Scenario: Runtime namespace exposes describe_graph
- **WHEN** a caller needs execution-lineage inspection rooted at one or more runtime executions
- **THEN** the shared `Dml` exposes that workflow as `dml.runtime.describe_graph(...)`

#### Scenario: Runtime namespace exposes launch-state inspection
- **WHEN** a caller needs the persisted executor resume state for one execution
- **THEN** the shared `Dml` exposes that workflow as `dml.runtime.read_launch_state(...)`

#### Scenario: Branch namespace exposes arbitrary upstream inspection
- **WHEN** a caller needs the configured upstream of a local branch other than the attached branch
- **THEN** the shared `Dml` exposes that workflow as `dml.branch.get_upstream(...)`

#### Scenario: Exact subsystem objects are grouped under ops
- **WHEN** a caller needs direct exact-input subsystem behavior such as `CommitOps`, `HeadOps`, or `IndexOps`
- **THEN** the shared `Dml` exposes those objects under `dml.ops.*` rather than as direct top-level `Dml` attributes

## ADDED Requirements

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
