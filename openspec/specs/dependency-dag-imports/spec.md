## Purpose
Define import-only dependency lifecycle and committed DAG loading behavior.

## Requirements

### Requirement: Import-only dependency lifecycle
The system SHALL expose `dml dep add <name> <root>`, `dml dep list`, and `dml dep delete <name>` for import-only endpoints under `.dml/refs/dep/<name>/`. Names SHALL be validated single path segments. Each dependency SHALL store v1 endpoint config containing exactly `backend` and `root` in `config.json` beside `heads/` and `tags/`; unknown fields SHALL be rejected. Endpoint identity SHALL NOT persist in DML objects.

The shared public signatures SHALL be `dep.add(name: str, root: str) -> str`, `dep.list() -> dict[str, str]`, and `dep.delete(name: str) -> None` so the CLI generator derives the lifecycle commands without custom routing.

#### Scenario: Add a dependency
- **WHEN** a user runs `dml dep add models s3://bucket/models`
- **THEN** the system creates `.dml/refs/dep/models/config.json` containing normalized `{"backend":"s3","root":"s3://bucket/models"}` and makes `models` available to dependency fetch and import operations

#### Scenario: Reject invalid or duplicate dependency
- **WHEN** a dependency name contains `/`, its config is invalid, or that name already exists
- **THEN** add fails without changing dependency state

#### Scenario: Delete a dependency
- **WHEN** a user deletes dependency `models`
- **THEN** its endpoint config and tracking refs are removed as local GC roots without deleting objects still reachable from local commits or runtimes

#### Scenario: Delete unknown dependency
- **WHEN** a user deletes an unconfigured dependency
- **THEN** deletion fails without changing other dependency state

### Requirement: Fetch explicitly selects project root or dependency
The system SHALL implement `dml fetch [--dep DEP] [--depth N | --unshallow] [BRANCH|@TAG]`. It SHALL use resolved `remote.root` when `--dep` is absent, use the named dependency endpoint when supplied, and use branch `default.branch_name` when no ref selector is supplied. A positive depth SHALL bound commit ancestry while preserving complete included snapshots, `--unshallow` SHALL materialize all ancestry reachable from the selected ref, and the two options SHALL be mutually exclusive. `dep add` SHALL remain configuration-only and SHALL NOT persist a default history depth.

#### Scenario: Fetch project branch by default
- **WHEN** a user runs `dml fetch feature`
- **THEN** only `.dml/refs/remote/heads/feature` is updated from `remote.root`

#### Scenario: Fetch shallow project branch
- **WHEN** a user runs `dml fetch --depth 2 feature`
- **THEN** the feature tip and two commit generations with complete snapshots are locally available
- **AND** only `.dml/refs/remote/heads/feature` is updated

#### Scenario: Fetch dependency default branch
- **WHEN** a user runs `dml fetch --dep models`
- **THEN** only `.dml/refs/dep/models/heads/<default.branch_name>` is updated from dependency `models`

#### Scenario: Fetch shallow dependency branch
- **WHEN** a user runs `dml fetch --dep models --depth 1`
- **THEN** the dependency tip and complete current DAG snapshot are locally available without materializing otherwise-unavailable parent commits

#### Scenario: Unshallow dependency tag
- **WHEN** a user runs `dml fetch --dep models --unshallow @v1`
- **THEN** every commit reachable from dependency tag `v1` is locally available and its tracking tag is updated

#### Scenario: Fetch dependency tag
- **WHEN** a user runs `dml fetch --dep models @v1`
- **THEN** only `.dml/refs/dep/models/tags/v1` is updated

### Requirement: Dependency revisions are importable but not synchronizable
The system SHALL allow `dep=<name>` with namespace-independent revisions for inspection, `api.load`, DAG checkout, and node import. Repository checkout, branch/tag creation, upstream, pull, push, merge, rebase, and revert public APIs SHALL NOT expose dependency source selection.

#### Scenario: Import a DAG node from a dependency branch
- **WHEN** fetched dependency `models` branch `main` contains DAG `train`
- **THEN** `api.load("train", revision="main", dep="models")` can provide that DAG to `Dag.require()` for an import node

#### Scenario: Dependency selector rejected at unsupported boundary
- **WHEN** a caller attempts to provide a dependency selector to a synchronization or history-mutation API
- **THEN** the public API rejects the unsupported argument before remote mutation or local history changes

### Requirement: Imported dependency DAGs are self-contained after publication
The system SHALL publish every object reachable from a local DAG that imports a dependency DAG or node before publishing the destination branch or tag ref. Cloning the destination project SHALL not require the original dependency endpoint to read or execute that imported DAG.

#### Scenario: Clone a project with imported dependency work
- **WHEN** a local DAG imports a node from dependency `models` branch `main` and the local branch is pushed
- **THEN** a clone of `remote.root` can materialize the local DAG and imported node without configuring `models`

### Requirement: API load selects committed DAG by revision source
The system SHALL expose `api.load(name: str, dml: Dml | None = None, *, revision: Ref | str = "HEAD", remote: bool = False, dep: str | None = None) -> Dag`. `remote` and `dep` SHALL be mutually exclusive and affect symbolic lookup only; exact commits SHALL resolve from the local object database regardless of source selection. The named committed DAG SHALL be returned only when its ref exists locally.

#### Scenario: Load dependency DAG
- **WHEN** `api.load("train", revision="main", dep="models")` selects a fetched dependency commit containing `train`
- **THEN** it returns the committed DAG backed by the local database

#### Scenario: Load rejects conflicting or missing source
- **WHEN** source selectors conflict, the tracking ref was not fetched, or the DAG object is unavailable locally
- **THEN** load raises a descriptive repository error without network access

### Requirement: Dag require accepts a loaded committed Dag
The system SHALL expose `Dag.require(dag: str | Dag, node_name: str | None = None, *, name: str | None = None) -> Node`. It SHALL allow an open destination DAG to require the result or a named node from a properly loaded committed DAG while preserving the existing local DAG-name form, and SHALL validate source DAG and node refs before writing an import node.

#### Scenario: Require loaded DAG result
- **WHEN** an open DAG requires a loaded committed DAG without a node name
- **THEN** it writes an import node referencing that DAG and its result node

#### Scenario: Require loaded DAG named node
- **WHEN** an open DAG requires named node `weights` from a loaded committed DAG
- **THEN** it writes an import node referencing that named node

#### Scenario: Require rejects invalid loaded DAG
- **WHEN** the supplied DAG is open, missing locally, or lacks the requested node
- **THEN** require fails without mutating the destination DAG
