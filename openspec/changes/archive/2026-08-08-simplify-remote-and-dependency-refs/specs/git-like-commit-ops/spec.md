## ADDED Requirements

### Requirement: History mutation may select fetched remote revisions
Merge, rebase, and revert SHALL expose `remote=False` to select the revision from local remote tracking state. These workflows SHALL NOT expose dependency selection and SHALL NOT fetch implicitly.

#### Scenario: Merge fetched remote revision
- **WHEN** merge receives revision `main` with `remote=True`
- **THEN** it merges the commit from `.dml/refs/remote/heads/main`

#### Scenario: Rebase onto fetched remote revision
- **WHEN** rebase receives revision `main` with `remote=True`
- **THEN** it rebases onto the locally tracked remote commit

#### Scenario: Revert fetched remote revision
- **WHEN** revert receives revision `@v1` with `remote=True`
- **THEN** it reverts the commit from `.dml/refs/remote/tags/v1`

#### Scenario: Missing remote revision does not fetch
- **WHEN** a history mutation selects an absent remote tracking revision
- **THEN** it fails with fetch-required guidance without network access or history changes

### Requirement: Tag creation may select a fetched remote revision
Tag creation SHALL accept a namespace-independent revision and `remote=False`. With `remote=True`, it SHALL resolve the revision from existing remote tracking state. It SHALL NOT expose dependency selection or fetch implicitly.

#### Scenario: Create local tag from remote tracking branch
- **WHEN** local tag `baseline` is created from revision `main` with `remote=True`
- **THEN** `baseline` points to the already-fetched remote tracking commit for `main`

#### Scenario: Missing remote tag source does not fetch
- **WHEN** tag creation selects an absent remote tracking revision
- **THEN** it fails with fetch-required guidance without creating the local tag

## MODIFIED Requirements

### Requirement: DAG checkout from revision
The system SHALL support checking out one DAG from a resolved local, remote tracking, or named dependency revision into the current branch tree and committing that change. Remote and dependency source selection SHALL be separate from revision text.

#### Scenario: Checkout local DAG with same name
- **WHEN** `dml dag checkout HEAD~1 train` resolves local `HEAD~1` containing `train -> dag:a`
- **THEN** the system commits `train -> dag:a` into the current branch

#### Scenario: Checkout dependency DAG with alias
- **WHEN** DAG checkout selects dependency `models`, revision `main`, DAG `train`, and alias `baseline_train`
- **THEN** the system commits `baseline_train` pointing to the selected DAG ref

#### Scenario: Checkout refuses overwrite by default
- **WHEN** the target name exists with a different DAG and replace is false
- **THEN** DAG checkout fails without creating a commit

#### Scenario: Checkout replaces when requested
- **WHEN** the target name exists and replace is true
- **THEN** DAG checkout commits the selected DAG under that name

### Requirement: Revision resolution
The system SHALL accept every supported revision grammar form with local, remote, or dependency source selection and resolve it to a concrete commit without network access when possible. A normalized source argument SHALL select local refs by default, remote tracking refs when `remote=True`, or dependency tracking refs when `dep=<name>`. Exact commit IDs and refs SHALL resolve from the local object database regardless of selected source. The only invalid source-argument combination is simultaneous `remote=True` and non-null `dep`; any otherwise unresolvable combination SHALL raise a descriptive resolution error.

#### Scenario: Resolve local branch
- **WHEN** revision `main` has no source selector
- **THEN** it resolves only as local branch `main`

#### Scenario: Resolve remote tracking branch
- **WHEN** revision `main` is supplied with `remote=True`
- **THEN** it resolves from `.dml/refs/remote/heads/main`

#### Scenario: Resolve dependency tag
- **WHEN** revision `@v1` is supplied with `dep="models"`
- **THEN** it resolves from `.dml/refs/dep/models/tags/v1`

#### Scenario: Resolve first-parent ancestry
- **WHEN** local revision `HEAD~2` is supplied
- **THEN** resolution starts from `.dml/HEAD` and walks two first-parent steps

#### Scenario: Exact commit ignores symbolic namespace
- **WHEN** an existing exact commit ref is supplied with local, remote, or dependency source selection
- **THEN** it resolves to that local database object

#### Scenario: Symbolic form unavailable in selected namespace
- **WHEN** a syntactically valid revision cannot resolve from the selected source
- **THEN** resolution raises a descriptive error without fetching

#### Scenario: Conflicting source arguments are rejected
- **WHEN** `remote=True` and non-null `dep` are supplied together
- **THEN** resolution fails before ref lookup

### Requirement: Checkout repository state from revision
The system SHALL support repository checkout from local or remote tracking revisions. Local branch checkout SHALL attach HEAD; remote tracking, tags, ancestry, and commit refs SHALL detach HEAD. Repository checkout SHALL NOT expose dependency source selection.

#### Scenario: Checkout local branch attaches runtime
- **WHEN** checkout resolves local branch `main`
- **THEN** `.dml/HEAD` attaches to local branch `main`

#### Scenario: Checkout remote branch detaches runtime
- **WHEN** checkout resolves revision `main` with `remote=True`
- **THEN** `.dml/HEAD` detaches at the remote tracking commit

#### Scenario: Checkout tag or commit detaches runtime
- **WHEN** checkout resolves `@v1`, ancestry, or a direct commit
- **THEN** `.dml/HEAD` detaches at that commit

### Requirement: Mutable project workflows require an attached branch
The system SHALL require attached HEAD before default workflows mutate branch history or publish a branch tip. Push SHALL use the branch's configured remote-root upstream branch; an untracked branch SHALL publish to the same branch name and record that upstream only after success.

#### Scenario: Push uses configured upstream branch
- **WHEN** local branch `foo` tracks remote-root branch `main`
- **THEN** push publishes `foo` to branch `main` at resolved `remote.root`

#### Scenario: First push records same-name upstream
- **WHEN** untracked local branch `foo` pushes successfully
- **THEN** it publishes remote branch `foo` and records upstream branch `foo`

#### Scenario: Failed first push leaves branch untracked
- **WHEN** first publication of local branch `foo` fails
- **THEN** no upstream is recorded

#### Scenario: Push requires attached HEAD
- **WHEN** HEAD is detached and default push is requested
- **THEN** push fails

### Requirement: Repository inspection workflows resolve revisions locally
The system SHALL provide show, log, and diff workflows that resolve local, remote tracking, or dependency tracking revisions from local state only. Their revision text SHALL not contain endpoint identity.

#### Scenario: Show resolves remote tracking revision locally
- **WHEN** a user runs show for revision `main` with `remote=True`
- **THEN** it resolves existing remote tracking state without contacting `remote.root`

#### Scenario: Show resolves dependency revision locally
- **WHEN** a user runs show for revision `main` with `dep="models"`
- **THEN** it resolves existing dependency tracking state without contacting that dependency

#### Scenario: Diff source applies to primary revision
- **WHEN** diff selects remote revision `main` relative to explicit local `HEAD`
- **THEN** only the primary revision uses remote tracking source

#### Scenario: Diff default base uses selected commit parent
- **WHEN** diff omits `relative_to`
- **THEN** it compares the selected commit with that commit's parent

### Requirement: Branch creation and listing expose tracked remote workflows
The system SHALL support branch creation with an optional revision and optional remote source selection. It SHALL resolve explicit revisions locally, never fetch implicitly, and record a branch-only upstream when requested by the branch workflow. Dependency source selection SHALL not be exposed.

#### Scenario: Remote tracking revision initializes branch
- **WHEN** branch `feature` is created from revision `main` with `remote=True`
- **THEN** local `feature` points to the already-fetched remote tracking commit

#### Scenario: Explicit local revision initializes branch
- **WHEN** branch `feature` is created from local `HEAD~1`
- **THEN** local `feature` points to that commit

#### Scenario: Missing remote tracking revision does not fetch
- **WHEN** branch creation selects absent remote tracking branch `feature`
- **THEN** it fails with fetch-required guidance and does not contact `remote.root`

#### Scenario: Branch list omits unborn current branch
- **WHEN** HEAD is attached to unborn branch `main`
- **THEN** branch list omits `main` until its ref is materialized

### Requirement: Repository status reports upstream synchronization
The system SHALL report the current branch's configured remote-root upstream branch and ahead/behind counts relative to its local remote tracking ref when available.

#### Scenario: Status reports configured upstream
- **WHEN** local branch `feature` tracks remote branch `main`
- **THEN** status identifies upstream branch `main` without an endpoint name

#### Scenario: Status reports synchronization counts
- **WHEN** the upstream tracking ref exists and differs from the current branch
- **THEN** status reports ahead and behind counts

#### Scenario: Status reports unavailable upstream counts
- **WHEN** no upstream exists or it has not been fetched
- **THEN** status reports unavailable counts without inferring an upstream
