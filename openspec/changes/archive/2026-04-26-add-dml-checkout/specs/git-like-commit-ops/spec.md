## MODIFIED Requirements

### Requirement: Revision resolution
The system SHALL resolve revision values used by git-like commands to concrete local commit refs without performing network fetches.

#### Scenario: Resolve branch shorthand
- **WHEN** a command receives `main` as a revision
- **THEN** the system resolves it as local branch `main`

#### Scenario: Resolve remote-tracking branch shorthand
- **WHEN** a command receives `origin/main` as a revision
- **THEN** the system resolves it through the configured remote URI to local tracking ref `dml://<owner>/<project>#main`

#### Scenario: Resolve fetched DML branch URI
- **WHEN** a command receives `dml://alice/tools#main` as a revision and that tracking ref exists locally
- **THEN** the system resolves it to the commit stored for that tracking ref

#### Scenario: Resolve fetched DML tag URI
- **WHEN** a command receives `dml://alice/tools@v1.0` as a revision and that tracking ref exists locally
- **THEN** the system resolves it to the commit stored for that tracking ref

#### Scenario: Unfetched DML URI is not fetched implicitly
- **WHEN** a command receives `dml://alice/tools#main` as a revision and no matching local tracking ref exists
- **THEN** the command fails without contacting the remote

#### Scenario: Resolve first-parent ancestry
- **WHEN** a command receives `HEAD~2` as a revision
- **THEN** the system resolves it by walking two first-parent steps from the current head commit

#### Scenario: Resolve local tag shorthand
- **WHEN** a command receives `v1.0` as a revision and `v1.0` resolves as a local tag
- **THEN** the system resolves it to the commit referenced by that tag

### Requirement: Checkout repository state from revision
The system SHALL support checking out repository state from a resolved revision and SHALL distinguish branch-attached from detached checkouts.

#### Scenario: Checkout branch attaches runtime
- **WHEN** `dml checkout main` resolves `main` to a local branch
- **THEN** the system sets active HEAD to branch `main` and reports branch-attached checkout

#### Scenario: Checkout tag detaches runtime
- **WHEN** `dml checkout v1.0` resolves `v1.0` to a tag target commit
- **THEN** the system clears active HEAD and reports detached checkout at that commit

#### Scenario: Checkout commit expression detaches runtime
- **WHEN** `dml checkout HEAD~1` resolves to a concrete commit
- **THEN** the system clears active HEAD and reports detached checkout at that commit

#### Scenario: Commit while detached does not advance branch
- **WHEN** a user checks out a non-branch revision and then runs commit flow through `IndexOps.commit`
- **THEN** the system commits the index without advancing any branch head

#### Scenario: Checkout unresolved remote URI fails locally
- **WHEN** `dml checkout dml://alice/tools#main` is requested and no local tracking ref exists for that URI
- **THEN** checkout fails without implicit fetch and reports that the revision cannot be resolved locally

### Requirement: Clone composes fetch then checkout
The system SHALL implement clone as `fetch` followed by `checkout`, using the fetched target revision for checkout semantics.

#### Scenario: Clone branch uses fetch then attached checkout
- **WHEN** `dml clone dml://alice/tools#main` is requested
- **THEN** the system fetches `dml://alice/tools#main` and checks out `main` as a branch-attached HEAD

#### Scenario: Clone tag uses fetch then detached checkout
- **WHEN** `dml clone dml://alice/tools@v1.0` is requested
- **THEN** the system fetches `dml://alice/tools@v1.0` and checks out the resolved commit in detached mode

#### Scenario: Clone direct commit is not supported yet
- **WHEN** `dml clone <uri>@<commit-ref>` is requested for a direct commit target that is not fetchable as a branch/tag ref
- **THEN** clone fails with an error indicating direct commit clone is unsupported until fetch supports commit-target retrieval

#### Scenario: Clone does not run init hooks
- **WHEN** `dml clone` initializes a local repository for the first time
- **THEN** the system does not invoke `dml init` and does not run init hooks as part of clone
