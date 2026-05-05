## MODIFIED Requirements

### Requirement: Revision resolution
The system SHALL resolve revision values used by git-like commands to concrete local commit refs without performing network fetches. `HEAD` and ancestry expressions based on `HEAD` SHALL resolve through the repository's `.dml/HEAD` file.

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

#### Scenario: Resolve first-parent ancestry from HEAD file
- **WHEN** a command receives `HEAD~2` as a revision
- **THEN** the system resolves `HEAD` through `.dml/HEAD` and walks two first-parent steps from that resolved commit

#### Scenario: Resolve local tag shorthand
- **WHEN** a command receives `v1.0` as a revision and `v1.0` resolves as a local tag
- **THEN** the system resolves it to the commit referenced by that tag

### Requirement: Checkout repository state from revision
The system SHALL support checking out repository state from a resolved revision and SHALL distinguish branch-attached from detached checkouts by rewriting `.dml/HEAD`.

#### Scenario: Checkout branch attaches runtime
- **WHEN** `dml checkout main` resolves `main` to a local branch
- **THEN** the system writes `.dml/HEAD` as `ref: refs/local/heads/main` and reports branch-attached checkout

#### Scenario: Checkout tag detaches runtime
- **WHEN** `dml checkout v1.0` resolves `v1.0` to a tag target commit
- **THEN** the system writes `.dml/HEAD` as that detached commit and reports detached checkout at that commit

#### Scenario: Checkout commit expression detaches runtime
- **WHEN** `dml checkout HEAD~1` resolves to a concrete commit
- **THEN** the system writes `.dml/HEAD` as that detached commit and reports detached checkout at that commit

#### Scenario: Commit while detached does not advance branch or HEAD
- **WHEN** a user checks out a non-branch revision and then runs commit flow through `IndexOps.commit`
- **THEN** the system may create the new detached commit but does not advance any branch head and does not rewrite `.dml/HEAD`

#### Scenario: Checkout unresolved remote URI fails locally
- **WHEN** `dml checkout dml://alice/tools#main` is requested and no local tracking ref exists for that URI
- **THEN** checkout fails without implicit fetch and reports that the revision cannot be resolved locally

## ADDED Requirements

### Requirement: Mutable project workflows require an attached branch
The system SHALL require `.dml/HEAD` to be attached to a local branch before default project workflows mutate branch history or publish a branch tip.

#### Scenario: Push uses attached HEAD branch by default
- **WHEN** `.dml/HEAD` is attached to local branch `foo` and the user runs project push without an explicit branch override
- **THEN** the system pushes local branch `foo` to remote branch URI `dml://<owner>/<project>#foo`

#### Scenario: Pull requires attached HEAD
- **WHEN** `.dml/HEAD` is detached and the user runs project pull without an explicit mutable branch target
- **THEN** the command fails instead of selecting a branch from config or environment

#### Scenario: Merge requires attached HEAD when defaulting destination
- **WHEN** `.dml/HEAD` is detached and the user runs a merge workflow that would otherwise target the current branch
- **THEN** the command fails because the current checkout is not a mutable branch target
