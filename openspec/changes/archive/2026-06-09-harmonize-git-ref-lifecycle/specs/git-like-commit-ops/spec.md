## MODIFIED Requirements

### Requirement: Revision resolution
The system SHALL resolve revision values used by git-like commands to concrete local commit refs without performing network fetches. `HEAD` and ancestry expressions based on `HEAD` SHALL resolve through the repository's `.dml/HEAD` file. Remote refs SHALL use canonical `dml://owner/project#branch` and `dml://owner/project@tag` syntax rather than named-remote shorthand.

#### Scenario: Resolve branch shorthand
- **WHEN** a command receives `main` as a revision
- **THEN** the system resolves it as local branch `main`

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

#### Scenario: Resolve local tag shorthand with explicit tag selector
- **WHEN** a command receives `@v1.0` as a revision and `v1.0` resolves as a local tag
- **THEN** the system resolves it to the commit referenced by that tag

### Requirement: Checkout repository state from revision
The system SHALL support checking out repository state from a resolved revision and SHALL distinguish branch-attached from detached checkouts by rewriting `.dml/HEAD`.

#### Scenario: Checkout branch attaches runtime
- **WHEN** `dml checkout main` resolves `main` to a local branch
- **THEN** the system writes `.dml/HEAD` as `ref: refs/local/heads/main` and reports branch-attached checkout

#### Scenario: Checkout tag detaches runtime
- **WHEN** `dml checkout @v1.0` resolves `@v1.0` to a tag target commit
- **THEN** the system writes `.dml/HEAD` as that detached commit and reports detached checkout at that commit

#### Scenario: Checkout commit expression detaches runtime
- **WHEN** `dml checkout HEAD~1` resolves to a concrete commit
- **THEN** the system writes `.dml/HEAD` as that detached commit and reports detached checkout at that commit

#### Scenario: Checkout fetched remote branch remains detached
- **WHEN** `dml checkout dml://alice/tools#main` resolves to a fetched remote-tracking commit
- **THEN** the system writes `.dml/HEAD` as that detached commit
- **AND** it does not implicitly create or attach a local branch

#### Scenario: Commit while detached advances detached HEAD
- **WHEN** a user checks out a non-branch revision and then runs commit flow through `Dml.runtime.commit`
- **THEN** the system creates the new commit
- **AND** rewrites detached `.dml/HEAD` to that new commit
- **AND** does not advance any local branch ref

### Requirement: Mutable project workflows require an attached branch
The system SHALL require `.dml/HEAD` to be attached to a local branch before default project workflows mutate branch history or publish the default same-name branch tip.

#### Scenario: Push uses attached HEAD branch by default
- **WHEN** `.dml/HEAD` is attached to local branch `foo` and the user runs project push without an explicit revision override
- **THEN** the system pushes local branch `foo` to remote branch URI `dml://<owner>/<project>#foo`

#### Scenario: Pull requires attached HEAD
- **WHEN** `.dml/HEAD` is detached and the user runs project pull without an explicit mutable branch target
- **THEN** the command fails instead of selecting a branch from config or environment

#### Scenario: Merge requires attached HEAD when defaulting destination
- **WHEN** `.dml/HEAD` is detached and the user runs a merge workflow that would otherwise target the current branch
- **THEN** the command fails because the current checkout is not a mutable branch target

#### Scenario: Checkout unresolved remote URI fails locally
- **WHEN** `dml checkout dml://alice/tools#main` is requested and no local tracking ref exists for that URI
- **THEN** checkout fails without implicit fetch and reports that the revision cannot be resolved locally

### Requirement: Repository inspection workflows resolve revisions locally
The system SHALL provide repository inspection workflows for `show`, `log`, and `diff` that resolve revisions locally without performing implicit network fetches.

#### Scenario: Show resolves fetched remote URI locally
- **WHEN** a user runs `dml show dml://alice/demo#main`
- **THEN** the system resolves the revision through existing local fetched-remote state
- **AND** it does not contact the remote automatically

#### Scenario: Diff resolves both revisions locally
- **WHEN** a user runs `dml diff dml://alice/demo#main HEAD`
- **THEN** the system resolves both revisions from local state only

### Requirement: Branch and tag lifecycle commands expose the full local ref lifecycle
The system SHALL support explicit branch and tag lifecycle commands for local mutable and local immutable refs.

#### Scenario: Branch create uses current HEAD by default
- **WHEN** a user runs `dml branch create feature` while `HEAD` resolves to commit `C`
- **THEN** the system creates local branch `feature` at `C`
- **AND** it does not change the current `HEAD` attachment

#### Scenario: Branch create may target explicit revision
- **WHEN** a user runs `dml branch create feature dml://alice/demo#main`
- **THEN** the system resolves that revision locally
- **AND** creates local branch `feature` at the resolved commit

#### Scenario: Branch move repoints branch
- **WHEN** a user runs `dml branch move feature HEAD~1`
- **THEN** the system repoints local branch `feature` to the resolved commit

#### Scenario: Branch rename keeps attached HEAD on renamed branch
- **WHEN** `HEAD` is attached to local branch `main` and the user runs `dml branch rename main trunk`
- **THEN** the system renames the local branch ref to `trunk`
- **AND** `.dml/HEAD` remains attached to `trunk`

#### Scenario: Branch delete removes non-current branch ref
- **WHEN** a user runs `dml branch delete feature` and `feature` is not the currently attached branch
- **THEN** the system removes the local branch ref

#### Scenario: Tag create uses current HEAD by default
- **WHEN** a user runs `dml tag create v1`
- **THEN** the system creates local tag `v1` at the current `HEAD` commit

#### Scenario: Tag delete removes tag ref
- **WHEN** a user runs `dml tag delete v1`
- **THEN** the system removes the local tag ref

### Requirement: Status reports current branch state and same-name tracking relationship
The system SHALL provide a repository status workflow that reports the current `HEAD` state, local branches, live indexes, and same-name ahead/behind counts against the fetched remote branch when both an attached local branch and configured project identity are available.

#### Scenario: Status reports attached head with tracking counts
- **WHEN** `HEAD` is attached to branch `main`, `remote.project` is configured, and fetched remote branch `dml://alice/demo#main` exists locally
- **THEN** the response reports attached head state for `main`
- **AND** includes ahead/behind counts relative to that fetched remote branch

#### Scenario: Status reports detached head without tracking counts
- **WHEN** HEAD is detached and a user runs `dml status`
- **THEN** the response reports detached head state and the current commit
- **AND** ahead/behind are absent or null because no attached local branch is being tracked

### Requirement: Show returns commit delta over DAG namespace
The system SHALL compute commit-introduced change for `dml show` as DAG-map additions, removals, and updates between the selected commit tree and its base tree, exposed through a `diff` payload keyed by `added`, `removed`, and `modified`.

#### Scenario: Show detects DAG addition
- **WHEN** a commit introduces `train -> dag:a` where the base tree had no `train`
- **THEN** `dml show` reports `train` under `diff.added`

#### Scenario: Show detects DAG update
- **WHEN** a commit changes `train` from `dag:a` to `dag:b`
- **THEN** `dml show` reports `train` under `diff.modified` with before and after refs

#### Scenario: Show detects DAG removal
- **WHEN** a commit removes `train -> dag:a`
- **THEN** `dml show` reports `train` under `diff.removed`

### Requirement: Git-like project workflows are owned by `Dml` orchestration
Git-like project command workflows SHALL be available through the shared internal `Dml` orchestration boundary, which coordinates commit, head, branch, tag, and remote operations while delegating concrete repository actions to lower-level ops classes.

#### Scenario: Pull executes through Dml workflow
- **WHEN** a caller invokes project pull with remote target, branch target, and user context
- **THEN** `Dml` resolves same-name branch context, fetches the matching fetched remote branch, and applies merge behavior through internal ops

#### Scenario: Push executes through Dml workflow
- **WHEN** a caller invokes project push with remote target and push options
- **THEN** `Dml` resolves the revision through its selector boundary and performs project-aware remote push behavior through the relevant ops classes

#### Scenario: Remote delete executes through push delete workflow
- **WHEN** a caller invokes `dml push --delete <revision>`
- **THEN** `Dml` resolves the revision through the normal selector boundary
- **AND** deletes the selected remote branch or tag ref instead of publishing a new value

## REMOVED Requirements

### Requirement: Branch creation and listing expose git-like branch inspection workflows
**Reason**: The older requirement mixes a now-missing top-level `branch` API with named-remote listing behavior that no longer matches the DML model.
**Migration**: Replace it with the explicit branch and tag namespace lifecycle model plus fetched-remote `dml://...` selectors.
