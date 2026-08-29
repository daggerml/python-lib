## MODIFIED Requirements

### Requirement: Revision resolution
The system SHALL resolve revision values used by git-like commands to concrete local commit refs without performing network fetches. `HEAD` and ancestry expressions based on `HEAD` SHALL resolve through the repository's `.dml/HEAD` file. A remote-tracking branch selector SHALL use `<remote-name>/<branch-name>`.

#### Scenario: Resolve branch shorthand
- **WHEN** a command receives `main` as a revision
- **THEN** the system resolves it as local branch `main`

#### Scenario: Resolve remote-tracking branch shorthand
- **WHEN** a command receives `origin/main` as a revision
- **THEN** the system resolves it through the local tracking ref for remote `origin` branch `main`

#### Scenario: Resolve first-parent ancestry from HEAD file
- **WHEN** a command receives `HEAD~2` as a revision
- **THEN** the system resolves `HEAD` through `.dml/HEAD` and walks two first-parent steps from that resolved commit

#### Scenario: Resolve local tag shorthand
- **WHEN** a command receives `v1.0` as a revision and `v1.0` resolves as a local tag
- **THEN** the system resolves it to the commit referenced by that tag

### Requirement: Mutable project workflows require an attached branch
The system SHALL require `.dml/HEAD` to be attached to a local branch before default project workflows mutate branch history or publish a branch tip. Default push SHALL use the current branch's configured upstream. If the current branch is untracked, default push SHALL publish to `origin/<local-branch>` and record that upstream only after publication succeeds.

#### Scenario: Push uses configured upstream
- **WHEN** `.dml/HEAD` is attached to local branch `foo` tracking `research/main` and the user runs push
- **THEN** the system pushes local branch `foo` to remote branch `research/main`

#### Scenario: First push establishes origin upstream
- **WHEN** attached local branch `foo` has no upstream and the user runs push successfully
- **THEN** the system publishes to `origin/foo`
- **AND** configures `foo` to track `origin/foo`

#### Scenario: Failed first push leaves branch untracked
- **WHEN** attached local branch `foo` has no upstream and publication to `origin/foo` fails
- **THEN** `foo` remains untracked

#### Scenario: Push requires attached HEAD
- **WHEN** `.dml/HEAD` is detached and the user runs push
- **THEN** the command fails

### Requirement: Branch creation and listing expose tracked remote workflows
The system SHALL support `branch create [--remote REMOTE] [--revision REV] NAME`. `REMOTE` SHALL default to `origin`; the created local branch SHALL track `REMOTE/NAME`. When revision is omitted and `REMOTE/NAME` exists remotely, creation SHALL fetch that branch and initialize the local branch at its tip. Otherwise, omitted revision SHALL retain current-HEAD and unborn-branch behavior. An explicit revision SHALL always take precedence over a matching remote branch.

#### Scenario: Existing remote branch initializes new local branch
- **WHEN** `dml branch create feature` is invoked and remote `origin` has branch `feature`
- **THEN** the system fetches `origin/feature`, creates local `feature` at that commit, and configures it to track `origin/feature`

#### Scenario: Selected remote initializes new local branch
- **WHEN** `dml branch create --remote research feature` is invoked and remote `research` has branch `feature`
- **THEN** the system creates local `feature` at the fetched `research/feature` commit and configures that upstream

#### Scenario: Explicit revision overrides remote tip
- **WHEN** `dml branch create --revision HEAD~1 feature` is invoked and `origin/feature` exists
- **THEN** local `feature` points to `HEAD~1` and tracks `origin/feature`

#### Scenario: Missing remote branch uses current head
- **WHEN** `dml branch create feature` is invoked, `origin/feature` does not exist, and HEAD resolves to a concrete commit
- **THEN** local `feature` points to the current HEAD commit and tracks `origin/feature`

#### Scenario: Branch list omits unborn current branch
- **WHEN** HEAD is attached to unborn branch `main`
- **THEN** `dml branch list` does not include `main` until that branch ref is materialized

### Requirement: Repository status reports upstream synchronization
The system SHALL report the current branch's configured upstream and ahead/behind counts relative to its local remote-tracking ref when available.

#### Scenario: Status reports configured upstream
- **WHEN** attached branch `feature` tracks `origin/main`
- **THEN** status identifies `origin/main` as its upstream

#### Scenario: Status reports synchronization counts
- **WHEN** the upstream tracking ref exists and current branch differs from it
- **THEN** status reports the computed ahead and behind counts

#### Scenario: Status reports unavailable upstream counts
- **WHEN** the current branch has no upstream or its upstream has not been fetched
- **THEN** status reports unavailable ahead and behind counts without inferring an upstream by local branch name
