## MODIFIED Requirements

### Requirement: DML URIs track fetched remote refs
The system SHALL track fetched remote branches and tags locally by configured remote name and branch or tag name. A remote-tracking selector SHALL use `<remote-name>/<branch-name>` for branches and `<remote-name>@<tag-name>` for tags.

#### Scenario: Store fetched branch tracking ref
- **WHEN** `dml fetch origin` fetches remote branch `main`
- **THEN** local storage tracks it as `origin/main` pointing to the resolved commit

#### Scenario: Store fetched tag tracking ref
- **WHEN** `dml fetch origin` fetches remote tag `v1.0`
- **THEN** local storage tracks it as `origin@v1.0` pointing to the resolved commit

#### Scenario: Tracking ref stores commit pointer
- **WHEN** a fetched remote ref is persisted locally
- **THEN** the persisted tracking ref contains the resolved commit pointer

#### Scenario: Remote tracking selector resolves locally
- **WHEN** a user-facing command receives `origin/main`
- **THEN** the command resolves it locally through the tracking ref for `origin/main`

### Requirement: Fetch updates remote-tracking heads
The system SHALL fetch all branch and tag refs for a configured named remote, materialize each referenced commit closure locally, and update the corresponding local remote-tracking refs. `fetch` SHALL accept at most one optional remote name and SHALL default to `origin`. A branch- or tag-qualified DML project URI SHALL instead fetch only that addressed ref and update its URI-keyed tracking ref.

#### Scenario: Fetch default origin
- **WHEN** `dml fetch` succeeds and `origin` has branches `main` and `feature` plus tag `v1`
- **THEN** local tracking refs for `origin/main`, `origin/feature`, and `origin@v1` are updated

#### Scenario: Fetch selected remote
- **WHEN** `dml fetch research` succeeds
- **THEN** it updates tracking refs for every branch and tag in remote `research` without updating other remotes

#### Scenario: Unknown remote fails
- **WHEN** `dml fetch unknown` is requested
- **THEN** the command fails without changing local tracking refs

#### Scenario: Fetch explicit project ref
- **WHEN** `dml fetch dml://alice/research#main` succeeds
- **THEN** local storage updates the URI-keyed tracking ref for `dml://alice/research#main` without requiring a configured named remote

### Requirement: Pull fetches and merges the configured upstream
The system SHALL implement branch pull as fetching the current attached branch's configured upstream remote followed by merge of that upstream tracking ref into the current branch. Pull SHALL accept no positional remote or branch argument.

#### Scenario: Pull configured upstream
- **WHEN** current local branch `feature` tracks `origin/main` and `dml pull` succeeds
- **THEN** `origin/main` is refreshed and `feature` advances to the merge result or fetched commit when fast-forwardable

#### Scenario: Pull untracked branch fails
- **WHEN** the current attached branch has no configured upstream
- **THEN** `dml pull` fails without fetching or advancing the branch

#### Scenario: Pull remote argument is rejected
- **WHEN** a user supplies a positional argument to `dml pull`
- **THEN** command parsing rejects the invocation

### Requirement: Project sync requires a configured named remote
The system SHALL require a configured named remote before default project-addressed synchronization. `origin` SHALL be the default named remote for `fetch` and for first publication of an untracked branch.

#### Scenario: Default sync without origin
- **WHEN** a repository has remote storage but no remote named `origin` and default fetch or first branch publication is requested
- **THEN** the operation fails with a descriptive error stating that `origin` is required

#### Scenario: Named upstream does not require origin
- **WHEN** the current branch tracks `research/main` and remote `research` is configured
- **THEN** pull and push use `research` without requiring `origin`
