## MODIFIED Requirements

### Requirement: DML URIs track fetched remote refs
The system SHALL track fetched remote branches and tags locally by canonical normalized DML URI.

#### Scenario: Store fetched branch tracking ref
- **WHEN** `dml fetch dml://alice/tools#main` succeeds
- **THEN** local storage tracks `dml://alice/tools#main` as pointing to the resolved commit

#### Scenario: Store fetched tag tracking ref
- **WHEN** `dml fetch dml://alice/tools@v1.0` succeeds
- **THEN** local storage tracks `dml://alice/tools@v1.0` as pointing to the resolved commit

#### Scenario: Tracking ref stores commit pointer
- **WHEN** a fetched remote ref is persisted locally
- **THEN** the persisted tracking ref contains the resolved commit pointer

#### Scenario: Canonical URI head is stored
- **WHEN** a remote fetch resolves project `alice/tools` branch `main`
- **THEN** the local tracking ref is stored under canonical URI `dml://alice/tools#main`

#### Scenario: Derived expression is not stored as URI head
- **WHEN** a remote operation resolves a derived expression such as `HEAD~2`
- **THEN** the system stores only the canonical project branch or tag URI for any tracking head it writes

#### Scenario: URI tracking ref length is validated
- **WHEN** a command would create a tracking ref whose canonical DML URI exceeds 64 bytes
- **THEN** the command fails without writing the tracking ref

#### Scenario: Overlong URI is rejected directly
- **WHEN** a canonical DML URI exceeds 64 bytes
- **THEN** the system rejects it and does not hash or rewrite it into an alternate tracking key

#### Scenario: URI tracking ref characters are validated explicitly
- **WHEN** a command would create a DML URI tracking ref
- **THEN** the system validates the canonical URI as a DML project URI before writing the tracking ref

#### Scenario: User-facing DML URI resolves to local tracking ref
- **WHEN** a user-facing command receives `dml://alice/tools#main`
- **THEN** the command resolves it locally through the tracking ref for `dml://alice/tools#main`

### Requirement: Remote operations parse DML URIs
The system SHALL parse and canonicalize DML revision URIs through one centralized shared revision URI parser/stringifier boundary before deriving remote project ref paths.

#### Scenario: Push parses branch URI through shared parser
- **WHEN** push targets canonical URI `dml://alice/demo#main`
- **THEN** remote operations derive `refs/projects/alice/demo/heads/main.json` from the shared parsed revision object

#### Scenario: Fetch parses tag URI through shared parser
- **WHEN** fetch targets canonical URI `dml://alice/demo@v1.0`
- **THEN** remote operations derive `refs/projects/alice/demo/tags/v1.0.json` from the shared parsed revision object

#### Scenario: Branch and tag capability checks remain operation-specific
- **WHEN** a mutation operation targets the wrong selector type for its command
- **THEN** the operation fails at the command boundary even though URI parsing and canonicalization succeed

### Requirement: Fetch updates fetched remote refs by canonical DML selector
The system SHALL fetch a remote project branch or tag by reading its remote ref, materializing the referenced commit closure locally, and updating the corresponding local fetched remote ref.

#### Scenario: Fetch explicit project URI
- **WHEN** `dml fetch dml://alice/tools#main` succeeds
- **THEN** local storage contains the fetched commit closure and tracks `dml://alice/tools#main` as pointing to the fetched commit

#### Scenario: Fetch explicit project tag URI
- **WHEN** `dml fetch dml://alice/tools@v1.0` succeeds
- **THEN** local storage contains the fetched commit closure and tracks `dml://alice/tools@v1.0` as pointing to the fetched commit

#### Scenario: Fetch project-relative branch through configured project
- **WHEN** `remote.project` is `dml://alice/tools` and a user runs `dml fetch #main`
- **THEN** the system fetches `dml://alice/tools#main` and updates that fetched remote ref locally

### Requirement: Pull fetches and merges same-name fetched branch
The system SHALL implement branch pull as fetch followed by merge of the same-name fetched remote branch into the current attached local branch.

#### Scenario: Pull attached main
- **WHEN** the current branch is `main` and the user runs `dml pull`
- **THEN** the system fetches `dml://alice/demo#main`
- **AND** merges that fetched remote branch into local branch `main`

#### Scenario: Pull detached head fails
- **WHEN** the current checkout is detached and the user runs `dml pull`
- **THEN** pull fails without fetching or merging into an implicit branch target

### Requirement: Push updates or deletes project refs using explicit selector semantics
The system SHALL update remote project refs using revision parsing and SHALL support remote deletion through `push --delete <revision>`.

#### Scenario: Push attached branch by default
- **WHEN** the current branch is `main` and the user runs `dml push`
- **THEN** the system publishes local branch `main` to remote branch `dml://alice/demo#main`

#### Scenario: Push explicit local tag
- **WHEN** a user runs `dml push @v1`
- **THEN** the system publishes local tag `v1` to remote tag `dml://alice/demo@v1`

#### Scenario: Push delete branch selector
- **WHEN** a user runs `dml push --delete #feature`
- **THEN** the system resolves that selector as the configured project's remote branch `dml://alice/demo#feature`
- **AND** deletes that remote branch ref

#### Scenario: Push delete explicit remote tag selector
- **WHEN** a user runs `dml push --delete dml://alice/demo@v1`
- **THEN** the system deletes that remote tag ref

## REMOVED Requirements

### Requirement: Push uses ETag and fast-forward safety
**Reason**: The new ref-lifecycle proposal intentionally focuses first on coherent command and ref semantics rather than preserving the older create/force/non-fast-forward contract wording.
**Migration**: Reintroduce explicit safety semantics in a later focused change if they remain desired after the harmonized ref model lands.
