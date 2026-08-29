## ADDED Requirements

### Requirement: Revision parsing contracts are centrally owned by one matrix
The repository SHALL define namespace-independent revision parsing and source-selection behavior in one maintained parameterized contract matrix rather than duplicating grammar assertions across workflow tests.

#### Scenario: Parsing matrix is the single maintained owner
- **WHEN** maintained tests assert revision parsing or local, remote, and dependency source selection
- **THEN** those assertions live in the centralized parsing matrix

#### Scenario: Workflow contracts avoid duplicate grammar assertions
- **WHEN** workflow tests validate state transitions or side effects
- **THEN** they use canonical inputs without duplicating parsing variants

## MODIFIED Requirements

### Requirement: Revision-form matrix covers accepted and rejected local resolution boundaries
The centralized parsing matrix SHALL cover namespace-independent revision grammar and explicit local, remote, and dependency source selection. Every branch, `@tag`, `HEAD`, ancestry, direct commit ID, and exact commit-ref form SHALL be accepted with every source. `remote` and `dep` selectors SHALL be mutually exclusive and SHALL affect symbolic lookup only, never network access. Valid but unavailable combinations SHALL fail during resolution rather than parsing.

#### Scenario: Local branch resolves by default
- **WHEN** revision `main` is evaluated without a source selector
- **THEN** it resolves only from local refs

#### Scenario: Remote branch uses separate source selection
- **WHEN** revision `main` is evaluated with `remote=True`
- **THEN** it resolves from `.dml/refs/remote/heads/main`

#### Scenario: Dependency tag uses separate source selection
- **WHEN** revision `@v1` is evaluated with `dep="models"`
- **THEN** it resolves from `.dml/refs/dep/models/tags/v1`

#### Scenario: Source selectors are mutually exclusive
- **WHEN** revision resolution receives both `remote=True` and `dep="models"`
- **THEN** it fails before reading refs

#### Scenario: Unfetched selected revision fails locally
- **WHEN** selected remote or dependency tracking state does not contain the revision
- **THEN** resolution fails without connecting to an endpoint and reports that fetch is required

#### Scenario: Exact commit resolves with any source
- **WHEN** an existing direct commit is evaluated with local, remote, or dependency source selection
- **THEN** every case resolves to the same local database commit

#### Scenario: Detached HEAD ancestry resolves from HEAD file
- **WHEN** `.dml/HEAD` contains a detached commit payload and the suite evaluates local revision `HEAD~1`
- **THEN** resolution walks ancestry from the detached commit stored in `.dml/HEAD`

## REMOVED Requirements

### Requirement: Revision and URI parsing contracts are centrally owned by one parameterized matrix suite
**Reason**: DML project URIs are removed; the replacement matrix owns revision grammar and explicit source selection only.
**Migration**: Remove URI cases and add local, remote, dependency, and mutual-exclusion cases to the revision matrix.
