## ADDED Requirements

### Requirement: Contract-first test taxonomy
The repository SHALL organize maintained tests by contract intent with distinct locations for fast invariant checks and integration behavior.

#### Scenario: Fast contract tests live under contracts taxonomy
- **WHEN** a test verifies a documented contract or invariant in isolation
- **THEN** it is placed under `tests/contracts/`

#### Scenario: Integration tests live under integration taxonomy
- **WHEN** a test exercises multi-component behavior, external processes, remote roundtrips, or runtime orchestration
- **THEN** it is placed under `tests/integration/`

### Requirement: Canonical contract IDs are embedded directly in test identifiers
Each maintained contract-focused test SHALL include a canonical contract ID expressed as a direct literal string in test naming surfaces.

#### Scenario: Parameterized lifecycle case includes canonical ID
- **WHEN** a test case is defined in `pytest.mark.parametrize`
- **THEN** the case `id=` string includes the canonical contract ID followed by a human-readable case label

#### Scenario: Canonical IDs avoid indirection
- **WHEN** a test references a canonical contract ID
- **THEN** the ID is specified directly in the test or parameterized case definition and does not require a shared ID registry indirection

### Requirement: Lifecycle coverage uses parameterized stage matrices
Lifecycle-oriented contracts SHALL be tested with parameterized cases that explicitly represent each lifecycle stage.

#### Scenario: Lifecycle stages are represented as explicit parameterized cases
- **WHEN** a contract family spans kickoff, resume/poll, and terminal behavior
- **THEN** one parameterized test defines stage-specific cases with distinct IDs and assertions for each stage

#### Scenario: Stage-specific failures identify contract and stage
- **WHEN** a lifecycle parameterized case fails
- **THEN** the failure node identifier includes both canonical contract ID and stage label

### Requirement: Integration tests are marked slow
Integration tests SHALL be marked `@pytest.mark.slow` so they can be excluded from quick local runs.

#### Scenario: Integration test carries slow marker
- **WHEN** a test resides in the integration taxonomy or otherwise requires integration-level runtime behavior
- **THEN** the test is marked `@pytest.mark.slow`

#### Scenario: Fast test selection excludes integration tests
- **WHEN** contributors run `pytest -m "not slow"`
- **THEN** tests marked `slow` are excluded and the remaining suite represents the fast-path contract checks

### Requirement: Legacy test suite is fully migrated and superseded tests are removed
The repository SHALL complete migration of maintained tests to the contract matrix setup and SHALL remove superseded legacy tests to avoid duplicate maintenance.

#### Scenario: Superseded legacy tests are removed after parity
- **WHEN** a legacy test's contract coverage is represented by migrated contract-matrix tests
- **THEN** the legacy test is removed from maintained test paths

#### Scenario: End state contains only maintained tests aligned to taxonomy
- **WHEN** migration is complete
- **THEN** maintained tests conform to taxonomy, canonical ID, lifecycle parameterization, and slow-marker requirements defined in this specification

### Requirement: Migration ledger governs parity and removal
The repository SHALL track migration progress in a ledger that maps canonical contract IDs from legacy tests to migrated tests and records parity evidence before legacy removal.

#### Scenario: Batch plan records concrete suite order and risk
- **WHEN** migration planning is established
- **THEN** the ledger records bounded batch order with risk levels and exit criteria for each batch

#### Scenario: Contract mapping is explicit for each migrated suite
- **WHEN** a suite is selected for migration
- **THEN** the ledger records canonical contract IDs and old/new test file mappings for that suite

#### Scenario: Legacy test removal requires parity evidence
- **WHEN** a legacy suite is proposed for removal
- **THEN** the ledger includes passing evidence for targeted migrated suites, `pytest -m "not slow"`, and full `pytest` prior to removal
