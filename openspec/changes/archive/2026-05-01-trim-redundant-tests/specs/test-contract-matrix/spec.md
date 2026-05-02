## MODIFIED Requirements

### Requirement: Legacy test suite is fully migrated and superseded tests are removed
The repository SHALL complete migration of maintained tests to the contract matrix setup and SHALL remove superseded legacy tests to avoid duplicate maintenance.

#### Scenario: Superseded legacy tests are removed after parity
- **WHEN** a legacy test's contract coverage is represented by migrated contract-matrix tests
- **THEN** the legacy test is removed from maintained test paths

#### Scenario: End state contains only maintained tests aligned to taxonomy
- **WHEN** migration is complete
- **THEN** maintained tests conform to taxonomy, canonical ID, lifecycle parameterization, and slow-marker requirements defined in this specification

#### Scenario: Redundant parser smoke tests are removed once equivalent arg-level coverage exists
- **WHEN** a parser-creation smoke test duplicates parser argument assertions already maintained in the same suite
- **THEN** the redundant parser-creation smoke test is removed after parity verification

#### Scenario: Duplicate revision parsing checks are removed after central matrix adoption
- **WHEN** revision/ref/URI parsing forms are covered by the centralized parsing contract matrix
- **THEN** duplicate parsing checks in workflow-oriented contract tests are removed and workflow tests remain focused on operational invariants

#### Scenario: External-process orchestration tests are classified as slow
- **WHEN** a test requires subprocess execution, adapter polling loops, remote roundtrips, or equivalent runtime orchestration
- **THEN** the test is marked `slow` and excluded from `pytest -m "not slow"` selection

#### Scenario: Expensive adapter-path duplicates are collapsed into parameterized matrices
- **WHEN** multiple maintained tests exercise the same adapter-path contract family with near-identical setup and assertions
- **THEN** they are consolidated into one parameterized matrix suite that preserves canonical contract IDs and behavior-stage traceability
