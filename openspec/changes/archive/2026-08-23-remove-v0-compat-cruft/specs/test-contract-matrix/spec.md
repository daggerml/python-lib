## ADDED Requirements

### Requirement: Maintained tests use one steady-state taxonomy
The repository SHALL keep each maintained contract or integration behavior in its current subsystem-owned test location and SHALL remove superseded, duplicate, migration-only, and alias-only tests. Archived migration evidence SHALL NOT impose an active ledger or dual-suite requirement.

#### Scenario: Maintained contract has one owner
- **WHEN** a behavior is covered by the current contract or integration taxonomy
- **THEN** no superseded legacy suite duplicates that behavior

#### Scenario: Historical ledger remains archival only
- **WHEN** contributors update current tests
- **THEN** they follow the current taxonomy and contract IDs without updating an archived migration ledger

## REMOVED Requirements

### Requirement: Core test migration preserves existing coverage content
**Reason**: The migration completed before the current subsystem-owned taxonomy and no second suite remains to migrate.
**Migration**: None. Maintain current contract and integration coverage directly.

### Requirement: Legacy test suite is fully migrated and superseded tests are removed
**Reason**: This is a completed transition, not an ongoing second test mode.
**Migration**: None. The new steady-state requirement prohibits duplicate superseded tests.

### Requirement: Migration ledger governs parity and removal
**Reason**: The test taxonomy migration is complete, and retaining an archived migration ledger as permanent governance creates indefinite dual-maintenance process for tests that should already have one canonical location.
**Migration**: None. Maintained tests follow the steady-state taxonomy, preserve canonical contract IDs where applicable, and remove superseded tests in the same change once replacement coverage is verified.

### Requirement: Core rewrite removes superseded legacy tests after parity
**Reason**: The core rewrite and parity transition are complete; retaining this requirement implies an older suite still exists.
**Migration**: None. Maintain only the current subsystem-owned tests.
