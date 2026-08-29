## MODIFIED Requirements

### Requirement: HeadOps resolves HEAD to the active commit
The system SHALL resolve `.dml/HEAD` to the active commit when one exists. For attached HEAD, a missing current-branch ref file SHALL be treated as an unborn branch and SHALL resolve to no commit rather than to an error. For detached HEAD, the stored commit payload SHALL still resolve directly to a concrete commit.

#### Scenario: Attached HEAD resolves through local branch ref
- **WHEN** `.dml/HEAD` contains `ref: refs/local/heads/main`
- **AND** `.dml/refs/local/heads/main` exists
- **THEN** `HeadOps` resolves HEAD to the commit stored at `.dml/refs/local/heads/main`

#### Scenario: Attached HEAD resolves to unborn branch
- **WHEN** `.dml/HEAD` contains `ref: refs/local/heads/main`
- **AND** `.dml/refs/local/heads/main` does not exist
- **THEN** `HeadOps` reports attached branch `main`
- **AND** it resolves the active commit as `null` instead of failing

#### Scenario: Detached HEAD resolves directly
- **WHEN** `.dml/HEAD` contains `commit:abc123`
- **THEN** `HeadOps` resolves HEAD to `commit:abc123` without consulting any branch ref

## ADDED Requirements

### Requirement: HeadOps treats unborn HEAD as current-branch state only
The system SHALL treat a missing branch ref as unborn only for the branch currently named by attached `.dml/HEAD`. Missing other branch refs SHALL continue to fail as missing refs.

#### Scenario: Missing non-current branch ref still fails
- **WHEN** a caller asks for local branch `feature`
- **AND** `.dml/HEAD` is attached to `main`
- **AND** `.dml/refs/local/heads/feature` does not exist
- **THEN** the lookup fails as a missing branch ref
