## ADDED Requirements

### Requirement: HeadOps owns persisted checkout state
The system SHALL route `.dml/HEAD` creation, parsing, update, and commit resolution through `HeadOps` public methods rather than allowing callers to read or write the checkout-state file directly.

#### Scenario: Non-HeadOps caller needs current checkout state
- **WHEN** an internal caller needs to know whether the repository is attached or detached
- **THEN** it obtains that state through a `HeadOps` public method instead of reading `.dml/HEAD` directly

#### Scenario: Repository bootstrap creates attached HEAD
- **WHEN** repository initialization creates the initial local branch
- **THEN** `HeadOps` persists `.dml/HEAD` as `ref: refs/local/heads/<branch>` for that branch

### Requirement: HeadOps persists HEAD using two plain-text forms only
The system SHALL persist `.dml/HEAD` using exactly one of two plain-text payload forms: `ref: refs/local/heads/<branch>` for attached mode or `commit:<id>` for detached mode.

#### Scenario: Attached HEAD is written
- **WHEN** a checkout operation attaches to local branch `feature`
- **THEN** `.dml/HEAD` contains exactly `ref: refs/local/heads/feature`

#### Scenario: Detached HEAD is written
- **WHEN** a checkout operation detaches at commit `commit:abc123`
- **THEN** `.dml/HEAD` contains exactly `commit:abc123`

#### Scenario: Invalid HEAD payload fails closed
- **WHEN** `.dml/HEAD` contains any other payload form
- **THEN** `HeadOps` rejects the repository state and does not guess an alternate checkout target

### Requirement: HeadOps resolves HEAD to the active commit
The system SHALL resolve `.dml/HEAD` to a concrete commit ref by following the attached local branch ref or by returning the detached commit directly.

#### Scenario: Attached HEAD resolves through local branch ref
- **WHEN** `.dml/HEAD` contains `ref: refs/local/heads/main`
- **THEN** `HeadOps` resolves HEAD to the commit stored at `.dml/refs/local/heads/main`

#### Scenario: Detached HEAD resolves directly
- **WHEN** `.dml/HEAD` contains `commit:abc123`
- **THEN** `HeadOps` resolves HEAD to `commit:abc123` without consulting any branch ref
