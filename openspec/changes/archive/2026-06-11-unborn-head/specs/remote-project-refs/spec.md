## MODIFIED Requirements

### Requirement: Init creates initial branch and attaches HEAD
The system SHALL initialize local project state by attaching `.dml/HEAD` to the initial branch without creating a synthetic initial commit. The initial branch ref SHALL remain absent until the first real branch commit is created.

#### Scenario: Init creates unborn attached HEAD
- **WHEN** `dml init demo` succeeds
- **THEN** the system creates `demo/.dml/`, `demo/.dml/config.toml`, `.dml/HEAD`, and local database storage under `demo/.dml/db/`
- **AND** `.dml/HEAD` is attached to the default branch
- **AND** the corresponding local branch ref file does not exist yet

#### Scenario: Init does not create initial empty commit
- **WHEN** `dml init demo` succeeds
- **THEN** local storage does not contain a synthetic initial empty commit solely to materialize the branch tip

#### Scenario: Detached init without commit is rejected
- **WHEN** init is requested in detached mode before any commit exists
- **THEN** init fails because detached HEAD requires a concrete commit
