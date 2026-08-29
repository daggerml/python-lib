## MODIFIED Requirements

### Requirement: Legacy test suite is fully migrated and superseded tests are removed
The repository SHALL fully migrate maintained tests to the target taxonomy and remove superseded legacy tests after parity. Duplicate revision and source-selection parsing checks SHALL be removed after centralized matrix coverage exists.

#### Scenario: Duplicate revision parsing checks are removed
- **WHEN** revision grammar and source-selection forms are covered by the centralized matrix
- **THEN** workflow tests retain only operational invariants

#### Scenario: External-process tests remain slow
- **WHEN** a test requires subprocesses, polling, or remote roundtrips
- **THEN** it remains classified as slow

### Requirement: Core git-like workflow tests avoid duplicate parsing matrices
Maintained git-like workflow tests SHALL use canonical revision inputs for operational assertions and avoid duplicating revision grammar and source-selection breadth owned by the centralized matrix.

#### Scenario: Workflow test uses representative valid selector
- **WHEN** a repository workflow test needs a revision selector
- **THEN** it uses representative branch, tag, commit, or ancestry forms without a separate grammar matrix

#### Scenario: Parsing breadth remains centralized
- **WHEN** accepted or rejected revision/source forms change
- **THEN** the breadth-first cases change in the dedicated revision parsing suite

### Requirement: Core generated-input tests are bounded and fast
Generated-input tests for `daggerml._core` SHALL use Hypothesis only where generation improves confidence in accepted contract spaces and SHALL bound examples and recursive shapes for fast feedback.

#### Scenario: Accepted input spaces use bounded strategies
- **WHEN** Hypothesis is used for ref names, revision selectors, endpoint configs, config values, or serde values
- **THEN** strategies generate contractually accepted inputs with explicit bounds

#### Scenario: Recursive serde generation stays small
- **WHEN** serde round-trip tests generate nested values
- **THEN** values remain bounded to finite supported object shapes that keep tests fast
