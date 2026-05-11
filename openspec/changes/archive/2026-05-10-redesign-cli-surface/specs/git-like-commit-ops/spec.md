## ADDED Requirements

### Requirement: Repository inspection workflows resolve revisions locally
The system SHALL provide repository inspection workflows for `show`, `log`, and `diff` that resolve revisions locally without performing implicit network fetches.

#### Scenario: Show resolves revision locally
- **WHEN** a user runs `dml show origin/main`
- **THEN** the system resolves `origin/main` through existing local tracking state
- **AND** it does not contact the remote automatically

#### Scenario: Diff resolves both revisions locally
- **WHEN** a user runs `dml diff dml://alice/demo#main HEAD`
- **THEN** the system resolves both revisions from local state only

### Requirement: Branch listing exposes remote-tracking branches
The system SHALL support listing locally tracked remote branches for git-like branch inspection.

#### Scenario: Branch remote lists tracked refs
- **WHEN** a user runs `dml branch --remote`
- **THEN** the system returns the set of locally tracked remote branch selectors

### Requirement: Repository status reports current DAG map and live indexes
The system SHALL provide a repository status workflow that reports the current HEAD state, local branches, the DAG map for the current revision, and live indexes.

#### Scenario: Status reports attached head
- **WHEN** HEAD is attached to branch `main` and a user runs `dml status`
- **THEN** the response reports attached head state for `main`
- **AND** includes the DAG map for the commit selected by that head

#### Scenario: Status reports detached head
- **WHEN** HEAD is detached and a user runs `dml status`
- **THEN** the response reports detached head state and the current commit

### Requirement: Show returns commit delta over DAG namespace
The system SHALL compute commit-introduced change for `dml show` as DAG-map additions, removals, and updates between the selected commit tree and its base tree.

#### Scenario: Show detects DAG addition
- **WHEN** a commit introduces `train -> dag:a` where the base tree had no `train`
- **THEN** `dml show` reports `train` under `change.added`

#### Scenario: Show detects DAG update
- **WHEN** a commit changes `train` from `dag:a` to `dag:b`
- **THEN** `dml show` reports `train` under `change.updated` with `before` and `after`

#### Scenario: Show detects DAG removal
- **WHEN** a commit removes `train -> dag:a`
- **THEN** `dml show` reports `train` under `change.removed`
