## MODIFIED Requirements

### Requirement: Show returns commit metadata, full DAG map, and commit delta
`dml show <revision>` SHALL resolve the revision locally and return JSON with top-level `revision`, `commit`, `dags`, and `change` fields.

The `commit` field SHALL expose commit metadata rooted in the resolved commit record and SHALL NOT include `dag`. The `dags` field SHALL be the full DAG name-to-ref map for the resolved revision. The `change` field SHALL describe the DAG-map delta introduced by the resolved commit relative to its base commit.

#### Scenario: Show returns full DAG map and change
- **WHEN** a user runs `dml show HEAD`
- **THEN** the command returns JSON containing `revision`, `commit`, `dags`, and `change`
- **AND** `dags` contains the complete DAG map for the resolved commit
- **AND** `commit` does not include `dag`

#### Scenario: Show root commit uses empty base
- **WHEN** a user runs `dml show` on a root commit with no parents
- **THEN** `change.base` is `null`
- **AND** every DAG in `dags` appears as an addition in `change`

#### Scenario: Show merge commit uses first parent as base
- **WHEN** a user runs `dml show` on a merge commit with multiple parents
- **THEN** `change` is computed relative to the first parent commit

### Requirement: Log returns commit entries for a revision walk
`dml log [<revision>] [--limit N]` SHALL return commit entries starting from the resolved revision, defaulting to `HEAD`.

Each returned commit entry SHALL use the same commit-metadata shape as `dml show` and SHALL NOT include `dag`.

#### Scenario: Log defaults to HEAD
- **WHEN** a user runs `dml log`
- **THEN** the command resolves `HEAD`
- **AND** returns JSON containing `revision` and `commits`

#### Scenario: Log entries omit commit dag
- **WHEN** a user runs `dml log`
- **THEN** every entry in `commits` omits `dag`
