## MODIFIED Requirements

### Requirement: Fetch explicitly selects project root or dependency
The system SHALL implement `dml fetch [--dep DEP] [--depth N | --unshallow] [BRANCH|@TAG]`. It SHALL use resolved `remote.root` when `--dep` is absent, use the named dependency endpoint when supplied, and use branch `default.branch_name` when no ref selector is supplied. A positive depth SHALL bound commit ancestry while preserving complete included snapshots, `--unshallow` SHALL materialize all ancestry reachable from the selected ref, and the two options SHALL be mutually exclusive. `dep add` SHALL remain configuration-only and SHALL NOT persist a default history depth.

#### Scenario: Fetch project branch by default
- **WHEN** a user runs `dml fetch feature`
- **THEN** only `.dml/refs/remote/heads/feature` is updated from `remote.root`

#### Scenario: Fetch shallow project branch
- **WHEN** a user runs `dml fetch --depth 2 feature`
- **THEN** the feature tip and two commit generations with complete snapshots are locally available
- **AND** only `.dml/refs/remote/heads/feature` is updated

#### Scenario: Fetch dependency default branch
- **WHEN** a user runs `dml fetch --dep models`
- **THEN** only `.dml/refs/dep/models/heads/<default.branch_name>` is updated from dependency `models`

#### Scenario: Fetch shallow dependency branch
- **WHEN** a user runs `dml fetch --dep models --depth 1`
- **THEN** the dependency tip and complete current DAG snapshot are locally available without materializing otherwise-unavailable parent commits

#### Scenario: Unshallow dependency tag
- **WHEN** a user runs `dml fetch --dep models --unshallow @v1`
- **THEN** every commit reachable from dependency tag `v1` is locally available and its tracking tag is updated

#### Scenario: Fetch dependency tag
- **WHEN** a user runs `dml fetch --dep models @v1`
- **THEN** only `.dml/refs/dep/models/tags/v1` is updated
