## ADDED Requirements

### Requirement: Dml clone bootstraps a local repo from a remote project ref
The system SHALL expose `Dml.clone(...)` as a bootstrap workflow that initializes a local repository, records branchless `remote.project`, fetches the selected remote project ref, and sets local HEAD to that cloned ref.

#### Scenario: Clone branch-qualified project URI
- **WHEN** a caller clones `dml://alice/demo#feature`
- **THEN** the system initializes the local repo
- **AND** persists `remote.project = dml://alice/demo`
- **AND** fetches remote branch `feature`
- **AND** leaves HEAD attached to local branch `feature` at the fetched commit

#### Scenario: Clone tag-qualified project URI
- **WHEN** a caller clones `dml://alice/demo@v1`
- **THEN** the system initializes the local repo
- **AND** persists `remote.project = dml://alice/demo`
- **AND** fetches remote tag `v1`
- **AND** leaves HEAD detached at the fetched commit

### Requirement: Bare project clone imputes the default branch
The system SHALL treat a bare project URI as a request to clone the configured default branch.

#### Scenario: Clone bare project URI
- **WHEN** a caller clones `dml://alice/demo`
- **THEN** the system selects branch `default.branch_name`
- **AND** fetches that branch
- **AND** leaves HEAD attached to the corresponding local branch
