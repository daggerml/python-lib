## MODIFIED Requirements

### Requirement: Dml clone bootstraps a local repo from a remote project ref
The system SHALL expose `Dml.clone(...)` as a bootstrap workflow that initializes a local repo, records the cloned project as named remote `origin`, fetches the selected remote project ref, and sets local HEAD to that cloned ref. A cloned branch SHALL track `origin/<branch>`.

#### Scenario: Clone branch-qualified project URI
- **WHEN** a caller clones `dml://alice/demo#feature`
- **THEN** the system initializes the local repo
- **AND** persists remote `origin` as `dml://alice/demo`
- **AND** fetches remote branch `feature`
- **AND** leaves HEAD attached to local branch `feature` at the fetched commit
- **AND** configures `feature` to track `origin/feature`

#### Scenario: Clone tag-qualified project URI
- **WHEN** a caller clones `dml://alice/demo@v1`
- **THEN** the system initializes the local repo
- **AND** persists remote `origin` as `dml://alice/demo`
- **AND** fetches remote tag `v1`
- **AND** leaves HEAD detached at the fetched commit

### Requirement: Bare project clone imputes the default branch
The system SHALL treat a bare project URI as a request to clone the configured default branch and configure the resulting local branch to track that branch on `origin`.

#### Scenario: Clone bare project URI
- **WHEN** a caller clones `dml://alice/demo`
- **THEN** the system selects branch `default.branch_name`
- **AND** fetches that branch
- **AND** leaves HEAD attached to the corresponding local branch
- **AND** configures that branch to track `origin/<default.branch_name>`
