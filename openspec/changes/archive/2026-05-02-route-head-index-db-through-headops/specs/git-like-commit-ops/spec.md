## ADDED Requirements

### Requirement: Branch-targeted commit workflows update branches through HeadOps
The system SHALL perform branch advancement in git-like commit workflows through `HeadOps` public methods rather than direct head storage access.

#### Scenario: Merge updates branch through HeadOps
- **WHEN** a branch-targeted merge needs to fast-forward or store a merge commit
- **THEN** the workflow advances the branch through `HeadOps` using the expected current commit and the new commit

#### Scenario: Revert updates branch through HeadOps
- **WHEN** a branch-targeted revert creates a new commit
- **THEN** the workflow advances the branch through `HeadOps` rather than writing the head object directly

#### Scenario: DAG checkout updates branch through HeadOps
- **WHEN** DAG checkout creates a new commit on a branch
- **THEN** the workflow advances the branch through `HeadOps` rather than writing the head object directly
