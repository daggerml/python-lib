## ADDED Requirements

### Requirement: Branch upstream inspection SHALL accept any local branch name
The system SHALL expose the configured remote-root upstream branch for an arbitrary valid branch name, independent of the current checkout. A configured lookup SHALL return `UpstreamInfo` with exact shape `{branch: str}`. A valid branch name with no upstream association SHALL return `None`, including when no local ref currently exists for that name. Invalid branch names SHALL fail validation, and malformed persisted upstream metadata SHALL raise `DmlRepoError` rather than returning partial or coerced data.

#### Scenario: Inspect non-current branch upstream
- **WHEN** local branch `feature` tracks remote-root branch `main` while another branch is checked out
- **THEN** upstream inspection for `feature` returns branch `main`

#### Scenario: Inspect branch without upstream
- **WHEN** local branch `feature` has no upstream association
- **THEN** upstream inspection for `feature` returns `None`

#### Scenario: Valid unknown branch has no association
- **WHEN** no local ref or upstream association exists for valid branch name `missing`
- **THEN** upstream inspection for `missing` returns `None`

#### Scenario: Invalid branch name fails validation
- **WHEN** upstream inspection receives an invalid branch name
- **THEN** it fails without reading an upstream association

#### Scenario: Malformed upstream metadata fails closed
- **WHEN** persisted upstream metadata for `feature` does not have exact shape `{branch: str}` with a valid branch value
- **THEN** upstream inspection raises `DmlRepoError`

### Requirement: Upstream inspection SHALL remain branch-only
The system SHALL expose arbitrary upstream inspection only for local branches. It SHALL NOT add upstream metadata or an upstream lookup operation to tags or dependencies.

#### Scenario: Tag surface has no upstream lookup
- **WHEN** a caller inspects the tag namespace
- **THEN** no tag upstream lookup operation is exposed
