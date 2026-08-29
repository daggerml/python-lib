## Purpose

Provide remote-root branch upstreams so synchronizing local branches is reliable when local and remote names differ.

## Requirements

### Requirement: Branch upstream lifecycle
The system SHALL allow an attached local branch to configure one upstream branch name on `remote.root`. Changing or removing a local branch SHALL update its upstream association so no association remains for a deleted branch and a renamed branch retains its upstream. Upstream state SHALL NOT store an endpoint name, root, or dependency.

#### Scenario: Set current branch upstream
- **WHEN** attached local branch `feature` sets its upstream to branch `main`
- **THEN** later pull and push operations for `feature` target branch `main` at resolved `remote.root`

#### Scenario: Rename branch preserves upstream
- **WHEN** local branch `feature` tracking remote branch `main` is renamed to `review`
- **THEN** `review` continues to track remote branch `main`

#### Scenario: Delete branch removes upstream
- **WHEN** a non-current local branch with an upstream is deleted
- **THEN** no upstream association remains for that local branch

#### Scenario: Dependency cannot be an upstream
- **WHEN** a caller attempts to set dependency state as a branch upstream
- **THEN** the operation fails without changing the branch upstream

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
