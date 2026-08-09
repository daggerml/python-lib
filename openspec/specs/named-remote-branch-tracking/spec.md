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
