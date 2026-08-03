## Purpose

Provide named remote projects and explicit per-branch upstreams so synchronizing local branches is reliable even when local and remote names differ.

## Requirements

### Requirement: Named remote lifecycle
The system SHALL allow a repository to add, list, and delete named remotes, where each remote name identifies one branchless DML project URI. Remote names MUST not contain `/`. A repository initialized or cloned with legacy `remote.project` configuration SHALL expose that project as remote `origin`.

#### Scenario: Add and list a remote
- **WHEN** a user adds remote `research` for `dml://alice/research`
- **THEN** listing remotes includes `research` and its project URI

#### Scenario: Delete a remote
- **WHEN** a user deletes remote `research`
- **THEN** `research` is no longer available for fetch or upstream configuration

#### Scenario: Reject an ambiguous remote name
- **WHEN** a user adds a remote whose name contains `/`
- **THEN** the operation fails without changing remote configuration

### Requirement: Branch upstream lifecycle
The system SHALL allow an attached local branch to configure one upstream as `<remote-name>/<branch-name>`. Changing or removing a local branch SHALL update its upstream association so no association remains for a deleted branch and a renamed branch retains its upstream.

#### Scenario: Set current branch upstream
- **WHEN** attached local branch `feature` sets its upstream to `origin/main`
- **THEN** later pull and push operations for `feature` target remote `origin` branch `main`

#### Scenario: Rename branch preserves upstream
- **WHEN** local branch `feature` tracking `origin/main` is renamed to `review`
- **THEN** `review` continues to track `origin/main`

#### Scenario: Delete branch removes upstream
- **WHEN** a non-current local branch with an upstream is deleted
- **THEN** no upstream association remains for that local branch
