## MODIFIED Requirements

### Requirement: Merge advances current head
The system SHALL merge another commit or branch into the current branch by creating a merge commit when needed and advancing the current head. When the current attached head has no resolved commit because the branch is unborn, merge SHALL treat that destination as empty history and advance the current head to the merged commit without requiring a synthetic base commit.

#### Scenario: Merge non-conflicting branch
- **WHEN** a user merges a branch whose tree changes do not conflict with the current branch
- **THEN** the system creates a merge commit with both commits as parents and advances the current head to that merge commit

#### Scenario: Merge fast-forward
- **WHEN** the current branch head is an ancestor of the merged commit
- **THEN** the system advances the current head to the merged commit without creating an unnecessary merge commit

#### Scenario: Merge into unborn attached head
- **WHEN** `.dml/HEAD` is attached to local branch `main`
- **AND** branch `main` has no materialized commit ref yet
- **AND** the merged revision resolves to commit `commit:abc123`
- **THEN** the system advances branch `main` directly to `commit:abc123`
- **AND** it does not require or create a synthetic initial commit

### Requirement: Branch creation and listing expose git-like branch inspection workflows
The system SHALL support creating a local branch from the current HEAD commit when one exists, and SHALL preserve git-like unborn-branch behavior when the current attached branch has no commit yet. Local branch listing SHALL continue to report only materialized local branch refs.

#### Scenario: Branch remote lists tracked refs
- **WHEN** a user runs `dml branch --remote`
- **THEN** the system returns the set of locally tracked remote branch selectors

#### Scenario: Branch create copies the current head commit without moving HEAD
- **WHEN** a caller invokes `dml.branch("feature")` while HEAD is attached to `main`
- **AND** `main` resolves to a concrete commit
- **THEN** the system creates local branch `feature` at the current HEAD commit
- **AND** HEAD remains attached to `main`

#### Scenario: Branch create repoints unborn attached HEAD
- **WHEN** a caller invokes `dml.branch("feature")` while HEAD is attached to unborn branch `main`
- **THEN** the system rewrites `.dml/HEAD` to attach to `feature`
- **AND** it does not create `.dml/refs/local/heads/feature`

#### Scenario: Branch list omits unborn current branch
- **WHEN** HEAD is attached to unborn branch `main`
- **THEN** `dml branch list` does not include `main` until that branch ref is materialized

## ADDED Requirements

### Requirement: First branch commit materializes an unborn branch ref
The system SHALL materialize the current branch ref when the first history-producing commit is finalized on an attached unborn branch.

#### Scenario: First named commit on unborn branch writes ref
- **WHEN** HEAD is attached to unborn branch `main`
- **AND** runtime finalization produces the first history commit `commit:abc123`
- **THEN** the system writes `.dml/refs/local/heads/main` pointing to `commit:abc123`
- **AND** HEAD remains attached to `main`
