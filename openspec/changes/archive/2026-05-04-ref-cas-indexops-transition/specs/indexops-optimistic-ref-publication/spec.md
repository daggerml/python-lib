## ADDED Requirements

### Requirement: IndexOps publishes index mutations through post-transaction compare-and-swap
The system SHALL have affected `IndexOps` mutation paths derive new commits in LMDB before publishing them through `HeadOps` compare-and-swap operations on file-backed index or branch refs.

#### Scenario: Index mutation publishes after LMDB commit
- **WHEN** an `IndexOps` mutation updates an existing index state
- **THEN** it reads the base commit through `HeadOps`, writes the new immutable commit in an LMDB write transaction, closes that transaction, and only then asks `HeadOps` to compare-and-swap the index ref to the new commit

### Requirement: IndexOps retries from the current stored commit after stale ref conflicts
The system SHALL retry affected `IndexOps` mutation paths when `HeadOps` reports a stale pointer conflict, using the conflict's current stored commit as the next base commit.

#### Scenario: Index compare-and-swap loses a race
- **WHEN** `HeadOps.update_index_commit` rejects an `IndexOps` publication attempt with `DmlPointerConflictError(current_commit=commit:new)`
- **THEN** `IndexOps` starts a fresh LMDB write transaction using `commit:new` as the base commit and rebuilds the mutation before retrying publication

### Requirement: Branch-targeted index commits publish branch movement after commit creation
The system SHALL publish branch advancement for `IndexOps.commit(..., head=...)` only after the new commit has been durably created in LMDB.

#### Scenario: Branch-backed commit finalization
- **WHEN** `IndexOps.commit` finalizes a working index onto a branch
- **THEN** it writes the new commit in LMDB, closes the LMDB transaction, and only then asks `HeadOps` to advance the branch from the expected old commit to the new commit

### Requirement: Detached scratch commit helpers do not create temporary index refs
The system SHALL build builtin and failed-execution scratch commit state without publishing temporary index refs under `.dml/refs/local/indexes`.

#### Scenario: Builtin helper constructs scratch commit
- **WHEN** builtin execution needs a temporary DAG commit to materialize a result
- **THEN** the helper builds detached scratch commit state directly in LMDB and returns the resulting DAG/commit refs without creating or deleting a temporary index ref file
