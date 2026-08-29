## ADDED Requirements

### Requirement: Core git-like repository workflows are covered at the correct test boundary
Maintained `_core` tests SHALL cover git-like repository workflows using the boundary that owns the contract under test: `Dml` for caller-facing local porcelain behavior, `CommitOps` for precise commit-graph and DAG-tree edge behavior, and `_core` integration tests for remote sync workflows.

#### Scenario: Local repository porcelain is tested through Dml
- **WHEN** maintained tests verify local git-like workflows such as `status`, `rev_parse`, `log`, `show`, `diff`, `checkout`, `merge`, `rebase`, `revert`, `dag.checkout`, or `dag.delete`
- **THEN** those tests live under `tests/_core/contracts/`
- **AND** they exercise the behavior through `Dml` unless the contract specifically depends on lower-level commit-graph setup that is clearer below the porcelain boundary

#### Scenario: Commit-graph edge behavior is tested through CommitOps
- **WHEN** maintained tests verify merge-base selection, first-parent ancestry walking, merge conflicts, revert conflicts, rebase replay, commit-relative diff behavior, or DAG-tree overwrite/delete edge cases
- **THEN** the tests MAY exercise `CommitOps` directly under `tests/_core/contracts/`
- **AND** they focus on exact graph and tree outcomes rather than re-testing `Dml` payload shaping

#### Scenario: Remote sync workflows are classified as integration behavior
- **WHEN** maintained tests verify `push`, `fetch`, or `pull`, or otherwise require remote protocol state, remote-tracking refs, or moto-backed S3 orchestration
- **THEN** those tests live under `tests/_core/integration/`
- **AND** they are marked `slow` according to the integration marker policy

### Requirement: Core git-like workflow tests avoid duplicate parsing matrices
Maintained `_core` git-like workflow tests SHALL rely on canonical valid revision inputs for operational assertions and SHALL avoid duplicating parsing-grammar breadth already owned by the centralized revision parsing matrix.

#### Scenario: Workflow test uses representative valid selector
- **WHEN** a checkout, merge, show, diff, fetch, pull, or push contract test needs a revision selector
- **THEN** the test uses one or more representative valid selector forms needed for that workflow
- **AND** it does not expand into a separate grammar matrix for branch, tag, commit, and URI parsing permutations already owned elsewhere

#### Scenario: Parsing breadth remains centralized
- **WHEN** maintainers need to add or adjust accepted and rejected selector forms for revision parsing
- **THEN** the maintained breadth-first matrix lives in the dedicated revision parsing contract suite
- **AND** git-like workflow suites remain focused on side effects, state transitions, and repository invariants
