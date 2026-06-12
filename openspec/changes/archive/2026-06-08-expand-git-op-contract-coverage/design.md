## Context

The current `_core` contract suite proves pointer validation and revision parsing basics, but it leaves most git-like repository behavior either uncovered or only indirectly exercised through a few integration tests. The missing surface spans several layers:

- `dml.py` owns the user-facing porcelain for status, revision resolution, checkout, merge, rebase, revert, and remote sync.
- `commit.py` owns DAG-tree diffs, merge/rebase/revert graph behavior, commit inspection, and DAG checkout/delete mechanics.
- `head.py` owns attached-vs-detached HEAD state and local/remote pointer persistence.
- `remote.py` participates in `push`, `fetch`, and `pull`, which are real integration workflows rather than pure local contracts.

This proposal is still a test-suite change only. It does not introduce new repository behavior; it makes the existing behavior legible and enforceable through maintained tests.

## Goals / Non-Goals

**Goals:**

- Add readable maintained coverage for git-like `_core` workflows at the right abstraction boundary.
- Test user-facing local repository workflows through `Dml` rather than reconstructing implementation details in every test.
- Use direct `CommitOps` tests only for graph-shape, diff, and conflict cases that are clearer or more precise below the porcelain layer.
- Keep remote sync workflows in `_core` integration tests and mark them according to the repository integration policy.
- Reuse the centralized revision-parsing ownership split so workflow tests stay focused on repository invariants rather than selector grammar.
- Remove or reduce superseded repo-op tests after parity is represented by the new contract and integration suites.

**Non-Goals:**

- Do not change production semantics for git-like repository operations as part of the proposal itself.
- Do not duplicate parsing-matrix coverage for branch/tag/URI grammar variants already owned elsewhere.
- Do not force all remote behavior into fast contract tests.
- Do not pursue broad line coverage of `commit.py`, `head.py`, or `dml.py`; the focus is meaningful contract coverage.

## Decisions

### Use `Dml` For Porcelain Contracts And `CommitOps` For Precise Graph Cases

Fast maintained tests should exercise local repository workflows primarily through `Dml`, because that is the orchestration boundary responsible for attached-vs-detached rules, local revision resolution, and branch advancement. Direct `CommitOps` tests should be limited to cases where we want precise control over commit ancestry, merge bases, conflicting DAG-name edits, or revert/rebase edge conditions.

This keeps tests aligned with the actual caller boundary while still allowing sharply targeted coverage of lower-level history logic.

Alternative considered: test everything through `CommitOps` only. That would miss `Dml`-owned invariants such as detached-head rejection, status payload shaping, and local-only revision resolution.

### Split Local Contracts From Remote Sync Integration

`status`, `rev_parse`, `log`, `show`, `diff`, `checkout`, `merge`, `rebase`, `revert`, `dag.checkout`, and `dag.delete` should live under `tests/_core/contracts/` as fast local repository tests. `push`, `fetch`, and `pull` should live under `tests/_core/integration/` because they depend on remote protocol behavior, remote-tracking refs, and moto-backed S3 flows.

This matches the existing test taxonomy and avoids turning contract tests into disguised remote integration tests.

Alternative considered: place all git-like workflows under one contract suite. That would blur the boundary between local invariants and remote orchestration, and would weaken the usefulness of `pytest -m "not slow"`.

### Organize Files By Behavior Family Rather Than Source Module

The new tests should be grouped by repository behavior families rather than mirroring source modules one-for-one.

Proposed contract files:

- `tests/_core/contracts/test_revision_resolution_contracts.py`
- `tests/_core/contracts/test_history_queries_contracts.py`
- `tests/_core/contracts/test_checkout_contracts.py`
- `tests/_core/contracts/test_merge_rebase_revert_contracts.py`
- `tests/_core/contracts/test_dag_tree_contracts.py`
- `tests/_core/contracts/test_head_refs.py` (expanded rather than replaced)

Proposed integration file:

- `tests/_core/integration/test_remote_repo_sync_integration.py`

Alternative considered: one `test_git_ops_contracts.py` file. That would make failures harder to localize and would encourage oversized fixtures spanning unrelated concerns.

### Reuse Existing Helpers And Keep Parsing Ownership Centralized

The implementation should reuse `tests/_core/helpers.py`, `tests/_core/conftest.py`, the existing moto-backed remote fixtures, and the current revision/ref strategies where they fit. Workflow tests should use representative valid selectors and rely on `tests/_core/contracts/test_revision_selectors.py` for parsing breadth rather than duplicating grammar matrices inside checkout or merge tests.

Alternative considered: new dedicated parsing permutations inside each repo-op suite. That would recreate the duplication the central parsing matrix was meant to prevent.

### Treat Legacy Repo-Op Tests As Replaceable Once Parity Exists

Where an existing integration or legacy `_core` test only overlaps the new repo-op coverage, the maintained suite should prefer the behavior-named contract/integration tests and remove or trim the older coverage once parity is clear.

Alternative considered: keep all overlapping tests indefinitely. That would create duplicate maintenance and make it unclear which suite defines the intended contract.

## Risks / Trade-offs

- Duplicate coverage between parsing tests and workflow tests -> Use canonical valid selectors in workflow tests and keep grammar breadth in the dedicated parsing matrix.
- Overly implementation-shaped `CommitOps` tests -> Restrict direct `CommitOps` usage to graph cases where the lower-level boundary is the clearest way to express the contract.
- Slow or flaky remote-sync tests -> Keep the worker count small, reuse moto-backed fixtures already present in `_core`, and limit integration assertions to stable remote-tracking and branch-state outcomes.
- Legacy overlap can linger -> Explicitly map new suites to overlapping old coverage during implementation and remove superseded tests in the same change once parity is confirmed.

## Migration Plan

1. Add or expand `_core` contract suites for local git-like repository workflows.
2. Add targeted `CommitOps` contract cases for merge/rebase/revert and DAG-tree edge behavior.
3. Add `_core` integration coverage for `push`, `fetch`, and `pull` using the existing remote fixtures.
4. Remove or trim superseded repo-op tests after parity is represented by the new suites.
5. Run the targeted `_core` tests, the fast-path test selection, and the contributor-required verification commands.

## Open Questions

- Should `status` contract coverage also assert the current DAG map once the payload shape matches the OpenSpec wording, or should that remain deferred until production behavior changes to expose it consistently?
- Do we want one remote-sync integration workflow that covers `push` -> `fetch` -> `checkout`, or separate smaller integration tests for `push`, `fetch`, and `pull` with less shared setup?
