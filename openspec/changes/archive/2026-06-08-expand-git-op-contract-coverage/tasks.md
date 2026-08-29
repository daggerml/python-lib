## 1. Local Repository Porcelain Contracts

- [x] 1.1 Add `_core` contract coverage for `Dml.status()` attached/detached reporting, branch listing, and live index counts.
- [x] 1.2 Add `_core` contract coverage for `Dml.rev_parse()` and local revision resolution using representative valid selectors without duplicating parsing-matrix breadth.
- [x] 1.3 Add `_core` contract coverage for `Dml.log()`, `Dml.show()`, and `Dml.diff()` covering parent-relative and explicit-base history inspection behavior.
- [x] 1.4 Add `_core` contract coverage for `Dml.checkout()` covering attached local-branch checkouts and detached commit/tag/remote-tracking checkouts.

## 2. History Mutation And DAG Tree Contracts

- [x] 2.1 Add `_core` contract coverage for `Dml.merge()`, `Dml.rebase()`, and `Dml.revert()` for attached-branch success and detached-head rejection paths.
- [x] 2.2 Add targeted `CommitOps` contract cases for merge conflict detection, revert conflict detection, linear-history rebase replay, and first-parent ancestry edge cases.
- [x] 2.3 Add `_core` contract coverage for `dag.checkout()` and `dag.delete()`, including overwrite, missing-name, and detached-head constraints.
- [x] 2.4 Expand `test_head_refs.py` or adjacent repo-op contract suites for local/remote branch-tag pointer round-trips and duplicate-ref rejection where that behavior remains part of the maintained pointer contract.

## 3. Remote Sync Integration Coverage

- [x] 3.1 Add `_core` integration coverage for `push()` using attached-branch defaults and explicit revision constraints.
- [x] 3.2 Add `_core` integration coverage for `fetch()` and local remote-tracking ref updates.
- [x] 3.3 Add `_core` integration coverage for `pull()` covering attached-branch success and detached-head rejection.
- [x] 3.4 Mark the new remote-sync integration tests `slow` and keep their setup bounded and deterministic.

## 4. Parity And Verification

- [x] 4.1 Map the new repo-op contract and integration suites against any overlapping existing tests and remove or trim superseded maintained coverage after parity is confirmed.
- [x] 4.2 Run targeted `_core` contract and integration tests for the new repo-op suites and fix any gaps in test setup or assertions.
- [x] 4.3 Run the fast-path local selection and the contributor-required verification commands, or document any intentional exclusions if full verification cannot run in the apply phase.
