## Why

DaggerML's git-like repository workflows carry most of the risk around branch state, revision resolution, DAG tree history, and remote sync, but the maintained `_core` contract suite only covers a small slice of that surface today. We need explicit contract coverage for those workflows before regressions in `Dml` porcelain or `CommitOps` graph behavior become harder to detect and localize.

## What Changes

- Add `_core` contract tests for local git-like repository workflows exposed through `Dml`, including `status`, `rev_parse`, `log`, `show`, `diff`, `checkout`, `merge`, `rebase`, `revert`, `dag.checkout`, and `dag.delete`.
- Add targeted `_core` contract tests that exercise `CommitOps` directly for graph-shape and conflict-heavy behavior that is awkward or ambiguous to set up only through porcelain.
- Add `_core` integration coverage for remote sync workflows such as `push`, `fetch`, and `pull`, and keep that coverage classified separately from fast local contract tests.
- Preserve the current parsing-ownership split so repo workflow tests use canonical valid selector forms rather than re-testing revision grammar already owned by the parsing matrix.
- Remove or trim superseded repo-op tests once the new contract and integration suites provide clear parity for the maintained behavior.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `test-contract-matrix`: Add explicit maintained-test requirements for git-like `_core` repository workflows, including the boundary between `Dml` porcelain contracts, targeted `CommitOps` graph contracts, and remote-sync integration coverage.

## Impact

- Affected tests: `tests/_core/contracts/` and `tests/_core/integration/` gain new repo-op suites covering local history, checkout/mutation, DAG tree operations, and remote sync.
- Affected specifications: `openspec/specs/test-contract-matrix/spec.md` receives a delta for git-like repository workflow coverage expectations.
- Dependencies: no new runtime dependency is expected; existing test fixtures and moto-backed remote helpers should be reused where possible.
- Runtime/API behavior: no intended production behavior change, but the new tests may expose existing defects in history mutation, HEAD handling, or remote-tracking workflows that later apply work will need to fix.
