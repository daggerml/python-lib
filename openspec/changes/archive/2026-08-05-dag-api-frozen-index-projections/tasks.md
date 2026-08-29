## 1. Specification and docs

- [x] 1.1 Add the frozen-index API projection capability spec.
- [x] 1.2 Document `Dag.freeze()` / `Dag.unfreeze()` and frozen-index read behavior.

## 2. API implementation

- [x] 2.1 Add `Dag.freeze()` and `Dag.unfreeze()` that replace `token` with the runtime-returned ref.
- [x] 2.2 Route index-backed named reads through their described partial DAG.
- [x] 2.3 Do not alter mutation methods or any `_core` file.

## 3. Verification

- [x] 3.1 Add API contract tests for token transitions, frozen reads, and preserved uncommitted-result behavior.
- [x] 3.2 Run targeted tests, full quality gates, and an explicit diff boundary check.
