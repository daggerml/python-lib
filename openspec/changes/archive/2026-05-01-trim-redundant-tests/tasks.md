## 1. Build centralized parsing contract matrix

- [x] 1.1 Create a new contract test suite for revision/ref/URI parsing with parameterized case IDs that include canonical contract IDs.
- [x] 1.2 Port parsing assertions from mixed workflow tests into the new matrix (ref parsing, URI canonicalization, revision-form classification, and local-resolution rejection boundaries).

## 2. Prune redundant parser-smoke tests

- [x] 2.1 Remove parser-creation smoke tests that are fully duplicated by subcommand argument parsing tests in the same file.
- [x] 2.2 Remove duplicate revision parsing checks from workflow-oriented contract tests once parity is covered by the matrix.

## 3. Reclassify external-process tests and collapse expensive adapter duplicates

- [x] 3.1 Inventory fast-path tests that invoke subprocess adapters, polling loops, or equivalent runtime orchestration and mark qualifying tests as `slow`.
- [x] 3.2 Consolidate duplicate expensive adapter-path tests into parameterized matrices while preserving canonical contract IDs and stage labels.
- [x] 3.3 Verify parity for collapsed adapter-path coverage against the removed/reclassified tests.

## 4. Preserve and verify invariant-focused coverage

- [x] 4.1 Keep workflow/delegation/state-transition tests intact and confirm they no longer assert parser grammar variants.
- [x] 4.2 Run targeted contract test files touched by this change and fix any coverage gaps introduced by consolidation.
- [x] 4.3 Run `pytest -m "not slow"` and confirm the fast contract path remains green after redundancy removal.
- [x] 4.4 Compare non-slow runtime before/after and record reduction against target. (Internal contract non-slow runtime: 59.89s -> 46.93s, ~21.6% faster, 7 adapter-path tests moved to slow)
