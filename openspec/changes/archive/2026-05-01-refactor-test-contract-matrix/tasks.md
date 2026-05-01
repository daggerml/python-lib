## 1. Taxonomy and governance baseline

- [x] 1.1 Add and publish repository test taxonomy guidance covering directories, naming, canonical IDs, lifecycle parameterization, and slow-marker policy.
- [x] 1.2 Add OpenSpec requirement artifacts for the new test-contract-matrix capability.
- [x] 1.3 Create a migration ledger artifact that maps contract IDs, old/new file locations, parity evidence, and legacy removal state.

## 2. Contract test structure and naming migration

- [x] 2.1 Create `tests/contracts/` and `tests/integration/` structure and migrate execution/runtime contract suites into the new locations.
- [x] 2.2 Update migrated contract tests to include canonical contract IDs directly in function names and parameterized case IDs.
- [x] 2.3 Refactor lifecycle-heavy suites into parameterized stage-matrix tests that cover kickoff, resume/poll, and terminal outcomes.
- [x] 2.4 Execute Batch 1 migration for `test_executor_base`, `test_ssh_executor`, and `test_default_runtime` using the initial ledger mappings (`EXB-HDL-*`, `SSH-RES-*`, `SSH-HDL-*`, `DRT-STS-*`).

## 3. Integration classification and marker enforcement

- [x] 3.1 Mark all migrated integration suites with `@pytest.mark.slow`, including process/remote/polling-heavy coverage.
- [x] 3.2 Ensure quick-run workflow (`pytest -m "not slow"`) remains valid for fast-path contract checks.
- [x] 3.3 Align contributor and CI guidance with marker-based selection policy while preserving full-suite CI coverage.

## 4. Full-suite migration and legacy removal

- [x] 4.1 Execute Batch 2 migration for lifecycle-heavy local runtime suites (`test_local_runtime`, `test_funkify`) with stage-matrix coverage.
- [x] 4.2 Execute Batch 3 migration for execution-state and internal integration-heavy suites (`test_exec_state`, `_internal/test_integration_roundtrip`).
- [x] 4.3 Execute Batch 4 migration for infrastructure-heavy integration suites (including `test_ssh_integration`) and remaining integration coverage.
- [x] 4.4 Maintain batch-by-batch parity evidence (targeted suites, `pytest -m "not slow"`, full `pytest`) in the migration ledger before removing each legacy file.
- [x] 4.5 Remove superseded legacy tests after parity validation to avoid duplicate maintenance.
- [x] 4.6 Run full test suite verification and resolve migration regressions before closing the change.
