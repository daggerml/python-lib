## Why

The current contract test suite includes redundant parser smoke tests and duplicated revision/URI parsing assertions spread across CLI and ops files. This increases maintenance cost and makes failures noisy, while weakening the contract-first intent of the test taxonomy.

## What Changes

- Remove parser-creation smoke tests that are already covered by more specific parser argument tests in the same file.
- Consolidate revision/ref/URI parsing checks into a single parameterized contract-focused suite instead of repeating equivalent checks in multiple workflow tests.
- Reclassify external-process runtime-orchestration tests from fast contract selection into `slow` coverage where they match integration-style execution behavior.
- Collapse duplicate expensive adapter execution paths into parameterized stage/case matrices so one maintained suite covers each contract family without repeated near-identical runtime cost.
- Keep workflow tests focused on operational invariants (delegation, head movement, state transitions, and boundary errors) rather than parser grammar duplication.
- Preserve or improve traceability by using canonical contract IDs and parameterized case IDs for the consolidated parsing matrix.

## Capabilities

### New Capabilities
- `revision-parsing-contract-matrix`: Centralized, parameterized contract matrix for ref parsing, DML URI canonicalization, and revision resolution forms/error boundaries.

### Modified Capabilities
- `test-contract-matrix`: Tighten migration/removal guidance for superseded redundant tests in contract suites by explicitly pruning duplicate parser checks once parity is confirmed.

## Impact

- Affected tests in `tests/contracts/internal/cli/**` and `tests/contracts/internal/ops/**`, primarily parser-smoke and revision-parsing overlap points.
- Affected fast-path runtime orchestration tests currently selected by `pytest -m "not slow"`, especially external-process adapter execution paths.
- New/updated OpenSpec test capability artifacts under `openspec/specs/`.
- No user-facing runtime/API behavior changes; scope is test structure and maintainability.
