## Why

The current test suite mixes concerns, duplicates lifecycle assertions across many files, and does not consistently distinguish fast contract checks from slower integration behavior. We need a contract-driven structure so each test maps to one documented invariant, lifecycle coverage is concise and systematic, and fast local feedback is reliable.

## What Changes

- Reorganize tests into a contract-first taxonomy with dedicated `tests/contracts/` and `tests/integration/` areas.
- Require canonical contract IDs in test names and parameterized case IDs using direct literal strings (no registry indirection).
- Consolidate lifecycle assertions into parameterized tests that cover each lifecycle stage as explicit cases.
- Require `@pytest.mark.slow` for integration tests and other infrastructure-heavy tests so `pytest -m "not slow"` is a dependable quick path.
- Migrate all maintained tests to the new structure and remove superseded legacy tests once parity is confirmed.

## Capabilities

### New Capabilities
- `test-contract-matrix`: Defines repository test taxonomy, canonical test ID conventions, lifecycle parameterization rules, slow-marker policy, and full migration/removal expectations for legacy tests.

### Modified Capabilities
- None.

## Impact

- Affected code: test suite layout and naming across `tests/**`, shared test fixtures where needed to separate fast and integration concerns, and contributor-facing test documentation.
- APIs: no runtime or public API behavior change.
- Dependencies/systems: no new runtime dependency; CI and local test invocation patterns rely more explicitly on marker-based selection.
