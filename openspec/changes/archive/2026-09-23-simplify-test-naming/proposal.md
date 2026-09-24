## Why

The test suite's function names repeat context already supplied by pytest module paths and embed opaque contract identifiers. This makes collected test output harder to read and obscures related test setup. A behavior-preserving migration is needed before adding broader lifecycle coverage.

## What Changes

- Replace test names with concise, direct descriptions of the behavior under test.
- Remove repeated subsystem, test-kind, and numeric contract prefixes from test function names.
- Group related tests into classes when a shared subject or fixture improves local readability.
- Preserve every test case, parametrized case, assertion, marker, and coverage behavior during the migration.
- Update contributor guidance to define the simplified naming and grouping conventions.

## Capabilities

### New Capabilities

None. This is a behavior-preserving test-suite and contributor-documentation refactor.

### Modified Capabilities

None.

## Impact

- Test modules and pytest node IDs under `tests/`.
- `CONTRIBUTING.md` test taxonomy and naming guidance.
- CI and local test selection remain marker- and path-compatible; external tooling that hard-codes individual pytest node IDs must update those selectors.
