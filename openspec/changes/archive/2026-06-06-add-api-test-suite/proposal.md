## Why

`daggerml.api` is the primary Python-facing surface, but the maintained tests currently focus on `_core` behavior and leave the public wrapper layer under-specified. Adding a tactical API test suite will protect default-runtime resolution, DAG/Node wrapper delegation, literal codec normalization, and live public workflows without duplicating `_core` storage and execution tests.

## What Changes

- Add fast contract tests under `tests/contracts/` for `daggerml.api` default DML handling, `Dag` wrapper behavior, `Node` wrapper behavior, and literal codec normalization.
- Add a small live-runtime integration suite under `tests/integration/` that exercises public API workflows against an initialized `Dml` repository.
- Keep most public API tests isolated with mocked or fake `Dml` objects so failures identify wrapper regressions rather than core repository issues.
- Use integration tests only for high-signal end-to-end public workflows such as `new` / `put` / `commit` / `load`, cross-DAG `require`, collection helpers, scoped defaults, and context-manager error capture.
- Reconcile marker documentation/configuration if the live integration tests use a marker that is documented but not registered.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `test-contract-matrix`: Extend the test taxonomy and meaningful-contract expectations to cover the public `daggerml.api` surface, including the split between mocked contract tests and live-runtime integration tests.

## Impact

- Affected tests: new files under `tests/contracts/` and `tests/integration/`.
- Affected specs: `openspec/specs/test-contract-matrix/spec.md` via a delta for public API test coverage.
- Affected configuration: possible pytest marker registration update if `needs_dml` is used for live-runtime tests.
- No intended behavioral changes to `src/daggerml/api.py` or `_core` runtime behavior.
