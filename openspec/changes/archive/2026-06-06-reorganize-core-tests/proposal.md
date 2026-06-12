## Why

The public API for `daggerml._core` is now stable enough that its tests should be easy to find, select, and skip as a coherent subsystem suite. The current `_core` tests are split across top-level `contracts/`, `integration/`, and `helpers/` locations, which obscures ownership and makes marker-based selection unavailable.

## What Changes

- Move all existing `_core` test files into `tests/_core/`, preserving `contracts/` and `integration/` subdirectories under that subsystem root.
- Move `_core`-specific test support into `tests/_core/helpers.py`, `tests/_core/strategies.py`, and `tests/_core/conftest.py`.
- Mark every test collected under `tests/_core/` with `pytest.mark.core` so contributors can run `pytest -m core` or skip with `pytest -m "not core"` while keeping core tests included by default.
- Shorten `_core` test function names so they describe behavior without repeating the file or subsystem name.
- Add `_core`-local fixtures for shared environment isolation, moto-backed S3 server usage, and `fake_dml` patching of `daggerml._core.Dml`.
- Preserve the content, assertions, parametrization, and test count of existing `_core` tests; this change is organizational only.
- Do not add new tests and do not delete existing tests.

## Capabilities

### New Capabilities

### Modified Capabilities
- `test-contract-matrix`: Updates the maintained test taxonomy to allow a subsystem-owned `tests/_core/{contracts/,integration/}` layout, defines `core` marker selection behavior, and captures `_core` fixture placement constraints.

## Impact

- Affected files: `_core` test files currently under `tests/contracts/` and `tests/integration/`, `_core`-specific support currently under `tests/contracts/strategies_core.py` and `tests/helpers/core.py`, pytest configuration in `pyproject.toml`, and contributor/test documentation.
- Affected behavior: pytest collection node IDs and marker selection change; test assertions and coverage do not.
- Affected dependencies: no new runtime or test dependency is introduced; existing `moto` and `boto3` usage is centralized for `_core` tests.
