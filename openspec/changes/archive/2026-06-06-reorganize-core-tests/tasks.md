## 1. Baseline

- [x] 1.1 Record the current collected `_core` test node IDs and count from the existing `tests/contracts/test_core_*.py` and `tests/integration/test_core_*_integration.py` files.
- [x] 1.2 Identify every import of `tests.contracts.strategies_core` and `tests.helpers.core` that must change after relocation.

## 2. Directory Reorganization

- [x] 2.1 Move existing `_core` contract test files from `tests/contracts/` into `tests/_core/contracts/` without changing assertions, parameters, or test bodies.
- [x] 2.2 Move existing `_core` integration test files from `tests/integration/` into `tests/_core/integration/` without changing assertions, parameters, or test bodies.
- [x] 2.3 Move `tests/contracts/strategies_core.py` to `tests/_core/strategies.py` without changing strategy behavior.
- [x] 2.4 Move `tests/helpers/core.py` to `tests/_core/helpers.py` without changing helper behavior except where fixture centralization makes duplicate local helper code unnecessary.
- [x] 2.5 Update imports in moved files to reference `tests._core.strategies` and `tests._core.helpers`.

## 3. Core Fixtures and Markers

- [x] 3.1 Add `tests/_core/conftest.py` that marks every collected item under `tests/_core/` with `pytest.mark.core`.
- [x] 3.2 Register the `core` marker in `pyproject.toml` with selection guidance.
- [x] 3.3 Add `_core`-local environment cleanup fixtures modeled on `ignore/old-tests-bu/conftest.py` for clearing `AWS_*` and `DML_*` state.
- [x] 3.4 Add `_core`-local moto `ThreadedMotoServer` fixtures that expose endpoint environment variables, an S3 client, and bucket setup for tests requiring a proper S3 server.
- [x] 3.5 Add `_core`-local `fake_dml` fixture that patches `daggerml._core.Dml` and does not patch `daggerml.api.Dml`.

## 4. Naming Cleanup

- [x] 4.1 Rename moved test files to drop redundant `core` prefixes where the `tests/_core/` path already supplies subsystem context.
- [x] 4.2 Shorten moved test function names to describe behavior without repeating the file name or `core` prefix.
- [x] 4.3 Preserve all existing assertions, parametrized cases, Hypothesis settings, generated-input bounds, and fixture usage while renaming.

## 5. Documentation

- [x] 5.1 Update contributor test-layout documentation to describe `tests/_core/{contracts/,integration/}` as the `_core` subsystem-owned taxonomy.
- [x] 5.2 Document `core` marker selection with examples for running only core tests and skipping core tests.
- [x] 5.3 Keep documentation clear that default pytest runs include core tests.

## 6. Verification

- [x] 6.1 Run pytest collection for `tests/_core/` and confirm the collected test count matches the baseline from task 1.1.
- [x] 6.2 Run pytest collection with `-m core` and confirm the moved `_core` tests are selected.
- [x] 6.3 Run pytest collection with `-m "not core" tests/_core` and confirm the moved `_core` tests are excluded.
- [x] 6.4 Run the moved `_core` test suite.
- [x] 6.5 Run formatting/lint checks relevant to moved Python files.
