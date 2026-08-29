## Context

The maintained `_core` tests already exist, but their ownership is encoded by filename prefixes and imports rather than by test-suite layout. Contract-oriented files live under `tests/contracts/`, integration-oriented files live under `tests/integration/`, and shared `_core` test support is split between `tests/contracts/strategies_core.py` and `tests/helpers/core.py`. An empty `tests/_core/` directory already exists, indicating the intended subsystem root.

The desired end state keeps the contract/integration distinction while making `_core` the owning subtree:

```text
tests/_core/
  conftest.py
  helpers.py
  strategies.py
  contracts/
    test_*.py
  integration/
    test_*.py
```

This change is intentionally organizational. Existing test content, assertions, parametrization, and coverage are not to change.

## Goals / Non-Goals

**Goals:**
- Make `tests/_core/` the owning root for maintained `daggerml._core` tests.
- Preserve `contracts/` and `integration/` as subdirectories under the `_core` root.
- Add a registered `core` pytest marker and apply it to every test collected from `tests/_core/`.
- Keep `_core` tests included by default while supporting `pytest -m core` and `pytest -m "not core"`.
- Move `_core`-specific helpers and Hypothesis strategies into the `_core` subtree.
- Add `_core`-local shared fixtures for environment isolation, moto-backed S3 server access, and fake DML patching.
- Shorten test function names to remove redundant `core` and file-name repetition.

**Non-Goals:**
- Add new tests.
- Delete existing tests.
- Change existing test assertions, parametrized cases, generated-input bounds, or fixture behavior beyond relocation and centralization.
- Introduce a top-level `tests/conftest.py` before non-core tests exist and reveal shared needs.
- Patch `daggerml.api.Dml` from the `_core` `fake_dml` fixture.

## Decisions

### 1. Preserve taxonomy inside the subsystem root

Core tests will move to `tests/_core/contracts/` and `tests/_core/integration/` instead of flattening all files directly under `tests/_core/`.

Why:
- Keeps the existing contract/integration distinction visible.
- Makes subsystem ownership explicit without losing behavioral taxonomy.
- Leaves room for future subsystem-owned suites to use the same pattern if useful.

Alternative considered:
- Flatten everything under `tests/_core/`. Rejected because it would hide which tests are fast contract checks versus integration-level scenarios.

### 2. Apply `core` from `tests/_core/conftest.py`

`tests/_core/conftest.py` will mark every collected item under the subtree with `pytest.mark.core`, and `pyproject.toml` will register the marker.

Why:
- Avoids repeating `pytestmark = pytest.mark.core` in every test file.
- Keeps marker ownership local to the `_core` subtree.
- Supports marker selection while keeping default test runs unchanged.

Alternative considered:
- Add `pytestmark` in each moved test module. Rejected because it scatters a suite-level rule across many files.

### 3. Keep `_core` fixtures local for now

Common `_core` fixtures will live in `tests/_core/conftest.py`; no top-level `tests/conftest.py` will be introduced in this change.

Why:
- Current fixture needs are known only for `_core` tests.
- A top-level fixture file would imply cross-suite behavior before other maintained suites exist.
- This keeps future non-core tests free to establish their own needs first.

Alternative considered:
- Introduce `tests/conftest.py` now for AWS/moto cleanup. Rejected because it would prematurely affect all future tests.

### 4. Centralize moto server fixtures around a real server endpoint

The `_core` fixture layer will draw from `ignore/old-tests-bu/conftest.py` but should expose a moto `ThreadedMotoServer` endpoint rather than rely only on `mock_aws()` decorators. Fixtures should provide environment variables and boto3 clients pointed at the server so code paths that need a proper S3-compatible endpoint can reuse them.

Why:
- Upcoming remote-dependent tests need a real HTTP endpoint, not only in-process mocking.
- Existing code already honors `AWS_ENDPOINT_URL` for server-backed AWS clients.
- Centralizing the endpoint reduces repeated moto setup.

Alternative considered:
- Keep per-test `mock_aws()` usage only. Rejected because it does not exercise endpoint-based code paths needed by the rest of the repository.

### 5. Patch `daggerml._core.Dml` in `fake_dml`

The `_core` `fake_dml` fixture will patch the canonical `_core` export `daggerml._core.Dml`, not `daggerml.api.Dml`.

Why:
- The fixture belongs to `_core` tests and should target the `_core` surface.
- Patching `daggerml.api.Dml` would couple `_core` fixtures to the public API wrapper.

Trade-off:
- Modules that imported `Dml` before the fixture patch keep their local binding. This is acceptable and aligned with the intent; the fixture patches the canonical `_core` export for tests that depend on that export.

### 6. Rename tests without changing their bodies

Moved test functions will drop redundant prefixes such as `test_core_head_refs__...` when the file path already identifies the subsystem and surface. File names may also drop redundant `core_` prefixes.

Why:
- `--import-mode=importlib` is already configured, so shorter duplicate test function names in different modules are safe.
- Node IDs remain clear because pytest includes the path and module name.

Alternative considered:
- Preserve all function names exactly. Rejected because the migration is the right time to remove redundant naming noise while preserving behavior.

## Risks / Trade-offs

- Collection node IDs will change -> verify with collect-only before and after migration and ensure the number of collected `_core` tests is unchanged.
- Marker selection could silently miss tests if the conftest hook is wrong -> verify `pytest --collect-only -m core tests/_core` and `pytest --collect-only -m "not core" tests/_core` behavior.
- Import rewrites could accidentally alter support behavior -> keep helper and strategy content unchanged except for module paths, and run the moved `_core` suite.
- Moto server fixtures can add startup cost -> keep server fixture session-scoped and only require bucket/client setup in tests that request it.

## Migration Plan

1. Move existing `_core` test files from `tests/contracts/` and `tests/integration/` into `tests/_core/contracts/` and `tests/_core/integration/`.
2. Move `tests/contracts/strategies_core.py` to `tests/_core/strategies.py` and `tests/helpers/core.py` to `tests/_core/helpers.py`.
3. Update imports to the new support module paths.
4. Add `tests/_core/conftest.py` with the `core` marker hook, environment cleanup fixtures, moto server fixtures, and `fake_dml` patching of `daggerml._core.Dml`.
5. Register the `core` marker in pytest configuration.
6. Shorten test function names without changing test bodies, assertions, parameters, or generated-input settings.
7. Update contributor/test documentation to describe `tests/_core/{contracts/,integration/}` and marker selection.
8. Verify collection count and run the moved `_core` suite.

## Open Questions

- None. The `fake_dml` patch target, moto server requirement, subtree-local fixture scope, and no-content-change constraints are decided.
