# Test Taxonomy and Naming

## Status

specified

## Authority

This document is authoritative for test taxonomy, naming, and marker usage in this repository.
If guidance elsewhere conflicts on these topics, this document is the source of truth.


## Purpose

Define a contract-driven test layout so each test targets one documented invariant, lifecycle tests are parameterized, and integration tests are consistently marked `slow`.


## Scope

In scope:

- test directory taxonomy,
- test file/function naming conventions,
- canonical contract ID usage in test names and parameterized IDs,
- `slow` marker policy,
- migration policy for removing superseded legacy tests.

Out of scope:

- product runtime behavior contracts (owned by feature specs),
- exact test implementation details per module.


## Conventions

### Directory layout

- `tests/contracts/`: fast, isolated tests that verify one documented requirement or invariant.
- `tests/integration/`: multi-component or infrastructure-dependent tests.
- Existing folders (`tests/_internal/`, `tests/contrib/`, etc.) may remain during migration, but new/refactored suites SHALL target `tests/contracts/` or `tests/integration/`.

### File naming

- Contract tests SHOULD use `test_<surface>_<contract>.py`.
- Integration tests SHOULD use `test_<surface>_<scenario>_integration.py`.
- New files SHOULD avoid generic names such as `test_core.py` when a specific contract surface is known.

### Function naming

- Test functions SHOULD use `test_<contract_id_slug>__<behavior>()` where practical.
- Example: `test_exec_lc_003__resume_uses_launch_state()`.

### Canonical contract IDs

- Contract IDs SHALL be specified directly as literal strings (no indirection required).
- Contract IDs SHOULD use uppercase category prefixes and numeric suffixes, for example:
  - `ADP-OUT-001`
  - `EXEC-LC-003`
  - `EST-LOCK-004`
- Parameterized cases SHOULD include canonical IDs in `id=`, for example:
  - `id="EXEC-LC-003:resume-uses-launch-state"`

### Lifecycle parameterization

- Tests that exercise a lifecycle SHALL prefer one parameterized test per contract family over multiple near-duplicate tests.
- Lifecycle stages SHOULD be explicit in case IDs (for example `kickoff`, `resume`, `terminal-succeeded`, `terminal-failed`).

### Slow marker policy

- Any integration test requiring external processes, polling loops, remote roundtrips, or significant runtime orchestration SHALL be marked `@pytest.mark.slow`.
- Contract tests in `tests/contracts/` SHOULD remain unmarked and fast by default.
- Fast local loop remains `pytest -m "not slow"`.


## Migration policy

- Migration is full replacement, not indefinite dual maintenance.
- When a legacy test is superseded by a new contract-structured test, the legacy test SHALL be removed in the same change set or immediately after parity is confirmed.
- During migration, each moved contract SHOULD preserve traceability by carrying canonical contract IDs into new parameterized case IDs.
- The end state SHALL have all maintained tests aligned to this taxonomy.


## References

- `CONTRIBUTING.md`
- `pyproject.toml`
- `docs/adapter-execution-contract.md`
- `docs/execution-model.md`
- `docs/contrib/runtime-contract.md`
- `docs/contrib/executor-state.md`
