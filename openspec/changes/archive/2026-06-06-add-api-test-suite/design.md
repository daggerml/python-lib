## Context

`src/daggerml/api.py` exposes the main Python user surface: default `Dml` helpers, `new()`, `load()`, `temporary()`, `Dag`, `Node` subclasses, collection helpers, and literal codec normalization. The current maintained tests are concentrated under `tests/_core/`, so public API regressions can slip through unless they also happen to break lower-level repository behavior.

The public API layer is intentionally thin. Most methods either resolve a `Dml` instance, normalize Python values, delegate to `dml.runtime` / `dml.dag`, or wrap returned refs as Python-friendly `Node` objects. Tests should respect that boundary rather than re-testing storage, commit graph, remote, or execution internals.

## Goals / Non-Goals

**Goals:**

- Add fast, isolated contract tests for public API behavior that is meaningful, fragile, or user-visible.
- Mock or fake `Dml` for most tests so failures identify regressions in `api.py` wrapper logic.
- Add a small live integration suite that proves the public API works against an initialized repository.
- Cover the literal codec registry now housed in `api.py`, including plugin loading, recursive normalization, and `NodeCodec` import behavior.
- Align new tests with the repository test taxonomy in `CONTRIBUTING.md` and `test-contract-matrix`.

**Non-Goals:**

- Do not change `daggerml.api` behavior as part of this test-suite change.
- Do not duplicate `_core` tests for LMDB storage, commit merging, remote sync, execution coordination, or revision parsing.
- Do not introduce broad slow adapter or external-process tests for API coverage.
- Do not maintain parallel legacy public API test files if equivalent contract-matrix tests are introduced later.

## Decisions

### Use Four Public API Contract Suites

Create focused contract files under `tests/contracts/`:

- `test_api_defaults.py`: default `Dml` resolution, scoped/process defaults, top-level helpers, and `temporary()` construction.
- `test_api_dag_contracts.py`: `Dag` wrapper behavior, named-node access, commit/require/call delegation, context-manager error capture, and `_make_node()` classification.
- `test_api_node_contracts.py`: `Node`, `RunnableNode`, `ListNode`, `DictNode`, and collection helper behavior.
- `test_api_codecs.py`: codec plugin loading, codec ordering, error wrapping, recursive normalization, `MiscPyTypeCodec`, and `NodeCodec`.

Alternative considered: one large `test_api.py`. The focused files are preferable because `api.py` mixes several public concerns, and split files make failures easier to localize.

### Prefer Realistic Fakes Over Deep Core Setup For Contracts

Contract tests should use `MagicMock` objects or small fakes for `dml.runtime`, `dml.dag`, and `dml.show()`. These fakes should return realistic `Ref` objects and plain Python values at the same boundaries that real `_core.Dml` namespaces expose.

This is especially important for `_make_node()`, which classifies refs by calling `dag.dml.dag.get_node(ref)` and inspecting the returned Python value.

Alternative considered: instantiate a live `Dml` for all tests. That would provide more realism but would make most public API failures harder to distinguish from `_core` failures and would slow the fast-path suite.

### Keep Live Integration Small And Public-Surface Oriented

Add `tests/integration/test_api_live_runtime_integration.py` with a few high-signal workflows:

- `new()` / `put()` / `commit()` / `load()` with scalar, list, and dict values.
- distinction between `dag["result"]` named lookup and `dag.result` committed result.
- cross-DAG `require()` from a committed source DAG into a new DAG.
- collection helpers backed by real builtins: list indexing/slicing, append/conj, dict get/default, assoc, and contains.
- scoped default usage with `use_default_dml(dml)` and top-level `new()` / `load()`.
- context-manager error capture committing an `Error` result.

Integration tests should initialize a real repo under `tmp_path`. If the runtime path needs local execution isolation, follow the existing `_core` integration pattern that patches `_core.dml._index_ops` to `local_index_ops()`.

Alternative considered: no live integration. Mock-only tests would miss contract gaps between the public wrapper and the actual `_core.Dml` namespace objects.

### Treat Codec Tests As Public API Tests

The literal codec system lives in `api.py`, including entry-point loading, codec ordering, recursive normalization, and `NodeCodec`. It should be covered as part of the API suite rather than treated as `_core` behavior.

Codec tests should isolate global registry state (`_codecs`, `_plugins_loaded`) so they do not leak ordering or plugin-load effects between tests.

Alternative considered: only test codec behavior through `Dag.put()`. That would leave plugin error handling, ordering, and recursive edge cases insufficiently specified.

### Marker Reconciliation Is Part Of Implementation Hygiene

`CONTRIBUTING.md` documents `needs_dml`, but `pyproject.toml` currently registers `core`, `slow`, and `serial`. If API integration tests use `needs_dml`, the marker must be registered. If they only need a live in-process `Dml` instance and no external `daggerml-cli`, then `slow` may be enough.

Alternative considered: add `needs_dml` unconditionally. That could be misleading if these tests do not require the CLI or external binary installation.

## Risks / Trade-offs

- Mock drift from real `_core.Dml` namespace behavior -> use realistic `Ref` values and match observed `runtime` / `dag` method signatures from `_core.dml`.
- Global default `Dml` and codec registry state leaks between tests -> add fixtures that reset `_PROCESS_DEFAULT_DML`, `_SCOPED_DEFAULT_DML` usage, `_codecs`, and `_plugins_loaded` around relevant tests.
- Integration tests becoming broad `_core` retests -> keep assertions at the public API level and avoid inspecting LMDB internals.
- Plugin-entry-point tests accidentally loading installed third-party plugins -> monkeypatch `api._entry_points()` and registry state in codec tests.
- Context-manager error tests may depend on exact `Error` serialization -> assert public `Error` fields and value behavior, not internal object refs.

## Migration Plan

1. Add the new contract test files under `tests/contracts/`.
2. Add the small live integration file under `tests/integration/` with appropriate markers.
3. Reconcile pytest marker registration only if the chosen marker is not registered.
4. Run targeted API contract tests, targeted API integration tests, and the fast-path suite.

No rollback or data migration is required because this change adds tests and optional marker metadata only.

## Open Questions

- Should live public API integration tests use only `@pytest.mark.slow`, or should they also use `@pytest.mark.needs_dml` after registering that marker?
- Should `temporary()` be covered only with mocks in contracts, or also with one live smoke test once the broader live-runtime setup is in place?
