## Why

`src/daggerml/_internal/dml.py` currently uses a large private helper-method layer on `Dml`, and `_DagNamespace` still exposes a private helper method. That makes the caller-facing `Dml` boundary harder to reason about because orchestration logic is split between public methods and an informal private instance API.

## What Changes

- Remove private helper methods from `daggerml._internal.dml:Dml` and replace them with module-level functions in `dml.py`.
- Keep `Dml` private instance state limited to `_context` and `_tempdirs`.
- Keep namespace private instance state limited to `._dml` and remove any remaining private namespace helper methods.
- Update `Dml` public methods and namespace methods to delegate through module-level helper functions instead of `self._...` helper methods.
- Preserve existing public runtime, DAG, admin, config, and repository behavior; this is an internal boundary cleanup, not a user-facing feature change.

## Capabilities

### New Capabilities

### Modified Capabilities
- `unified-dml-surface`: tighten the internal `Dml` boundary so private state remains limited to `_context` and `_tempdirs`, while helper logic lives at module scope and namespaces only retain `._dml` as private state.

## Impact

- Affected code: `src/daggerml/_internal/dml.py` and tests that assert `Dml`/namespace structure directly.
- Affected APIs: internal `Dml` implementation shape and namespace implementation shape; no intended change to documented caller-facing methods.
- Tests: internal contract tests and any tests that depend on private helper methods or namespace helper structure.
- Systems: improves the clarity of the shared `Dml` orchestration boundary without changing repository semantics.
