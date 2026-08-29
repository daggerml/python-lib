## Why

Runtime mutation eligibility is currently split between S3-backed execution lifecycle state and a local `Index.lifecycle` tombstone, with lifecycle checks embedded piecemeal across `IndexOps`. That split makes cancellation behavior harder to reason about, leaves multi-transaction mutation workflows without one canonical guard, and complicates whole-operation retries at the `Dml` orchestration boundary.

## What Changes

- Add a public `ExecutionState.require_mutation(execution_id, db, mode="activation" | "mutation")` guard that reads execution lifecycle state, classifies it for the requested mode, drives `cancel(mode="drive")` for `cancel-pending`, and raises typed lifecycle errors when mutation must stop.
- Convert execution-aware `IndexOps.create(...)` and all mutating `IndexOps` methods to use execution lifecycle in S3 as the sole mutation authority.
- Remove local `Index.lifecycle` mutation gating so runtime mutation semantics are owned by execution records instead of a parallel LMDB tombstone.
- Introduce `BadExecutionStatusError` and `CanceledExecutionError` so callers can distinguish generic wrong-status failures from cancel-family lifecycle failures.
- Add a DML-layer retry wrapper for runtime mutation workflows so retries cover the full read-HEAD-through-db-ops path rather than only individual index operations.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `execution-state`: add the public mutation guard contract and define lifecycle classification behavior for activation vs mutation mode.
- `mutable-index-commit-model`: replace local index lifecycle mutation gating with execution-record-owned mutation gating and remove the local lifecycle tombstone requirement.
- `runtime-execution-records`: define typed wrong-status vs canceled-status failures for execution-aware activation and mutation paths, and require `running` as the only mutable execution lifecycle.

## Impact

- Affected code: `src/daggerml/_core/exec_state.py`, `src/daggerml/_core/index.py`, `src/daggerml/_core/dml.py`, `src/daggerml/_core/types.py`, and runtime contract tests under `tests/_core/`.
- Affected systems: runtime execution coordination, cancellation flow, and mutation retry behavior across DML runtime entrypoints.
- API impact: internal exception taxonomy and lifecycle guard behavior change, while user-facing runtime semantics remain centered on execution lifecycle state.
