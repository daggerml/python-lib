## Why

Cancellation currently combines graph detachment, adapter cancellation, and local index shutdown into one synchronous flow. That makes cancellation hard to resume safely, races with local index mutation, and leaves no durable local tombstone for callers that arrive after cancellation has already completed.

## What Changes

- Split runtime cancellation into two phases: `F1` planning/detachment and `F2` adapter driving.
- Add `mode="full" | "drive"` to `Dml.runtime.cancel`.
- Change execution cancellation lifecycle from `running -> cancel-pending -> canceled` to `running -> cancel-pending -> cancel-ready -> canceled`.
- Make `F1` remove live caller edges recursively and mark orphaned executions `cancel-pending` under per-execution locks.
- Make `F2` read its direct spawned set from execution state, drive only direct children that are still `cancel-pending`, and mark the current execution `cancel-ready` when descendant driving finishes or times out.
- Make `mode="full"` mark the current execution `canceled` after `F2`; `mode="drive"` does not.
- Replace the local index active bit with a local index lifecycle tombstone: `active -> inactive -> canceled`.
- Require every mutating index transaction to check local index lifecycle inside the transaction, drop LMDB work before joining cancellation, and surface `_core.CancellationError` after cancellation rendezvous.

## Capabilities

### Modified Capabilities
- `execution-admin-controls`: two-phase cancellation algorithm and `full`/`drive` runtime modes.
- `runtime-execution-records`: `cancel-ready`, root execution-record behavior, and direct-child F2 driving rules.
- `mutable-index-commit-model`: local index lifecycle tombstone and mutation-gate behavior.
- `executor-cancellation`: executor cancel invocation semantics after runtime `cancel-ready` gating.
- `unified-dml-surface`: `runtime.cancel(..., mode=...)` public/runtime surface.

## Impact

- Affected code: `src/daggerml/_core/exec_state.py`, `src/daggerml/_core/index.py`, `src/daggerml/_core/dml.py`, `src/daggerml/_core/types.py`, and runtime cancellation tests.
- Affected systems: execution-state lifecycle, edge-removal planning, local index mutation safety, and executor cancel orchestration.
- Caller impact: runtime cancellation becomes resumable and mode-driven, and canceled indexes become durable local tombstones instead of disappearing immediately.
