## Why

Adapter-backed execution currently leaves a gap between claiming an active execution id and creating that execution's lifecycle record. During that window, `active/<cache_key>` can point at an execution with no durable `execution_record`, which makes stale-state recovery and cancellation semantics less precise than they should be.

`IndexOps.create(cache_key, execution_id)` also behaves like a bootstrap helper rather than a cancellation-gated mutation step, even though it activates a reserved execution id and creates the local mutable index that will drive runtime mutation.

## What Changes

- Add `pending` as the pre-activation execution-record lifecycle for reserved adapter-backed child executions.
- Require `ExecutionState` to create the child execution record in `pending` before publishing or reusing `active/<cache_key>`.
- Make execution-aware `IndexOps.create(cache_key, execution_id)` the activation step that transitions the reserved execution from `pending` to `running`.
- Treat execution-aware `IndexOps.create` as a cancellation-gated mutation operation: `cancel-pending` drives `cancel(mode="drive")` before raising, and all other non-`pending` lifecycles reject activation without local mutation.

## Capabilities

### Modified Capabilities
- `execution-state`: reservation ordering and active-pointer behavior for pre-activation executions.
- `runtime-execution-records`: `pending` lifecycle and reservation-to-activation transition rules.
- `mutable-index-commit-model`: cancellation-gated activation semantics for execution-aware `IndexOps.create`.

## Impact

- Affected code: `src/daggerml/_core/exec_state.py`, `src/daggerml/_core/index.py`, and worker bootstrap paths that call `runtime.create(cache_key=..., execution_id=...)`.
- Affected systems: adapter launch/resume coordination, stale active-pointer recovery, and cancellation behavior during execution activation.
- Caller impact: reserved child executions become visible as `pending` before worker bootstrap, and execution-aware activation follows the same cancellation-drive pattern as other mutation operations.
