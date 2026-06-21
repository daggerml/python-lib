## Context

The runtime already distinguishes cache identity from execution identity and uses `active/<cache_key>` plus `exec/state/<execution_id>.json` to coordinate adapter-backed work. The current launch path, however, can create an active pointer before the execution record exists, and the worker-side `IndexOps.create(cache_key, execution_id)` can activate that execution without first treating activation as a cancellation-gated mutation.

The revised design introduces an explicit reservation phase:

1. the caller reserves a child execution id,
2. the runtime persists that execution as `pending`,
3. the runtime publishes or reuses the active pointer for that cache key,
4. the worker bootstraps and activates the reserved execution by transitioning `pending -> running`.

## Goals / Non-Goals

**Goals:**
- Make every active execution id correspond to a durable execution record.
- Give `pending` a precise meaning: reserved and coordinated, but not yet activated by a worker runtime.
- Make execution-aware `IndexOps.create` obey the same cancellation-drive pattern as other mutation operations.
- Keep root runtime index creation behavior unchanged unless execution-aware arguments are provided.

**Non-Goals:**
- Changing builtin execution behavior.
- Making execution-aware activation idempotent for `running` records.
- Redesigning terminal execution lifecycles beyond adding `pending`.

## Decisions

### Child execution reservation happens before active publication
When `start_fn` observes a cache miss and decides to launch or resume adapter-backed work, `ExecutionState` creates the child execution record in `pending` before that execution id is treated as the active execution for the cache key.

This removes the current ambiguity where an active pointer can temporarily reference an execution id with no execution record.

### `pending` is the only normal pre-activation lifecycle
`pending` means the child execution id has been reserved and may already have caller edges and spawned-child lineage, but no worker runtime has yet activated the local mutable index for that execution.

`running` remains the post-activation lifecycle for execution ids whose worker runtime has entered `IndexOps.create(cache_key, execution_id)` successfully.

### Execution-aware `IndexOps.create` is a mutation gate
`IndexOps.create(cache_key, execution_id)` is no longer just a bootstrap helper. It is the activation mutation for a reserved execution id.

Before creating or mutating local index state, execution-aware `create` reads the existing execution record:

- `pending`: proceed and transition the record to `running`
- `cancel-pending`: stop local activation, call `cancel(mode="drive")`, then raise `CancellationError`
- `cancel-ready` or `canceled`: raise `CancellationError` without local mutation
- `running`, `succeeded`, `failed`, or missing record: raise `DmlRepoError` without local mutation

This preserves a sharp activation edge: `create` is the only path that converts a reserved child execution from `pending` to `running`.

### Root index creation stays direct-to-running
Top-level or otherwise non-execution-aware `IndexOps.create()` continues to create its own execution/root record directly as `running`. The new `pending` lifecycle applies only to reserved adapter-backed child executions created through the execution-aware call path.

## Risks / Trade-offs

- [A worker may arrive after cancellation has already started] -> execution-aware `create` treats `cancel-pending` as a cooperative cancellation rendezvous and drives `cancel(mode="drive")` before failing.
- [A second process may try to activate an already-running child execution] -> the design rejects `running` as an invalid activation attempt instead of treating activation as reopen/idempotent.
- [The lifecycle model grows by one state] -> `pending` removes an existing coordination gap and makes stale active-pointer handling more deterministic.
