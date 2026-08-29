## Context

Current runtime cancellation is a single workflow that both detaches callers and calls adapters. It does not distinguish between planning and driving, and local index mutation paths do not have a durable local tombstone they can consult inside LMDB transactions. The revised design keeps S3 execution-state objects as the durable source of truth, moves graph detachment into `F1`, moves adapter driving into `F2`, and adds a local index lifecycle tombstone so concurrent mutators can quiesce without holding the database open.

## Goals / Non-Goals

**Goals:**
- Make cancellation resumable under thread and process parallelism.
- Keep cancellation orchestration entirely inside `daggerml._core`.
- Separate planning/detachment from adapter driving.
- Give local index mutation paths a durable tombstone they can check inside every mutation transaction.
- Preserve S3 execution-state objects as the only durable cancellation state needed for resumption.

**Non-Goals:**
- Validating every sharp edge of `full` vs `drive` at the public API boundary.
- Interrupting threads in the middle of an LMDB write transaction.
- Recording canceled descendants in `child_execution_ids`.

## Decisions

### Runtime cancellation has two explicit modes
`Dml.runtime.cancel(..., mode="full")` runs `F1` then `F2`, then marks the current execution `canceled`. `Dml.runtime.cancel(..., mode="drive")` runs only `F2`.

### F1 owns caller-edge detachment and `cancel-pending`
`F1(ex0)` acquires the execution lock for `ex0` when one exists, returns immediately if `ex0` still has active callers, writes `ex0.lifecycle = "cancel-pending"`, removes each direct edge `ex1/ex0`, and recurses into `F1(ex1)` only when `ex1` has no remaining callers after that edge removal.

This keeps cancellation propagation keyed to live caller ownership rather than to raw descendant reachability.

### F2 is the same driver in both modes
`F2(ex0)` sets the local index lifecycle to `inactive`, rereads `ex0.spawned_execution_ids`, filters that list to direct children whose lifecycle is `cancel-pending`, and repeatedly attempts cancel dispatch for direct children that have become `cancel-ready`. Once the direct drive set drains or the F2 timeout is reached, the runtime marks `ex0.lifecycle = "cancel-ready"`.

Only `mode="full"` advances the current execution from `cancel-ready` to `canceled`.

### Local indexes use a tombstone lifecycle
Local `Index` objects gain a durable lifecycle with states `active`, `inactive`, and `canceled`.

- `active`: normal mutation allowed.
- `inactive`: cancellation is in progress; mutators must leave LMDB, join `cancel(mode="drive")`, then fail as canceled.
- `canceled`: terminal local tombstone; mutators fail immediately without trying to drive cancellation again.

This local lifecycle is distinct from remote execution-record lifecycle. It exists only to coordinate local mutation safety and late callers.

### Mutation gates check inside every mutation transaction
Every mutating index path checks the local index lifecycle from inside its LMDB transaction. If a workflow spans multiple transactions, it performs that check at the start of each transaction.

When a mutator sees `inactive`, it aborts the LMDB work, calls `_core` cancellation in `mode="drive"` outside LMDB, then raises `_core.CancellationError`. When it sees `canceled`, it aborts and raises `_core.CancellationError` immediately.

This gives all concurrent mutators one cooperative cancellation rendezvous without requiring user-managed thread handling.

### Root execution records stay sharp-edged
`mode="full"` is intended for user-root cancellation. `mode="drive"` is intended for fn-dag/internal cancellation progress. The design does not add new public validation beyond what the implementation already needs to operate safely.

## Risks / Trade-offs

- [Timeout still ends in `canceled` for `full`] -> `canceled` means the runtime considers the execution canceled, not that every backend confirmed teardown.
- [Late callers may hit a local tombstone long after remote work was canceled] -> keep the tombstone durable and allow out-of-band cleanup.
- [Mutators can finish one in-flight transaction before they observe cancellation] -> mutation gates are cooperative at transaction boundaries rather than preemptive mid-write.
