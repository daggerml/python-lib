## Context

See `proposal.md` for motivation. Execution records currently coordinate cancellation through `cancel-requested`, `cancel-ready`, recursive driving, and a 60-second readiness timeout. Caller references are separate S3 edge objects, while lifecycle and spawned lineage are stored in CAS-updated execution records protected by embedded owner locks. Adapter cancellation is synchronous and documented as idempotent.

The cancellation decision depends on both an execution-record lifecycle and its incoming caller-edge prefix. Those objects cannot be changed in one S3 transaction. Caller registration and cancellation therefore need a shared ordering boundary so Phase 1 cannot observe zero callers and select an execution while a new caller concurrently becomes valid.

## Goals / Non-Goals

**Goals:**

- Represent cancellation with only `cancel-pending` and `canceled`.
- Complete graph selection before any adapter cancellation starts.
- Make terminal completion win harmlessly when it races with cancellation selection.
- Preserve shared executions that retain a valid caller.
- Make both phases restartable after interruption and safe under concurrent drivers.
- Retain embedded execution locks and S3 compare-and-swap as the coordination primitives.

**Non-Goals:**

- Introduce transactions spanning execution records, edge objects, and cache pointers.
- Change the adapter cancel request or response envelope.
- Guarantee discovery below an already-terminal intermediate execution that is skipped by Phase 1.
- Translate persisted `cancel-requested` or `cancel-ready` records at read time.
- Redesign invalidation, execution graph inspection, or historical lineage.

## Decisions

### 1. `cancel-pending` is durable cancellation-set membership

The lifecycle state machine becomes:

```text
pending/running -> cancel-pending -> canceled
       |                |
       +-> succeeded    +-> no index/result mutation
       +-> failed
```

Phase 1 persistence of `cancel-pending` means the execution belongs to the cancellation set and no longer accepts activation, index mutation, result publication, child spawning, or new caller registration. It is not merely a request that another runtime must acknowledge.

An execution already in `cancel-pending` is included when a later full or drive operation reconstructs work. This makes a crash after the Phase 1 CAS recoverable without another lifecycle.

Alternatives considered:

- Keep `cancel-requested` and reinterpret it. Rejected because its name and current distributed semantics imply an uncommitted request rather than completed selection.
- Retain `cancel-ready`. Rejected because the complete set is known before Phase 2, so readiness adds a redundant state and timeout protocol.

### 2. Phase 1 is an iterative, restartable graph-selection pass

Phase 1 maintains a pending work collection, a selected ordered set, and a completed set for terminal or missing records. For each candidate it:

1. Acquires the execution coordination lock and rereads the record.
2. Skips a terminal execution without error.
3. Reconstructs an already-`cancel-pending` execution into the selected set.
4. For an active execution, lists valid incoming caller edges while holding the lock and skips the candidate for now if any remain.
5. CAS-updates an unreferenced active execution to `cancel-pending` with cancellation metadata. A CAS conflict restarts evaluation from a fresh record and fresh caller listing.
6. After selection, conditionally deletes the matching cache pointer, idempotently removes every outgoing caller edge represented by `spawned_execution_ids`, and enqueues those spawned executions.

No adapter is invoked until pending graph work is exhausted. Cache deletion and edge deletion happen after durable selection because `cancel-pending` prevents new use if cleanup is interrupted. Recovery repeats those idempotent side effects for records already in `cancel-pending`.

A candidate skipped for caller references is not permanently added to the completed set. It may be enqueued again when another selected caller removes an edge. This is required for shared descendants:

```text
       root
      /    \
     A      B
      \    /
        C
```

If `C` is first observed while `B -> C` remains, it is deferred. Selecting `B` removes that edge and enqueues `C` again, allowing `C` to join the cancellation set once its last valid reference is gone.

Alternatives considered:

- Snapshot the entire graph before mutation. Rejected because the snapshot becomes stale immediately and does not simplify caller races.
- Permanently deduplicate every visited execution. Rejected because deferred shared descendants must be reconsidered after reference removal.

### 3. The execution lock orders caller registration against selection

Caller registration and cancellation selection use the callee execution lock as their shared ordering boundary.

Registration acquires or retains the callee lock, verifies that the lifecycle permits invocation, publishes the edge, completes caller forward-lineage registration, and only then invokes the adapter. If registration cannot complete, it removes its edge before releasing coordination.

Phase 1 acquires the same callee lock before listing caller edges and holds it through the `cancel-pending` CAS. Therefore:

```text
registration wins -> valid edge is visible -> Phase 1 preserves callee
cancellation wins -> lifecycle is cancel-pending -> registration aborts
```

The record CAS remains necessary even under the embedded lock because lock expiry and replacement can transfer ownership. A stale owner cannot commit after another owner changes the ETag.

Alternatives considered:

- Direct lifecycle CAS without the embedded lock. Rejected because an edge-prefix listing and record write are not atomic, allowing a new caller to appear between them.
- Store a numeric reference count in the execution record. Rejected because it duplicates canonical edge state and introduces increment/decrement recovery problems.

### 4. Phase 2 processes only the completed Phase 1 selection

Phase 1 returns an ordered selected set. Phase 2 processes that set in reverse selection order, which naturally visits descendants before callers because a callee cannot be selected until selected callers have removed their edges. Correctness does not depend on a persisted readiness state.

For each selected execution, Phase 2 acquires the execution lock, snapshots the record, and releases the lock before external adapter work. Terminal records are accepted as complete. A `cancel-pending` adapter-backed record is materialized from its persisted `argv_ref`, passed to its cancel adapter without holding the execution lock, and then locked again for the CAS update directly to `canceled`. A cacheless or otherwise non-adapter-backed selected execution skips adapter dispatch and is also CAS-updated to `canceled`. Releasing the lock around adapter invocation prevents a supervisor or nested runtime that observes `cancel-pending` from deadlocking on the outer adapter call.

A well-formed adapter return remains advisory and permits the `canceled` transition, matching current behavior. An exception that prevents a completed adapter call leaves the lifecycle `cancel-pending` so another driver can retry. If the adapter completed but lifecycle persistence was interrupted, retry may invoke it again; executor cancellation is already required to be idempotent.

Alternatives considered:

- Begin adapter cancellation while Phase 1 is still traversing. Rejected because this loses the clean boundary that defines the complete selected set and can destroy infrastructure needed to discover descendants.
- Persist a separate cancellation-plan object. Rejected because `cancel-pending` records and their spawned lineage already provide durable reconstruction.

### 5. Full and drive modes share the same two-phase engine

`full` mode requires a requester, starts Phase 1 from the supplied root, and then runs Phase 2 over the selected set. `drive` mode starts from persisted `cancel-pending` state, replays Phase 1 reconstruction and cleanup, and runs the same Phase 2 implementation.

Concurrent full or drive callers may reconstruct overlapping selected sets. Locks and lifecycle CAS make terminal persistence safe, while adapter idempotency makes repeated external cleanup safe. A direct cancellation of an execution that is already terminal returns successfully with no selected work instead of raising `BadExecutionStatusError`.

The existing cancellation summary shape is retained. The `timeout` collection remains present but empty so removing readiness timeout does not create an unrelated public response-shape break.

Alternatives considered:

- Remove drive mode. Rejected because operators need it to resume persisted cancellation after an interrupted full operation. Supervisors may stop their local worker when they observe `cancel-pending`, but they leave lifecycle completion to the external Phase 2 driver rather than recursively driving their own adapter.
- Preserve terminal-root errors. Rejected because normal completion satisfying the desired outcome should win a cancellation race without failing the overall operation.

### 6. Old intermediate lifecycle values are removed without read-time compatibility

Execution validation, lifecycle typing, rendering, guards, tests, and documentation use `cancel-pending` and no longer accept `cancel-requested` or `cancel-ready`. This keeps the new state machine explicit and avoids compatibility branches in every lifecycle consumer.

## Risks / Trade-offs

- [An incomplete registration edge could incorrectly preserve a callee] -> Registration removes its edge on any failure before completed caller lineage publication, and cancellation shares the callee lock with registration.
- [A deferred shared descendant could be missed] -> Referenced candidates are not permanently completed; every selected caller edge removal re-enqueues its spawned descendants.
- [A crash after selection leaves graph side effects incomplete] -> `cancel-pending` reconstruction replays cache deletion, outgoing-edge deletion, and descendant enqueueing idempotently.
- [Two drivers invoke the same adapter] -> Adapter cancellation remains idempotent, and lifecycle CAS converges on `canceled`.
- [Removing old lifecycle values makes existing in-progress records unreadable] -> Deployment requires no persisted `cancel-requested` or `cancel-ready` records; no compatibility translation is added.
- [Terminal intermediates can hide active descendants] -> The existing best-effort traversal limitation remains explicit and is outside this simplification.

## Migration Plan

1. Before deployment, verify that the execution store contains no `cancel-requested` or `cancel-ready` records requiring recovery.
2. Deploy lifecycle consumers, planning, driving, caller registration, supervisor behavior, rendering, tests, and documentation together.
3. Exercise cancellation against unshared, shared, diamond-shaped, terminal-race, interrupted, and concurrently driven execution graphs.
4. Roll back only if no `cancel-pending` records have been written. If they have, complete or explicitly convert those records before running code that only understands the old lifecycle values.
