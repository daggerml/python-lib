## Context

Execution coordination currently uses a cache-key lock, an active argv ref, an execution lifecycle record, launch state, and caller/callee lineage. The active ref is both the discovery pointer for new callers and the only remote manifest from which cancellation reconstructs adapter input. That couples cancellation to mutable cache-key ownership.

Cancellation also currently mixes graph planning with adapter calls. The new design separates revoking execution ownership from cleaning up external work. Phase 1 is synchronous and lock-protected. Phase 2 is asynchronous, distributed, and driven by execution runtimes from leaves toward callers.

## Goals / Non-Goals

**Goals:**

- Prevent a canceled execution from remaining discoverable as the active attempt.
- Preserve the exact active argv manifest as an execution-specific cancel target.
- Separate adapter invocation and adapter cancellation wire contracts.
- Make nested cancellation leaves-first and safe to drive from multiple runtimes.
- Bound readiness waits with a 60-second fallback.
- Keep lifecycle ownership in the runtime rather than in adapters.

**Non-Goals:**

- Redesign cache-key identity or execution dependency recording.
- Add a new external coordination service.
- Make adapter cancellation responses authoritative for runtime lifecycle transitions.
- Change executor methods `start()`, `poll()`, or `cancel()` beyond adapting them to the new operation envelopes.

## Decisions

### Separate operation contracts

The protocol will define `AdapterInvokeRequest` / `AdapterInvokeResponse` and `AdapterCancelRequest` / `AdapterCancelResponse`. Invocation carries runnable execution and resume data. Cancellation carries cancellation-specific data, including the argv pointer needed by the cancel path, and does not reuse invocation-only fields.

The executor interface remains `start()`, `poll()`, and `cancel()`: an invoke request dispatches to `start()` or `poll()`, while a cancel request dispatches to `cancel()`.

### Preserve the active manifest by moving it

Phase 1 will move the existing `refs/active/<cache_key>.json` object to `refs/cancel-targets/<execution_id>.json`, without reconstructing or regenerating the argv manifest. The move is conditional on the source manifest still naming the target execution. The destination remains rooted in the same CAS argv closure and stays alive until Phase 2 cleanup reaps it.

An execution ID is used for the cancel-target key because the target is execution-owned, while the active pointer remains cache-key-owned.

### Phase 1 planning algorithm

The user initializes a set of execution IDs. For each ID, the planner acquires that execution's cache-key lock, retrying until it succeeds. It rechecks callers and stops processing that ID if callers remain. Otherwise it marks the record `cancel-requested`, removes each direct caller/callee edge, conditionally moves its active ref to the cancel-target ref, adds its direct callees to the work set, and releases the lock. Removing the edges at the same Phase 1 transition makes each callee eligible for planning once its final caller is canceled.

Phase 1 performs no adapter calls. Mutation rejection remains the responsibility of the execution runtime's existing lifecycle guard.

### Distributed Phase 2

Each `cancel-requested` runtime waits for its callees to reach `cancel-ready`. It then invokes cancellation for those callees using their execution-owned cancel targets, marks those executions `canceled`, and marks itself `cancel-ready`.

`cancel-ready` means descendant cleanup has reached the runtime-owned handoff point and the execution's infrastructure is eligible for reaping. If the handoff remains in `cancel-ready` for more than 60 seconds, a runtime may invoke the cancel adapter anyway. Cancellation calls and lifecycle claims must be idempotent or conditionally claimed so normal and timeout paths cannot corrupt state.

### Lifecycle authority

Adapters report operation results only. The runtime writes `cancel-requested`, `cancel-ready`, and `canceled`, and decides when cancel-target refs can be deleted. Adapter responses cannot independently publish runtime lifecycle state.

### Documentation alignment

Update the execution and adapter documentation to describe the actual ref and execution-state keyspaces. Remove `argv_ptr` from invocation documentation; document it only where the cancel request explicitly carries the argv pointer required by cancellation.

## Risks / Trade-offs

- [Ref move failure] A conditional move can race with relaunch or another planner → hold the cache-key lock, verify the source execution ID, and make Phase 1 retry/idempotent.
- [Duplicate cancellation] Normal and timeout cleanup can target the same execution → use conditional lifecycle claims and require executor cancellation to be idempotent.
- [Stale cancel targets] A target ref can outlive adapter cleanup → delete it only after runtime-owned cleanup handling and make later deletion safe.
- [Lock expiry] A cache-key lock can expire during long coordination → retain current lock semantics for planning and keep adapter work outside Phase 1 locks.
- [Protocol compatibility] Existing adapters expect one mixed envelope → update adapter CLI parsing and executor dispatch together with the runtime contract.
