## Context

Cancellation should be one synchronous control-plane operation. `Dml.runtime.cancel` is the public entrypoint, but `IndexOps.cancel` is the real cancellation engine. The runtime owns execution-state mutations. Adapter chains own execution and cancellation of their jobs. During cancellation, the runtime notifies only direct child adapter chains; those chains delegate nested cancellation when needed, then tear down their own resources.

## Goals

- Make `Dml.runtime.cancel` a thin public entrypoint.
- Make `IndexOps.cancel` the single cancellation engine.
- Treat index roots and execution ids uniformly.
- Remove detached-cancellation semantics.
- Clear active ownership immediately on cancel.
- Process direct child edges concurrently.

## Non-Goals

- Deleting cache refs.
- Preserving `cancel-detached` semantics.
- Making edge records permanent after cancellation.

## Decisions

### 1. One synchronous cancel flow

`Dml.runtime.cancel` is a thin public entrypoint. `IndexOps.cancel` performs the full cancellation flow for one execution or index-root id.

For an index root:

1. Move `indexes/<id>.json` to `indexes/.cancelled/<id>.json` if it is still live.
2. Continue using `<id>` as the execution identity.

For any execution id:

1. Read `exec/state/<id>.json`.
2. Lock `active/<cache_key>` using the execution's `cache_key`.
3. Delete `active/<cache_key>` if present.
4. Unlock.
5. Persist `lifecycle = "cancel-pending"` and `cancellation_requested_by`.
6. Read `spawned_execution_ids`.
7. Process direct child edges concurrently in a thread pool.
8. Process direct children concurrently by asking the adapter chain responsible for each child to handle cancellation for that child exactly once.
9. Persist `lifecycle = "cancelled"`.
10. If this id is an index root, delete `indexes/.cancelled/<id>.json`.

`cancelled` means the runtime finished this synchronous control-plane step for `this_exec`: child worker tasks finished, and any nested `cancel(child)` calls delegated by those child adapter chains have returned. It does not mean more than that.

Adapter cancel return values are ignored for lifecycle purposes for now.

### 2. Runtime owns state writes

Cancellation state is runtime-owned.

The runtime writes:

- `exec/state/<id>.json`
- `active/<cache_key>`
- `exec/edges/<callee>/<caller>.json`
- index `.cancelled` pointers

Adapters do not write lifecycle state directly.

### 3. Adapter chains own child job cancellation

The runtime does not directly mutate child execution records from the parent cancel call. Instead, each child worker asks the adapter chain responsible for that child job to handle cancellation. That chain owns both execution and cancellation for the child job.

If that child has nested runtime work, the adapter chain may call `Dml.runtime.cancel(child)` at most once for that child execution, then the chain tears down its own infrastructure and returns. By convention, the last adapter in the chain usually owns both kickoff and cancellation, but that is not a required contract.

This avoids duplicate recursive cancellation through nested adapter chains and keeps child state changes inside the child's own runtime cancel call.

### 4. Cancellation requester is provenance

`cancellation_requested_by` records the immediate requester for the current cancel call.

It may be:

- a user identity for top-level cancellation
- the immediate parent execution id for nested cancellation

### 5. Direct child edges are processed concurrently

For one `cancel(this_exec)` call, all direct `spawned_execution_ids` are processed in parallel with a thread pool.

Each worker:

1. Removes `exec/edges/<child>/<this_exec>.json` if present.
2. Invokes the adapter chain responsible for that child once.
3. Lets that chain call `Dml.runtime.cancel(child)` at most once for that child if nested runtime cancellation is needed.
4. Ignores the child's adapter result for lifecycle purposes.

The runtime waits for the worker pool to finish before marking `this_exec` as `cancelled`.

### 6. Edges are live caller relationships, not durable history

Persisted edge objects represent live caller relationships. They may be observed as history only until cancellation removes them. Durable child history lives in `spawned_execution_ids`, not in persisted edge objects.

## Risks

- Nested adapter chains can recurse twice if ownership of `cancel(child)` is unclear.
- Synchronous cancellation can block longer than the detached model.
- Ignoring adapter cancel results weakens immediate error reporting.

## Open Questions

- Whether the child adapter cancel invocation should continue using the existing result envelope or move to a smaller success/failure-only payload.
