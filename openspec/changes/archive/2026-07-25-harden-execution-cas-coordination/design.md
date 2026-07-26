## Context

Execution records are S3-backed CAS documents. `get_or_start_fn()` reserves or reuses a callee execution, records a caller/callee edge, appends the callee ID to the caller's `spawned_execution_ids`, and then invokes the adapter. Cancellation planning updates that same caller record before traversing its spawned IDs.

This shared CAS record is already the coordination boundary: a successful spawn append is visible to a conflicting cancellation update, while a successful cancellation update prevents later spawn appends. The current implementation breaks that boundary by treating bounded CAS retry exhaustion as success.

## Goals / Non-Goals

**Goals:**

- Preserve the existing execution-record lifecycle model without adding a planning-only lifecycle value.
- Ensure adapter invocation follows successful durable child registration.
- Make failed launch registration clean up only attempt-owned coordination artifacts.
- Make terminal child bookkeeping retry contention rather than silently abandoning record updates.
- Preserve canceled direct children as uncompleted execution lineage.

**Non-Goals:**

- Change public runtime APIs, adapter protocols, or S3 object formats.
- Add a background reconciliation service or unbounded request-time retries.
- Change shared-callee cancellation behavior.

## Decisions

### Treat successful caller-record CAS as the launch fence

`_add_spawned_execution()` and cancellation planning update the same caller execution record. Both operations will reread and retry after CAS conflicts with bounded exponential backoff. A registration attempt that cannot append its child after the retry budget will raise; `get_or_start_fn()` will not invoke the adapter.

This keeps a single authoritative coordination record rather than introducing a transient lifecycle value and reader wait protocol.

### Roll back only launch-attempt-owned artifacts

The caller/callee edge is written before the caller-record append, so a cancellation planner can safely reason about live callers whenever it observes a spawned child. If registration fails, the attempt removes that edge. If the attempt reserved and published a fresh active execution, it also removes that attempt's active reference and reservation record. A reused active execution is not rolled back because it may belong to another caller.

### Preserve canceled children in spawned lineage

`spawned_execution_ids` denotes direct children that have not reached a normal terminal completion. `child_execution_ids` denotes direct children that reached `succeeded` or `failed`. A canceled child remains in `spawned_execution_ids`; cancellation consumes its lifecycle as a satisfied descendant but does not mutate the caller record merely to erase it. The cancellation-only `_finalize_spawned_edge()` mutation is removed.

### Bound retries and surface exhaustion

Both child registration and terminal-child bookkeeping use bounded exponential backoff. Exhaustion is an explicit coordination failure, never a log-and-continue outcome. Completion handling must retain enough coordination state to make a later terminal poll safe to retry; it must not discard a terminal result merely to clean up contention.

## Risks / Trade-offs

- [Persistent S3 contention or outage] -> Bounded retries surface a retryable failure rather than hanging a worker or silently corrupting lineage.
- [Rollback deletes shared coordination state] -> Track whether the current call reserved the active execution and only remove attempt-owned objects.
- [Canceled children remain in spawned summaries] -> Cancellation driving treats a canceled child as satisfied; graph consumers read child lifecycle to distinguish cancellation from active work.
- [Terminal completion retries later] -> Preserve the terminal execution identity and result-bearing state until terminal bookkeeping succeeds.
