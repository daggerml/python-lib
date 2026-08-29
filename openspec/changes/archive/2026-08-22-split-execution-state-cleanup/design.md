## Context

See `proposal.md` for motivation. Today one S3 JSON object combines immutable identity, semantic execution outcome, adapter continuation state, caller locking, controls, and forward lineage. Its embedded owner lock is held across adapter calls and gates every mutation, so a funk runtime cannot publish its result independently. Callers then use the published terminal lifecycle/result as a cache fast path, which can bypass executor teardown currently embedded in terminal polling.

The cache pointer and caller/callee edge layout remain separate and unchanged. The execution storage and adapter protocol are still v0, so this design intentionally provides no legacy parser or migration shim.

## Goals / Non-Goals

**Goals:**

- Give immutable identity, shared semantic state, and caller driver coordination distinct schemas and CAS domains.
- Let a funk runtime publish a normal result without acquiring or waiting for the adapter-driver lock.
- Preserve atomic semantic races such as result publication versus cancellation within one state object.
- Make normal external-resource pruning explicit, idempotent, observable, and independent from execution lifecycle.
- Let adapter backpressure delay every caller for an execution rather than only one polling loop.
- Keep `invoke` as the sole launch/status-check operation; do not add `poll`.

**Non-Goals:**

- Compatibility with unified v0 execution records or old adapter response statuses.
- Changes to cache-key identity, cache-pointer layout, caller-edge layout, or DAG result semantics.
- Guaranteed cleanup when every caller disappears permanently; this design makes cleanup resumable and demand-driven but does not add a background reconciler.
- Exactly-once adapter side effects; operations remain idempotent and may repeat after lost responses or stolen locks.

## Decisions

### Split one execution into three S3 objects

Each execution uses these keys:

```text
exec/execution/<execution_id>/metadata.json
exec/execution/<execution_id>/state.json
exec/execution/<execution_id>/driver.json
```

`metadata.json` is immutable after conditional creation:

```json
{
  "execution_id": "string",
  "cache_key": "string|null",
  "argv_ref": "node-argv ref|null",
  "created_at": "integer timestamp"
}
```

`state.json` is the semantic CAS domain:

```json
{
  "lifecycle": "pending|running|succeeded|failed|cancel-pending|canceled",
  "result_ref": "dag ref|null",
  "result_source": "runtime|adapter-error|null",
  "spawned_execution_ids": ["string"],
  "child_execution_ids": ["string"],
  "cancelation": "control record|null",
  "invalidation": "control record|null",
  "updated_at": "integer timestamp"
}
```

`driver.json` is the adapter-driver CAS domain:

```json
{
  "lock": {"owner": "string", "ttl": "positive number"},
  "not_before": "integer timestamp|null",
  "adapter_state": "object|null",
  "cleanup": {"status": "complete|failed", "error": "string|null"}
}
```

The nullable fields remain nullable rather than using placeholder state objects. No schema-version fields are stored. Alternative considered: retain one nested JSON record for clearer grouping. Rejected because unrelated writes would still share the driver lock and one high-contention CAS target.

### Keep semantic races in state and adapter serialization in driver

The driver lock serializes adapter calls and driver mutations only. The lock owner must revalidate ownership after every external adapter call before persisting its response. Lock expiration continues to use the `LastModified` and `Date` values from the same S3 driver response.

Every state writer uses conditional updates with bounded deadline-based exponential backoff and jitter. A retry rereads state and re-evaluates semantic preconditions; it stops successfully if another writer already established an equivalent fact, fails deliberately if the intended transition became invalid, and surfaces retry exhaustion. Runtime result publication therefore remains possible while a caller holds the driver lock.

Alternative considered: require the driver lock for state changes. Rejected because it recreates the result-publication blockage. Alternative considered: split lifecycle, result, controls, and lineage into still more files. Rejected because cancellation/result and lineage/control transitions benefit from one semantic snapshot and CAS boundary.

### Create all execution objects before publishing the cache pointer

Reservation conditionally creates metadata, state, and driver, then conditionally creates the unchanged plain-text cache pointer. A loser conditionally deletes only the unchanged objects it created. A pointer is usable only when all three objects validate. A pointer to a legacy unified record or partial three-object execution is stale and is conditionally repaired or removed.

Alternative considered: lazily create driver state on first adapter call. Rejected because cache-pointer publication would expose a partial attempt and complicate stale-pointer recovery.

### Assign field authority instead of whole-record authority

The funk runtime publishes a normal `result_ref` and `result_source = "runtime"` without changing lifecycle. The coordinating caller transitions such an active state to `succeeded`. A caller handling adapter failure creates the error DAG and atomically stores `result_source = "adapter-error"`, the result ref, and lifecycle `failed`. A runtime-published DAG containing a user-level Error is still lifecycle `succeeded`; `failed` remains reserved for execution-path failure.

Graph orchestration mutates forward lineage through guarded state CAS. Cancellation and invalidation mutate their controls and lifecycle through guarded state CAS, using the driver lock only where they must serialize adapter calls or caller-edge registration. Adapters never receive complete execution files.

Alternative considered: derive terminal lifecycle directly from result-ref presence. Rejected because runtime results and caller-synthesized adapter-error DAGs have different lifecycle meanings.

### Derive the next adapter operation from current state

The driver stores no operation discriminator:

```text
cancel-pending                         -> cancel
no result_ref                          -> invoke
result_ref + cleanup == null           -> cleanup
result_ref + cleanup complete|failed   -> no adapter call
```

Repeated `invoke` carries execution ID plus current adapter state. Null state dispatches executor start; object state dispatches executor status inspection. There is no poll operation. Cancellation ignores `not_before`.

Alternative considered: persist the operation associated with a retry. Rejected because it can become stale when the funk publishes a result while the retry delay is active; deriving from current state is both smaller and correct.

### Use explicit cleanup with outcome-independent semantics

Cleanup requests use this projection:

```json
{
  "operation": "cleanup",
  "execution_id": "string",
  "cache_key": "string",
  "remote": {"root": "string"},
  "runnable": "object",
  "adapter_state": "object|null",
  "scratch_uri": "string",
  "result_ref": "dag ref"
}
```

Invoke and cleanup return `status = "success"`, `status = "retry"`, or another nonempty failure code. Retry requires resumable object adapter state and may include nonnegative `retry_after_ms`; failure requires nonempty error text. Cleanup success writes `{status: "complete", error: null}`. Cleanup retry leaves cleanup null and updates driver continuation/backpressure state. Cleanup failure writes `{status: "failed", error: <message>}`. No cleanup response changes lifecycle or result.

The nullable cleanup marker is necessary even though cleanup is idempotent: without it every cache hit would call cleanup forever, while omitting cleanup calls would recreate the leak. Alternative considered: infer completion from null adapter state. Rejected because null also represents a fresh or synchronous executor and does not prove external resources were pruned.

### Persist shared adapter backpressure as an absolute not-before timestamp

On retry, the owner writes `not_before = now + retry_after_ms`, using a standard shared delay when no hint is supplied. The design assumes sufficiently aligned caller clocks for v0 not-before enforcement. Every invoke or cleanup driver acquires the lock, rereads both files, and skips the adapter while not-before remains future. Once eligible, the action is derived from current state. A non-retry response clears not-before.

Alternative considered: keep retry timing only in each API polling loop. Rejected because concurrent callers could serialize through the lock yet still hammer the same backend. Alternative considered: store an operation with the timestamp. Rejected because result publication can change the next action during the delay.

### Move built-in normal teardown into cleanup

`ExecutorBase` replaces the unused `gc` hook with explicit `cleanup`; its dispatcher accepts invoke, cleanup, and cancel. Local and Lambda adapters carry the new request/response contract. Provider throttling that can be identified reliably maps to retry with a delay; unclassified transport errors remain failures.

Script invoke/status inspection stops deleting its work directory. Cleanup waits while required supervisor finalization is active, then reaps and removes the directory. Docker status inspection stops removing containers or temporary images; cleanup waits for safe terminal state and removes both. Batch cleanup prunes its temporary job definition and execution-owned resources after safe terminal observation. SSH uses durable nested adapter state across fresh calls and forwards cleanup rather than relying on one permanently polling SSH process.

Docker and Batch currently run nested adapters inside ephemeral environments with `--poll`. Their internal adapter driver must complete or terminally record nested cleanup before that environment exits; outer cleanup separately removes wrapper resources. This is required because nested filesystem/process state is no longer reachable after the container or Batch task exits.

Alternative considered: leave teardown in terminal invoke and add cleanup only as a fallback. Rejected because result publication intentionally causes callers to stop invoking, making the terminal path unreliable.

### Make cache return and cleanup progress compatible

Terminal cache usability remains based on lifecycle, result, cancelation, and invalidation, not cleanup. A caller finding a reusable result offers the execution driver one chance to advance cleanup when the lock and not-before permit, then returns the result regardless of pending or failed cleanup. If another caller owns the lock, that owner is responsible for progress. This avoids making successful computation unavailable because resource pruning is delayed.

## Risks / Trade-offs

- [Cross-file updates are not atomic] -> Keep each invariant within one file, order reservation before pointer publication, and make every cross-file workflow idempotent and resumable.
- [Clock skew can shorten or extend backpressure] -> Explicitly accept aligned caller clocks for v0 and keep lock expiry on S3 response time.
- [A driver can die after an adapter side effect] -> Require operation idempotency by execution ID, persist durable continuation state, and allow lock stealing after TTL.
- [All callers can disappear before cleanup] -> Preserve cleanup-pending state for later cache callers; a background reconciler remains outside this change.
- [Cleanup failure can leave real resources] -> Persist terminal cleanup diagnostics without corrupting the reusable execution outcome.
- [State CAS contention increases because it is unlocked] -> Use bounded deadline-based retries with jitter and semantic convergence checks.
- [Nested ephemeral execution has two cleanup layers] -> Require nested driver cleanup before environment exit and outer wrapper cleanup afterward.
- [No legacy compatibility invalidates old execution caches] -> Treat old or partial records as stale and require a fresh execution under the v0 layout.

## Migration Plan

1. Land storage readers/writers, state CAS helpers, and driver locking as one v0 protocol transition.
2. Land adapter response types, explicit cleanup dispatch, and all built-in executor changes in the same release so no mixed protocol is supported.
3. Update runtime/cache inspection, cancellation, invalidation, and lineage logic to read the split objects.
4. Treat cache pointers to unified records as stale; leave unreachable legacy execution objects for normal remote-prefix cleanup rather than parsing them.
5. Roll back by reverting the release and using a fresh remote execution prefix; split records are not readable by the old runtime.
