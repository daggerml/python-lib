## Context

Remote execution currently uses separate S3 objects for cache-key locks, active execution refs, launch state, lifecycle records, transport results, cancel targets, cache results, and invalidation markers. Operations that logically update one execution therefore span multiple objects and expose recovery windows to parallel callers.

This v0 change intentionally replaces that layout. There are no deployed repositories or compatibility requirements. S3 conditional create, update, and delete operations remain the coordination primitive.

## Goals / Non-Goals

**Goals:**

- Make `cache/<cache_key>` the sole binding from computation identity to its current execution attempt.
- Make `execution/<execution_id>` the sole mutable record for attempt-owned state.
- Serialize every execution-record mutation through an embedded owner lock and S3 CAS.
- Use S3 response timestamps rather than machine clocks for lease expiry.
- Preserve separate immutable caller-edge objects and adapter-owned `io/<execution_id>/` data.
- Make retries, expired-lock recovery, cancelation, and invalidation safe across callers and machines.

**Non-Goals:**

- Backward-compatible reads, dual writes, or migration from the old remote layout.
- Exactly-once adapter side effects; adapters remain responsible for idempotency by execution ID.
- Combining caller-edge objects or adapter IO into execution records.
- Using UUID7 ordering to arbitrate execution ownership.

## Decisions

### Use one cache pointer and one record per attempt

`cache/<cache_key>` contains only an execution ID. `execution/<execution_id>` contains:

```text
execution_id: str
cache_key: str | null
lifecycle: pending | running | succeeded | failed |
           cancel-requested | cancel-ready | canceled
created_at: int
updated_at: int
lock: {owner: str, ttl: float} | null
adapter_state: object | null
argv_ref: str | null
result_ref: str | null
spawned_execution_ids: list[str]
child_execution_ids: list[str]
cancelation: {requested_by: str, requested_at: int} | null
invalidation: {requested_by: str, requested_at: int} | null
```

`argv_ref` and `result_ref` are typed DaggerML ref strings. A failed adapter outcome commits an error DAG and stores that DAG as `result_ref`; both `succeeded` and `failed` are terminal cache hits when `result_ref` is present.

Alternative considered: retain separate active and terminal cache refs. Rejected because it preserves a two-pointer handoff and prevents the cache pointer from being the invariant current-execution binding.

### Create the execution before claiming the cache key

On a cache miss, a caller creates a UUID7 execution record already locked by a fresh UUID4 owner, then conditionally creates the cache pointer. If pointer creation conflicts, the caller conditionally deletes its unchanged execution record and rereads the winning pointer. UUID values provide identity only; S3 `If-None-Match` selects the winner.

Alternative considered: create the cache pointer first. Rejected because it exposes a pointer to a missing execution record.

### Embed a short owner lock in the execution record

The lock contains only `owner` and `ttl`. Acquisition changes `null` or expired lock state to a fresh UUID4 owner with CAS. Unlock changes the lock to `null` with CAS only while the stored owner still matches. Every other execution-record mutation requires matching lock ownership and CAS against the latest ETag.

An ownership epoch is unnecessary because owner UUIDs are unique, stale CAS writes fail, and a stale actor stops after rereading a different owner. External systems are not fenced by this lock; adapters are idempotent status-check interfaces and never mutate execution records.

Alternative considered: separate lock objects. Rejected because deleting an expired lock and creating its replacement requires two S3 operations and separates mutation authority from the protected state.

### Derive expiry from S3 time

For an execution read, expiry is:

```text
LastModified + lock.ttl <= Date
```

Both timestamps come from the same S3 response. `LastModified` is refreshed by every successful execution-record write, so owner mutations refresh the lease. Expiry only permits another caller to steal the lock; if an adapter call returns after expiry and the owner remains unchanged, the owner may apply the response. If another caller stole the lock, the stale caller discards the response.

Alternative considered: persist `expires_at` using caller wall time. Rejected because machine clock skew would participate in ownership decisions.

### Persist adapter state after every call

A fresh call receives `adapter_state = null`. A running response must provide object state (or preserve existing object state); terminal and cancel responses may omit nullable state. Unrecoverable malformed protocol output raises `DmlRepoError`; reported non-success invoke statuses are treated as errors. An error commits an error DAG, sets `result_ref`, marks the execution `failed`, and leaves the cache pointer intact.

Executors maintain durable external state so repeated calls for one execution ID are idempotent status checks. Adapter IO remains outside runtime control under `io/<execution_id>/`.

### Publish results in the execution record

Workers upload the DAG object graph, acquire the execution lock, and CAS `result_ref` plus terminal lifecycle into the record. This replaces `transport/<execution_id>` and terminal cache-ref publication. Cache readers resolve the execution ID and return `result_ref` only for a reusable terminal record.

### Keep edges separate

Canonical reverse caller edges remain separate create-once objects because many callers may share one execution. Forward spawned/completed summaries remain in the caller execution record and are updated while holding that caller's lock.

### Cancelation marks before pointer deletion; invalidation deletes before marking

Cancelation first acquires and marks the execution record, then conditionally deletes `cache/<cache_key>` only when it still names that execution. Invalidation acquires the selected execution lock, conditionally deletes that execution's current cache pointer, then marks the selected record even if the pointer was rebound. Readers reject marked records even if deletion was interrupted. Because `argv_ref` remains in the execution record, no cancel-target move is needed.

## Risks / Trade-offs

- [One execution record becomes a CAS contention point] -> Keep locks short, retry from fresh ETags, and surface retry exhaustion.
- [Adapter call outlives the lease] -> Accept its response only while the owner UUID still matches; otherwise discard and rely on idempotent status checks.
- [Lost cache-pointer race leaves an orphan record if conditional cleanup fails] -> Never delete changed state; remote GC may collect unreachable records.
- [S3 `Date` and `LastModified` have finite timestamp precision] -> Use conservative positive TTLs much larger than timestamp precision and request latency.
- [Crash after marking but before deleting a cache pointer] -> Cache readers inspect the execution record and treat cancelation or invalidation as a miss.
- [Layout rollback cannot read new state with old code] -> Treat rollout as an atomic v0 format replacement; rollback requires clearing the development remote root.

## Migration Plan

1. Replace execution-state storage and S3 response metadata handling.
2. Replace active/cache/transport/cancel-target workflows with unified record operations.
3. Update adapter, worker, cancelation, invalidation, edge, and GC paths.
4. Replace old-layout tests and update documentation.
5. Clear any development remote roots created by older builds before testing the new format.

No legacy reader, writer, data migration, or compatibility alias will be added.

## Open Questions

None.
