## Context

The current execution flow uses `cache_key` as both the cache identity and the practical lookup key for multiple runtime concerns: lock ownership, executor-private resumable state, and some nested transport handoff behavior. That spreads state across runtime-owned and executor-owned prefixes and makes stale-lock recovery, nested execution behavior, and observability difficult to reason about.

This change introduces a clearer split between identities and responsibilities. `cache_key` remains the stable identity for the computation and cache entry. `execution_id` becomes the stable identity for one in-flight execution attempt. The runtime owns lock state, active execution pointers, immutable execution records, and call-edge lineage indexes. Adapters receive enough information to launch or poll, but no longer own the durable execution-state layout.

This is a cross-cutting change because it affects runtime execution flow, adapter envelopes, executor resumability assumptions, failure publication, and S3 data layout.

## Goals / Non-Goals

**Goals:**
- Separate computation identity (`cache_key`) from in-flight execution identity (`execution_id`).
- Make the runtime authoritative for durable execution ownership and stale-lock recovery.
- Create immutable execution records that capture the launch-time durable state required for all later polls.
- Remove `pending` and simplify adapter status handling to `running|succeeded|failed`.
- Publish failed executions into cache after completing the DAG with the error.
- Persist caller/callee lineage for both user-dag to fn-dag calls and fn-dag to fn-dag calls in both query directions.

**Non-Goals:**
- Tracking multiplicity or per-attempt history for repeated caller/callee edges.
- Defining a global append-only call log.
- Preserving executor-owned resumable-state prefixes for backwards compatibility.
- Introducing mutable execution records or heartbeat-updated execution metadata.

## Decisions

### 1. Runtime-owned active pointer and immutable execution record

The runtime will store:

- `active/<cache_key>`: plain-text `execution_id`
- `exec/<execution_id>.json`: immutable execution record created only on the first non-terminal adapter result for that execution

The execution record stores the launch-time durable state returned by the adapter and is never updated. Later polls reuse that stored state.

Why:
- Separates the stable cache identity from the in-flight execution identity.
- Avoids executor-private durable-state layouts.
- Makes stale-lock recovery resume the existing execution rather than invent a new one.

Alternatives considered:
- Reusing `cache_key` as the only durable identity: rejected because it conflates cache lookup, locking, and execution resumption.
- Mutable execution records: rejected because the design goal is a one-time persisted launch snapshot with simpler reasoning.

### 2. Lock recovery preserves execution identity

`start_fn` still locks by `cache_key`, but stale-lock recovery must preserve the current `execution_id` whenever `active/<cache_key>` points to an existing execution record.

Why:
- Lock ownership is transient coordination.
- Execution identity is the durable handle for the in-flight attempt.
- A stale lock should never silently fork a duplicate execution if resumable state exists.

Alternatives considered:
- Creating a new execution on stale-lock recovery: rejected because it risks duplicate launches for the same computation.

### 3. Adapter contract includes `execution_id` and initial `state`

The adapter envelope includes `execution_id` and `state`.

- First call for a new execution passes `state = null`.
- Resume calls pass the immutable stored state from `exec/<execution_id>.json`.

Adapters may return `state` on later calls, but the runtime ignores replacement state after the execution record is created.

Why:
- Adapters may need a stable execution-scoped identifier for external naming or storage.
- The first launch call is the right moment to return all durable resume handles.
- Ignoring replacement state keeps the runtime model simple and forces launch-time completeness.

Alternatives considered:
- Omitting `execution_id` from the adapter envelope: rejected because adapters may need an execution-scoped namespace distinct from `cache_key`.
- Persisting updated adapter state on every poll: rejected because it reintroduces mutable execution-state complexity.

### 4. Adapter statuses reduce to `running|succeeded|failed`

`pending` is removed. New executions either:

- return `running` with durable launch state,
- return `succeeded` with `dag_id`, or
- return `failed` with `error`.

Why:
- There is no longer a separate runtime notion of pre-launch in-flight state.
- The first call is expected to launch the work or complete synchronously.

Alternatives considered:
- Keeping `pending`: rejected because it does not carry unique semantics in the new model.

### 5. Failed execution is cached after DAG error completion

On adapter `failed`, the runtime completes the DAG with the error and publishes that failed outcome to cache, mirroring success publication.

Why:
- Failure is still a terminal result for a specific computation.
- Caching terminal failures avoids repeated duplicate launches for deterministic failures.

Alternatives considered:
- Leaving failures uncached: rejected because it preserves duplicate work and ambiguous terminal state.

### 6. Call-edge lineage uses per-caller and per-callee list objects

The runtime stores:

- `calls/from/index/<index_id>.json` -> sorted, deduped list of callee cache keys
- `calls/from/cache/<caller_ck>.json` -> sorted, deduped list of callee cache keys
- `calls/to/cache/<callee_ck>.json` -> object with sorted, deduped `indexes` and `cache_keys` lists

Definitions:
- user-dag: a DAG without an `argv` node and therefore without a cache key
- fn-dag: a DAG with an `argv` node and therefore with a cache key

Edges are written only on the new-execution path, immediately after the lock is acquired and inactive state is confirmed.

Why:
- User-dags can be callers but never callees, so their forward index can stay separate and simple.
- Reverse lookup for a callee must support mixed caller types.
- Writing once on new execution avoids duplicate lineage writes during resumes.

Alternatives considered:
- Per-edge objects: rejected for now in favor of fewer S3 objects and simpler query layout.
- Writing lineage only on success: rejected because lineage should represent attempted invocation, not only successful completion.

### 7. Edge files update via read/merge/retry with ETag checks

Each edge update performs:

1. read current object,
2. merge new member,
3. dedupe and sort,
4. conditional write with ETag,
5. retry the full sequence on conflict.

Why:
- This preserves correctness under concurrent writers without introducing a separate index service.

Alternatives considered:
- Blind overwrite: rejected because it loses concurrent updates.

## Risks / Trade-offs

- Immutable execution records require adapters to return all durable resume handles on the first launch call -> Mitigation: make that a hard adapter contract and validate it in executor tests.
- Terminal failure caching may preserve transient infrastructure failures longer than desired -> Mitigation: scope cache identity and retry policy carefully, and document that only deterministic adapter failures should be surfaced as terminal results.
- List-based edge indexes can see write contention on high fan-in/fan-out cache keys -> Mitigation: use ETag-based full retries and accept that per-edge objects remain a future escape hatch.
- Stale `active/<cache_key>` pointers could block reuse if execution records are deleted or corrupted -> Mitigation: treat active pointers with missing execution records as stale and delete them before proceeding.
- Execution records and call-edge indexes add new long-lived S3 objects -> Mitigation: keep payloads minimal and rely on deterministic naming for inspection and cleanup tooling.

## Migration Plan

1. Introduce the new adapter envelope and result validation rules.
2. Implement runtime support for lock recovery, active pointers, immutable execution records, and failed-result cache publication.
3. Update contrib adapters and executors to return launch-time durable state on first `running` result and to ignore persisted mutable executor-owned state.
4. Add call-edge persistence on the new-execution path.
5. Remove obsolete executor-owned resumable-state usage and documentation.
6. Backfill or discard older in-flight state as a one-time compatibility break; no migration of legacy execution records is required.

## Open Questions

- Whether execution records should be retained after terminal success/failure or garbage-collected once the cache entry and active pointer are resolved.
- Whether runtime responses for a live lock should remain `None`-like or become a richer descriptive non-terminal result surfaced to callers.
- Whether failed-result cache publication should distinguish deterministic execution failures from infrastructure/transient failures in the cached payload.
