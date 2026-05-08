## Context

Remote execution lineage is currently keyed by cache key and spread across multiple mutable S3 indexes. That makes administrative graph operations awkward because cache invalidation and cancellation need transitive closure over callers, but the existing storage duplicates forward and reverse lineage in shared JSON objects. Repeated `start_fn` polling also means dependencies are discovered incrementally rather than all at first launch.

This change moves graph identity to `execution_id`, keeps S3 as the source of truth, and assumes users will periodically ingest remote execution objects into a local database to plan rare manual invalidation and cancellation operations.

## Goals / Non-Goals

**Goals:**
- Represent dependency history using immutable execution-edge records keyed by execution id.
- Keep remote writes simple and S3-friendly for many asynchronous writers.
- Make cache invalidation and cancellation planning straightforward in a local SQL database.
- Preserve retry and rerun semantics by treating cache refs as pointers to current executions rather than as graph node identities.

**Non-Goals:**
- Introduce a shared remote transactional database.
- Define a fully automated background reconciler; planning remains a user-driven local operation.

This design explicitly rejects any backward compatibility with the existing cache-key lineage layout. The new execution-id-based model replaces the prior remote structure wholesale.

## Decisions

### Use execution id as graph-node identity
Execution lineage, invalidation markers, and cancellation markers will all be keyed by `execution_id`. `cache_key` remains the computation identity used for lock ownership and cache lookup, but no longer serves as the lineage node identity.

This decision provides no backward compatibility with cache-key lineage identities. The old lineage model is explicitly unsupported by this change.

Why this approach:
- reruns naturally create new graph nodes without reinterpreting historical edges
- invalidation and cancellation stay precise across retries
- execution history remains immutable and auditable

Alternative considered:
- continue using cache-key graph nodes and recursively rewrite cache-key lineage objects. Rejected because cache keys are reused across attempts and make administrative graph semantics ambiguous.

### Store canonical reverse edges in S3
The source-of-truth dependency relation will be stored as immutable objects at `exec/edges/<callee_eid>/<caller_eid>.json`. A caller writes this edge when it concretely discovers the dependency, even if discovery happens on a later poll.

This storage layout is a clean replacement for the old call-edge layout. No backward-compatible reads or writes to the prior lineage paths are allowed. In particular, nothing in the new implementation reads from `calls/from/...` or `calls/to/...`.

Why this approach:
- invalidation needs reverse-caller traversal, so listing by callee is the hot administrative query
- object creation is idempotent because the path is canonical
- no shared read/merge/write reverse index is needed

Alternatives considered:
- store edges by caller and rebuild reverse closure by scanning all edges. Rejected because manual invalidation would require expensive reverse scans.
- store both directions as shared mutable indexes. Rejected because multi-writer maintenance is error-prone on S3.

### Use one mutable execution object per execution id
`exec/state/<execution_id>.json` becomes the single execution object, updated with compare-and-swap semantics and monotone merges. It stores durable execution record fields such as `created_at`, plus runtime status, current durable adapter `state`, discovered `dependencies`, and `cancel_requested_by` when cancellation is requested.

This execution object replaces all prior execution-record and live-summary shapes without a compatibility layer.

Allowed execution `status` values are `running`, `cancel-requested`, `cancelled`, `succeeded`, and `failed`.

The `exec/state/<execution_id>.json` schema is:

```json
{
  "execution_id": "E1",
  "cache_key": "ck1",
  "created_at": 1760000000,
  "status": "running",
  "state": {},
  "dependencies": ["E2", "E3"],
  "updated_at": 1760000000,
  "cancel_requested_by": null
}
```

Why this approach:
- the split record/live model was not buying enough value to justify extra storage and coordination
- one execution object keeps durable record fields, adapter state, status, and dependencies in one place
- compare-and-swap on one execution-owned summary is simpler than mutating shared graph indexes

Alternative considered:
- split immutable records from mutable live summaries. Rejected because the only immutable fields were trivial and the split complicated reads and writes.

### Treat cache refs as projections onto execution history
`refs/cache/<cache_key>.json` will remain a normal cache ref to the current manifest for that cache key, and SHALL also record the current `execution_id` for graph planning. Invalidation starts by resolving a cache key to its current execution id from that ref metadata, then walking the execution graph locally.

This cache-ref meaning changes as part of the clean replacement. The old cache-key lineage interpretation is not preserved. Cache publication is create-only per cache key path; reruns must invalidate the current cache ref before a later execution republishes that cache key.

Why this approach:
- the cache remains a current-view pointer rather than the graph itself
- invalidating one historical execution clears the cache-key path so a later rerun can republish a fresh execution id

Alternative considered:
- invalidate cache keys directly as graph identities. Rejected because cache keys alias multiple executions over time.

### Plan invalidation and cancellation locally, commit control state remotely
Users will ingest `exec/state`, `exec/edges`, and `refs/cache` into a local database. Invalidation writes immutable `exec/invalidate/<execution_id>.json` tombstones for the reverse caller closure and deletes cache refs that point at invalidated executions. Cancellation updates `exec/state/<execution_id>.json` to `cancel-requested` for the requested execution and any propagated closure where no live callers remain.

These admin flows operate only on the new execution-id layout. There is no backward-compatible support for the prior cache-key lineage paths, and no admin flow reads from `calls/from/...` or `calls/to/...`.

The invalidation walk uses the current cache projection as a guardrail:

```text
seen = []
seen_set = set()
unseen = set(ref.execution_id for each existing user-requested cache ref)

while unseen:
  exec_id = pop(unseen)
  state = read exec/state/<exec_id>.json
  if missing:
    continue
  cache_ref = read refs/cache/<state.cache_key>.json
  if missing:
    continue
  if cache_ref.execution_id != exec_id:
    continue
  seen.append(exec_id)
  seen_set.add(exec_id)
  unseen |= callers(exec_id) - seen_set

for exec_id in reversed(seen):
  create exec/invalidate/<exec_id>.json with create-once/CAS semantics
  delete refs/cache/<cache_key>.json only if it still points to exec_id
```

This ensures historical executions are skipped once a cache key has advanced to a newer execution id, and that cache-ref deletion does not race away a newer publication.

The cancellation walk is the forward dual over execution dependencies:

```text
seen = []
seen_set = set()
unseen = set(user_requested_exec_ids)

while unseen:
  exec_id = pop(unseen)
  state = read exec/state/<exec_id>.json
  if missing:
    continue
  if state.status in {"succeeded", "failed", "cancelled"}:
    continue
  seen.append(exec_id)
  seen_set.add(exec_id)
  unseen |= set(state.dependencies) - seen_set

for exec_id in reversed(seen):
  state = reread exec/state/<exec_id>.json
  if missing:
    continue
  if state.status in {"succeeded", "failed", "cancelled"}:
    continue
  caller_count = number of callers of exec_id
                 whose state exists
                 and whose status is not in {"cancel-requested", "cancelled", "succeeded", "failed"}
  if caller_count > 1:
    continue
  CAS update exec/state/<exec_id>.json to status = "cancel-requested"
  and set cancel_requested_by
```

This yields inside-out cancellation: dependencies are processed before their callers, terminal executions are pruned, and a dependency is skipped while more than one uncancelled caller still points to it.

The `exec/invalidate/<execution_id>.json` schema is:

```json
{
  "execution_id": "E4",
  "cache_key": "ck1",
  "requested_by": "alice@example.com",
  "requested_at": 1760000000
}
```

Why this approach:
- closure queries and live-caller checks are easy in SQL and awkward in S3
- S3 remains append-mostly for writers and marker-based for admins
- user-driven admin actions are infrequent, so local planning is acceptable

Alternative considered:
- maintain a shared mutable database on S3. Rejected because whole-database compare-and-swap creates poor concurrency and brittle failure modes.

## Risks / Trade-offs

- [Late dependency discovery means graph snapshots can lag runtime progress] → Treat `exec/edges/*` and `exec/state/*` as eventually complete during running execution; planners should refresh before computing closures.
- [State-object updates may conflict when multiple actors touch the same execution summary] → Require compare-and-swap writes and monotone merges so retries are deterministic.
- [Deleting cache refs after invalidation can race with later successful reruns] → Invalidation is execution-targeted; deleting refs is conditioned on their recorded `execution_id` belonging to the invalidated set.
- [The new remote layout is incompatible with the prior cache-key lineage layout] → Treat this as a clean replacement and remove all prior-path reads and writes in the same change, including any reads from `calls/from/...` and `calls/to/...`.

## Migration Plan

1. Implement the execution-id remote layout under `exec/state/*`, `exec/edges/*`, `exec/invalidate/*`, and `refs/cache/*`.
2. Update runtime writers and readers to use only the new execution-id layout.
3. Update local/admin tooling to ingest the new S3 layout into a local database and compute invalidation/cancellation closures from execution ids.

Rollback strategy:
- None. This change is a clean replacement and does not preserve a backward-compatible path.

## Open Questions

None.
