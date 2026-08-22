## Context

`cache/<cache_key>` contains the current execution ID, while `execution/<execution_id>` owns lifecycle, result, lineage, and administrative state. The public cache API currently hides the pointer identity and accepts cache keys for invalidation. Existing invalidation follows caller IDs back through their cache keys, which can substitute a newer execution after pointer rebound.

Public exact execution identities are `index:` or `frozenindex:` `Ref` values. Lower execution-state layers remain string-ID addressed. The generated CLI derives commands and parsers from the public `Dml` signatures.

## Goals / Non-Goals

**Goals:**

- Expose the execution and reusable DAG identities currently associated with a cache key.
- Make explicit invalidation targets stable execution identities.
- Preserve pointer rebound while pruning historical propagated callers.
- Keep execution-ID traversal and cache-pointer eligibility separate.

**Non-Goals:**

- Change cache-key computation or `Dml.cache.get` behavior.
- Change cancellation planning, driving, readiness, or timeout behavior.
- Add historical DAG-to-execution or cache-key-to-execution indexes.
- Add compatibility aliases for cache-key invalidation.

## Decisions

### Return a structured cache description

`Dml.cache.describe(cache_key: str) -> CacheDescription | None` returns:

```text
execution: Ref
dag: Ref | null
lifecycle: EXECUTION_LIFECYCLES
```

The operation reads one cache-pointer snapshot and then reads that exact execution record. It does not reread the pointer to substitute a rebound execution. An absent pointer or missing selected execution record returns `None`; cleanup of a dangling pointer, if retained, uses only the original conditional snapshot so it cannot delete a rebound pointer.

The execution ref is `index:<execution_id>`. The DAG ref is copied from `result_ref` without materializing its object graph and is exposed only when the selected record is an unmarked `succeeded` or `failed` reusable terminal result. This separates identity inspection from `cache.get`, which materializes a reusable cached DAG.

Alternative considered: expand `cache.get` to return execution metadata. Rejected because it would change a simple result lookup and could not represent running entries cleanly.

### Accept execution refs at both administrative boundaries

`Dml.cache.invalidate(*executions: Ref)` and `Dml.runtime.cancel(execution: Ref, *, mode=...)` validate public runtime refs and delegate their IDs exactly once. Invalidation accepts both `index:` and `frozenindex:` refs, matching other runtime inspection controls. The cancellation parameter rename changes keyword spelling only; cancellation behavior is unchanged.

Alternative considered: accept bare execution-ID strings. Rejected because exact public runtime and execution identities use `Ref`, while strings remain the lower-level representation.

### Separate explicit-root selection from caller eligibility

Invalidation keeps an explicit root set and a pending execution-ID queue. Missing records are skipped and all deduplication uses execution IDs.

An explicit root is selected whenever its record exists, regardless of whether its cache pointer is absent or rebound. Under its execution lock, the runtime conditionally deletes a matching pointer, marks the exact root, and then queues its caller IDs directly from edge objects.

A propagated caller is eligible only when its locked record has a cache key and the current pointer still names that caller execution. A successful conditional deletion establishes eligibility; the runtime then marks it and queues its callers. An absent, rebound, or concurrently changed pointer prunes the branch. The replacement pointer value is never queued.

```text
edge p1 -> e1, cache/ck-p -> p2

explicit e1: select and mark
caller p1:   pointer does not name p1, prune
replacement p2: never selected
```

This preserves the existing delete-before-mark order. It also ensures traversal above `p1` occurs only when `p1` was still current and selected.

Alternative considered: mark every execution reached by an edge while merely preserving rebound pointers. Rejected because a rebound caller has already ceased to represent the active cached computation and invalidating its historical caller chain is unnecessary.

### Allow cacheless explicit roots in invalidation results

Explicit roots may have `cache_key = null`, so invalidation response records change `cache_key` from `str` to `str | None`. Propagated cacheless callers are ineligible because they have no current cache binding to invalidate.

## Risks / Trade-offs

- [Cache description is a point-in-time observation] -> Return the exact execution selected by the initial pointer snapshot and document that later calls may observe a replacement.
- [Delete-before-mark can be interrupted] -> Cache lookup rejects marked records, and a later explicit operation can mark an unmarked historical execution by ID.
- [A caller can rebind during eligibility testing] -> Hold its execution lock and require conditional deletion of the matching pointer before marking or traversing above it.
- [Breaking cache invalidation input] -> Provide `cache.describe` as the explicit cache-key-to-execution migration path and update CLI and user documentation together.

## Migration Plan

1. Add cache description and execution-ID invalidation internals.
2. Change public signatures and generated CLI behavior without compatibility aliases.
3. Update contracts and documentation in the same release.
4. Roll back atomically if needed; no persisted layout changes require data migration.
