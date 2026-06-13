## Context

Today each `DmlDB` wrapper can create its own raw LMDB-backed handle for the same on-disk DB path. Concurrent initialization has shown that duplicate environments in one process are unsafe even when repository bootstrap is serialized by the filesystem lock. The change needs to move deduplication into the C storage layer so same-path callers share one process-local registry slot and env lifecycle without adding Python-only ownership rules.

## Goals / Non-Goals

**Goals:**
- Deduplicate DB environment ownership per canonical path within one process.
- Move env lifecycle control into C so Python wrappers only hold lightweight tokens.
- Lease envs for transactions and other short-lived operations, then release them on close.
- Clear inherited registry state on PID mismatch before new env acquisition.
- Replace live-env resize behavior with reopen-at-larger-map-size behavior.

**Non-Goals:**
- Changing the public `Dml` or `DmlDB` API shape.
- Supporting more than a small fixed number of distinct open DB paths per process in the first version.
- Preserving persistent env ownership across idle periods.
- Allowing resize while transactions are active.

## Decisions

### Use a fixed-size process-local registry in C
Use a small fixed-size registry array keyed by canonical path, guarded by one process-local mutex. Each slot stores the normalized path, namespace/config metadata, the current env/dbi pointers, and an active refcount.

Why this over a dynamic hash table:
- smaller implementation surface
- easier lifetime management
- enough for the current workload if the limit is explicit

Trade-off:
- the process can address only a bounded number of distinct DB paths at a time

### Make DB handles lightweight registry tokens
`dml_db_open` will canonicalize the path, find or allocate a registry slot, and return a lightweight handle that identifies that slot. The handle will not own a live env.

Why this over persistent env-owning handles:
- removes duplicate-env ownership from Python wrappers
- lets the C layer centralize refcounting and fork invalidation

### Lease envs per transaction or short-lived operation
Transaction open will acquire the slot env, opening it if needed, increment the slot refcount, and begin the LMDB transaction. Transaction close will commit or abort, then decrement the refcount and close the env when the count reaches zero.

Why this over persistent envs:
- simpler ownership model
- no idle live envs
- no need for explicit wrapper finalization to manage env lifetime

### Keep PID on the registry, not per entry
Before any slot lookup or env acquisition, registry access will compare the stored PID with `getpid()`. On mismatch it will clear all slots, reset the PID, and continue in the new process.

Why this over per-entry fork checks:
- centralizes inherited-state invalidation
- matches the new registry-owned lifetime model

### Remove live resize and reopen with a larger map size instead
Map-full retry paths will stop using in-place resize on a shared live env. Instead they will close the failed transaction, release the env lease, reacquire the slot with a larger map size, and retry.

Why this over serializing resize on a live shared env:
- avoids resize races with active transactions
- aligns map-size changes with env creation time
- reduces shared mutable env state

## Risks / Trade-offs

- [Registry slot exhaustion] → Return a dedicated registry-full error and cover it with tests.
- [Path canonicalization bugs causing duplicate slots] → Canonicalize in C and use one normalized path form for lookup and storage.
- [Retry logic around map-full becomes more complex] → Keep the reopen path explicit and test map-full retry behavior directly.
- [Forked child accidentally using inherited tokens] → Clear the registry on PID mismatch before any new env acquisition.
- [Thread races on env open/close] → Guard all slot lookup, env acquire, env release, and reopen paths with the registry mutex.

## Migration Plan

- Update the C DB layer so handles become registry tokens and env lifetime moves to slot acquire/release paths.
- Update the Cython wrapper to match the new C semantics.
- Replace resize-based retry logic with reopen-at-larger-map-size retry logic.
- Add tests for concurrent same-path opens, transaction lease release, PID reset behavior, map-full retries, and registry-full failures.

## Open Questions

- What exact fixed registry size should ship first: 10 or another small constant?
- Should incompatible config for an existing slot hard-fail immediately, or can some size fields widen when no env is active?
- Should non-transaction helpers besides reopen-for-map-full share the same env lease hooks directly, or should they always route through the transaction APIs?
