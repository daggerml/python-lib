## 1. Remote Model Replacement

- [x] 1.1 Replace cache-key call-edge writes with canonical edge objects at `exec/edges/<callee_eid>/<caller_eid>.json`
- [x] 1.2 Replace the split record/live model with a single mutable execution object at `exec/state/<execution_id>.json`, including `created_at`
- [x] 1.3 Change cache refs so `refs/cache/<cache_key>.json` remains a proper ref while recording the current execution id for that cache key
- [x] 1.4 Add immutable invalidation tombstones at `exec/invalidate/<execution_id>.json` and include requester metadata

## 2. Runtime Execution Updates

- [x] 2.1 Update `start_fn` and related runtime paths to preserve execution-id identity across first launch and resume
- [x] 2.2 Implement compare-and-swap updates for `exec/state/<execution_id>.json` with monotone merges of `status`, `dependencies`, and `cancel_requested_by` while preserving `created_at` and the first-written adapter `state`
- [x] 2.3 Write edge objects when dependencies are concretely discovered during execution, including late discovery on later poll cycles
- [x] 2.4 Remove all reads and writes for the prior cache-key `calls/from/...` and `calls/to/...` lineage layout

## 3. Local Planning And Admin Operations

- [x] 3.1 Build local ingestion of `exec/state`, `exec/edges`, `exec/invalidate`, and `refs/cache` into a queryable local database
- [x] 3.2 Implement cache invalidation planning with the `seen`/`unseen` traversal, current-cache guard, create-once invalidation tombstones, and compare-and-swap cache-ref deletes
- [x] 3.3 Implement cancellation planning with forward dependency traversal, terminal-state pruning, reverse commit order, and uncancelled-caller counting before setting `cancel-requested`
- [x] 3.4 Update CLI or admin entry points to use the new local planning flow for invalidation and cancellation

## 4. Verification

- [x] 4.1 Add or update runtime tests that cover first-call state creation, resume reusing the stored adapter state, late dependency discovery, `created_at` preservation, cancellation requester recording, and canonical edge-object writes
- [x] 4.2 Add or update admin-planning tests for cache invalidation closure, current-cache guarding, CAS-protected cache-ref deletion, and cancellation propagation with shared, sole, and terminal dependencies
- [x] 4.3 Add tests that prove the new execution-id flows work without consulting `calls/from/...` or `calls/to/...`, for example by exercising the new layout while those old paths are absent or stale
