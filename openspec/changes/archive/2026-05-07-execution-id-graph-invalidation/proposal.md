## Why

The current remote execution lineage is keyed by cache key and maintained through multiple mutable forward and reverse indexes. That makes cache invalidation and cancellation propagation difficult to reason about, especially when executions are retried and cache keys are reused across attempts.

## What Changes

- This change explicitly rejects any backward compatibility with the existing cache-key lineage model. It is a clean, new implementation and SHALL replace the prior remote layout wholesale.
- Move remote execution lineage from cache-key-based call indexes to execution-id-based edge records.
- Change remote cache refs so each cache key remains a proper ref while recording the current execution id for that computation in ref metadata.
- Use a single mutable execution object under `exec/state/` to hold adapter state, lifecycle status, execution timestamps, and discovered dependencies.
- Add remote invalidation markers keyed by execution id and define a cache invalidation algorithm that computes caller closure locally, writes invalidation markers, and removes cache refs that point at invalidated executions.
- Drive cancellation through mutable execution `state/` and define a cancellation algorithm that propagates through execution dependencies when a downstream execution has no remaining live callers.
- Nothing in the new implementation reads from `calls/from/...` or `calls/to/...`; those paths are fully unsupported.
- **BREAKING** Replace the existing cache-key call-edge S3 layout (`calls/from/...`, `calls/to/...`) with execution-id-based storage under `exec/edges/`, `exec/state/`, and `exec/invalidate/`.

## Capabilities

### New Capabilities
- `execution-admin-controls`: Manual invalidation and cancellation controls over the execution graph, including closure-based propagation rules and required S3 markers.

### Modified Capabilities
- `execution-call-edges`: Change lineage persistence from cache-key forward/reverse indexes to execution-id edge records stored by callee execution.
- `runtime-execution-records`: Replace the split record/live model with a single execution object, change cache publication semantics so cache refs record current execution ids while remaining proper refs, and define execution state used by graph planning.

## Impact

- Affected code: runtime execution coordination, cache publication/deletion, remote storage layout, CLI/admin flows for cache invalidation and cancellation, and local graph-planning tooling.
- Affected systems: S3-backed execution metadata and any consumers of call-edge lineage.
- Affected APIs/data: remote object layout under `exec/` and `refs/cache/`; prior cache-key call-edge objects are not preserved or supported.
