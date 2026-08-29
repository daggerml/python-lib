## Why

Execution state is currently spread across lock files, executor-specific state prefixes, executor-private storage layouts, and local scratch paths, which makes execution ownership and stale-state recovery difficult to reason about. Nested execution also needs durable caller/callee lineage so the runtime can answer which user-dags and fn-dags invoked a given cache-keyed function.

## What Changes

- Replace the current mutable, executor-owned execution-state model with a runtime-owned model built around:
  - a lock per `cache_key`,
  - a plain-text active pointer from `cache_key` to `execution_id`,
  - an immutable execution record created on the first adapter call for an execution.
- Extend the adapter envelope to include `execution_id` and the initial persisted execution `state`.
- Simplify adapter result states by removing `pending`; adapters return `running`, `succeeded`, or `failed`.
- Require that adapters return all durable resume state on the first launch call for an execution; later polls reuse the immutable stored state and ignore newly returned state.
- Treat failed executions as terminal cached results by completing the DAG with the error and publishing that failed outcome to cache.
- Add S3-backed call-edge indexes for both directions:
  - user-dag (`index_id`) to callee `cache_key`,
  - fn-dag caller `cache_key` to callee `cache_key`,
  - reverse lookup from callee `cache_key` to both index and fn-dag callers.
- Update stale-lock handling so recovery keeps the active `execution_id` when resumable execution state still exists.

## Capabilities

### New Capabilities
- `runtime-execution-records`: Runtime-owned execution identity, immutable execution records, active execution pointers, and adapter envelope/result semantics for resumable execution.
- `execution-call-edges`: Queryable caller/callee lineage indexes between user-dags, fn-dags, and callee cache keys.

### Modified Capabilities

## Impact

- Affected code will include execution flow in `src/daggerml/_internal/ops/index.py`, execution-state helpers, adapter payload validation, and contrib executor/adapters that currently own resumable state.
- S3 layout will gain runtime-owned `active/`, `exec/`, and `calls/` objects, alongside updated lock handling.
- Adapter/executor contracts and docs will change to reflect immutable execution records, `execution_id` propagation, removal of `pending`, and failed-result cache publication.
- Tests will need to cover stale-lock recovery, immutable execution records, active pointer lifecycle, adapter payload/result validation, and bidirectional call-edge updates.
