---
status: specified
doc_type: overview
---

# Contrib Execution Graph

## Authority

This document is an orientation guide for how contrib execution state fits together at a high level.

It is not the authoritative spec for runtime state shape, backend storage, or lifecycle transitions.

- `docs/contrib/runtime-contract.md` is authoritative for adapter and executor lifecycle behavior.
- `docs/contrib/executor-state.md` is authoritative for the `ExecutionState` record shape, API, and state transitions.

## Purpose

Explain the current execution-state redesign without restating lower-level contracts.

## Overview

Built-in contrib runtimes no longer maintain a separate SQLite execution-graph model or a DynamoDB-backed state machine.

Instead, live execution coordination uses runtime-owned S3 objects around each `cache_key`:

- `fn-exec/locks/<cache_key>.json` is the advisory mutex.
- `fn-exec/active/<cache_key>` is the current in-flight execution-number pointer.
- `fn-exec/records/<cache_key>/<execution_number>.json` is the immutable launch-time execution record created on the first `running` result.
- `IndexOps.start_fn` checks the cache, acquires the mutex, rechecks the cache, resumes or launches the active execution, and releases the mutex.
- The adapter returns `{status, state?, dag_id?, error?}` where only `running|succeeded|failed` are valid statuses.
- Failed executions are materialized into failed DAGs and published to cache just like successful executions.

The execution identity is split:

- `cache_key` identifies the computation and cache entry.
- `execution_number` is the monotonically increasing attempt number for that cache key, starting at `0`.
- `execution_id` is the adapter-facing execution identifier derived as `<cache_key>-<execution_number>`.

Call lineage is also stored in S3:

- `fn-exec/calls/from/index/<index_id>.json`
- `fn-exec/calls/from/cache/<caller_ck>.json`
- `fn-exec/calls/to/cache/<callee_ck>.json`

This allows reverse and forward queries across user-dags and fn-dags.

## Required Environment Variables

- `DML_REMOTE_ROOT` — S3 URI prefix used for all remote operations, including lock, active execution, immutable execution records, and call-edge lineage.

`DML_DYNAMODB_TABLE` is no longer used or required.

## Boundaries

Use this document to understand the runtime at a glance.

Do not use it as the source of truth for:

- status values,
- lock semantics,
- metadata ownership,
- heartbeat rules,
- adapter or executor method contracts,
- backend serialization details.

For those details, follow the authoritative docs linked above.

## References

- [runtime-contract.md](runtime-contract.md)
- [executor-state.md](executor-state.md)
