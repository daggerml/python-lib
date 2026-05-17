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
- `dml/active/<cache_key>` is the current in-flight execution pointer.
- `dml/exec/launch/<execution_id>.json` is caller-owned resumable launch state.
- `dml/exec/state/<execution_id>.json` is runtime-owned lifecycle state.
- `IndexOps.start_fn` checks the cache, acquires the mutex, rechecks the cache, resumes or launches the active execution, and releases the mutex.
- The adapter returns `{status, state?, dag_id?, error?}` where `cancel-detached` is also a valid control-plane completion status.
- Failed executions are materialized into failed DAGs and published to cache just like successful executions.

The execution identity is split:

- `cache_key` identifies the computation and cache entry.
- `execution_id` is the runtime-assigned adapter-facing execution identifier for one execution attempt.

Call lineage is also stored in S3:

- `dml/exec/edges/<callee_execution_id>/<caller_execution_id>.json`

These live caller edges are caller-owned. They drive orphan detection and invalidation.

Historical cancellation traversal is stored separately in `execution_record.spawned_execution_ids`, which remains a best-effort summary even after live edges are removed.

## Required Environment Variables

- `DML_REMOTE_URI` — S3 URI prefix used for all remote operations, including lock, active execution, `launch_state`, `execution_record`, and live call-edge lineage.

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
