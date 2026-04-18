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

Built-in contrib runtimes no longer maintain a separate SQLite execution-graph model.

Instead, live execution coordination is centered on one DynamoDB-backed `ExecutionState` record per `cache_key`.

At a high level:

- an invocation creates or reuses the execution record with `ExecutionState.upsert(...)`,
- launch coordination atomically claims `pending -> running` so duplicate callers do not start duplicate work,
- executors store in-flight handles and debug data in `state["metadata"]`,
- heartbeats and bounded polling update the same record while work is active,
- terminal executor outcomes move through `succeeded` or `failed`,
- `IndexOps.start_fn` publishes results and writes the final `done` tombstone.

The execution identity is `cache_key`. There is no separate live graph schema, no `parent_id`-based graph contract, and no `canceled` lifecycle state in the built-in runtime model.

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
