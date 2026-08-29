## Why

Large DAG mutations can exhaust an LMDB map even when the database can safely grow. Current recovery is a one-shot retry that relies on reopening an idle environment; concurrent transactions can keep the old environment alive, causing the requested map size to be ignored and the retry to fail. Every normal local mutation should complete without surfacing map-full while configured capacity remains available.

## What Changes

- Add an internal `write_with_growth(fn)` path for replayable local write functions that retries until the write commits or the configured maximum map size is reached.
- Add an explicit, blocking native environment-resize operation that prevents new transactions for one database path while active leases drain, then reopens the environment at the requested larger size.
- Keep `map_size` as an initial-environment-open parameter; it remains ignored when an environment is already open so ordinary mutation transactions do not serialize behind resize behavior.
- Route local, replayable graph and remote-materialization writes through the growth-aware write path, keeping external side effects outside retried functions.
- Surface terminal capacity failures with database path and map-size context.
- Preserve the existing 100,000-item collection limit and DAG object model.

## Capabilities

### New Capabilities
- `db-write-with-growth`: Replayable local write functions automatically grow the LMDB map until commit or a terminal capacity limit.

### Modified Capabilities
- `db-env-registry`: Environment leases coordinate explicit blocking resize requests and adopt map growth performed by another process.

## Impact

- Affects `src/daggerml/_core/db.pyx`, `src/daggerml/_core/db.pyi`, `src/daggerml/_core/types.py`, and `c/src/dml_db.c` plus its public header.
- Affects core mutation callers in index, commit, remote materialization, runtime, and garbage collection paths.
- Adds native synchronization and retry-focused core contract and integration tests.
- Does not change public DAG semantics, collection limits, or add dependencies.
