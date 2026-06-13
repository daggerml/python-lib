## Why

Concurrent `Dml` and `DmlDB` creation against the same repository path can create multiple LMDB environments in one process, which has already produced transaction-open failures in parallel init flows. We need one process-local environment registry so callers targeting the same canonical path reuse the same underlying LMDB environment lifecycle and stop racing on duplicate opens.

## What Changes

- Add a process-local C registry keyed by canonical DB path so same-path callers reuse one shared registry slot.
- Change DB lifecycle management so transactions and other short-lived env users acquire an environment lease from the registry and release it on close.
- Remove persistent per-wrapper environment ownership; DB handles become lightweight tokens for registry lookup instead of long-lived environment owners.
- Move fork/PID invalidation to the registry level so a process change clears all inherited registry state before new env acquisition.
- Change map-size growth handling so retry paths reopen a fresh environment with a larger map size instead of resizing an already-shared live environment.

## Capabilities

### New Capabilities
- `db-env-registry`: Process-local registry behavior for canonical DB path deduplication, env leasing, PID invalidation, and map-size reopen semantics.

### Modified Capabilities

## Impact

- Affected code: `c/src/dml_db.c`, `c/include/dml_db.h`, `src/daggerml/_core/db.pyx`, `src/daggerml/_core/types.py`, and storage-focused tests.
- APIs: internal DB lifecycle semantics change, but the higher-level Python `Dml` and `DmlDB` surface should remain stable.
- Systems: LMDB environment ownership, transaction open/close paths, fork handling, and map-full retry behavior.
