## Why

The current DB layer exposes unused raw read/write helpers and forces callers to handle forked-process DB handles manually. That leaves dead API surface in `db.pyx` and makes process-fork behavior leak through to users and higher layers.

## What Changes

- **BREAKING** Remove raw payload read/write support from `src/daggerml/_core/db.pyx` `get()` and `put()` and remove the typed `get_raw()` and `put_raw()` helpers.
- Add automatic DB handle replacement and retry in `src/daggerml/_core/db.pyx` when handle-level C calls report fork invalidation.
- Make transaction entry, resize, size inspection, and other handle-level DB operations fork-transparent from the caller's perspective.
- Preserve fail-fast behavior for invalid inherited transaction objects; only DB handle recovery becomes automatic.
- Update tests to assert seamless child-process reuse of the same logical DB facade.

## Capabilities

### New Capabilities
- `db-handle-lifecycle`: Defines handle-level fork recovery, retry boundaries, and the supported raw DB API surface.

### Modified Capabilities

## Impact

- Affected code: `src/daggerml/_core/db.pyx`, `c/src/dml_db.c`, `src/daggerml/_core/types.py`, and DB concurrency tests.
- Affected APIs: internal raw DB transaction APIs and typed transaction helpers that currently expose raw payload access.
- Affected behavior: forked child processes should transparently reopen DB handles for handle-level operations instead of surfacing `DmlDbForkedError` to normal callers.
