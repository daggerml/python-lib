## Why

The current database boundary leaks LMDB environment and transaction lifecycle details into the ops layer, which makes DB access patterns hard to reason about and unsafe under concurrent shared-env usage. We need a single transactional boundary that keeps raw DB concerns inside `_db.pyx` and moves typed validation into one higher-level facade.

## What Changes

- **BREAKING** Replace the `_db.pyx` `DmlDbEnv` and `DmlDbEnvTxn` API with a single `dmldb(...)` context manager that opens an env and transaction together and yields one raw transaction object.
- **BREAKING** Introduce `daggerml._internal.types.DmlDB` as the only typed DB facade used by application code.
- **BREAKING** Remove `BaseOps` and convert ops methods to accept `db: DmlDB` explicitly instead of receiving a DB handle at construction time.
- **BREAKING** Move type validation from stored model classes into `DmlDB.put`, including graph-shape validation previously distributed across type `_validate` methods.
- **BREAKING** Remove environment reopen logic; PID changes invalidate the active env/transaction immediately.
- **BREAKING** Remove `RunnableDatum` and store/validate runnable values through the unified typed DB facade.
- Add a reusable `DmlDB.run_with_resize(...)` helper for write paths that need map-full retry and resize behavior.
- Add typed DB helpers such as `require` and `get_ctx` so higher layers stop reaching into raw DB details.

## Capabilities

### New Capabilities
- `typed-db-boundary`: Defines the new raw and typed database boundaries, transaction lifecycle, validation ownership, and resize retry behavior.

### Modified Capabilities
- `headops-pointer-management`: Head/index pointer operations will no longer depend on constructor-injected DB state and must work with explicit project/db inputs.
- `git-like-commit-ops`: Commit operations will no longer own hidden DB state and must execute against an explicit typed DB context.

## Impact

- Affected code: `src/daggerml/_internal/_db.pyx`, `src/daggerml/_internal/types.py`, `src/daggerml/_internal/dml.py`, all files under `src/daggerml/_internal/ops/`.
- Affected APIs: all internal DB-facing APIs, ops class instantiation patterns, and transaction helper utilities.
- Affected behavior: DB validation moves to write time in `DmlDB.put`; fork handling becomes fail-fast; map resize retry moves to `DmlDB.run_with_resize`.
- Affected tests: direct `_db` tests, ops tests, and concurrency-sensitive runtime tests.
