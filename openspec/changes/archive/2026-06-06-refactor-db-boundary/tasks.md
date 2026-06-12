## 1. Raw DB Boundary

- [ ] 1.1 Replace `DmlDbEnv` and `DmlDbEnvTxn` in `_internal/_db.pyx` with a single `dmldb(...)` context manager and one raw transaction object.
- [x] 1.2 Remove env reopen and repair logic from `_internal/_db.pyx` and keep only fail-fast PID invalidation behavior.
- [x] 1.3 Ensure the raw transaction object exposes the operations needed by the typed facade, including raw get/put and orphan listing.

## 2. Typed DB Facade

- [x] 2.1 Add `daggerml._internal.types.DmlDB` as a reusable context manager over `dmldb(...)`.
- [ ] 2.2 Move persistence validation into `DmlDB.put` and remove redundant persistence-validation ownership from stored model types.
- [x] 2.3 Add `DmlDB` helpers `require`, `get_ctx`, `get_raw`, `put_raw`, `list_orphans`, and `run_with_resize`.
- [ ] 2.4 Remove `RunnableDatum` and route runnable storage through the unified typed facade.

## 3. Ops API Rewrite

- [ ] 3.1 Remove `BaseOps`, `_tx`, `TxnContext`, and constructor-injected `_db` usage.
- [x] 3.2 Rewrite `HeadOps` to the explicit `project_home` / `db: DmlDB` signatures defined in `design.md`.
- [x] 3.3 Rewrite `DagOps` and `NodeOps` to the explicit `db: DmlDB` signatures defined in `design.md`.
- [x] 3.4 Rewrite `CommitOps` to the explicit `project_home` / `db: DmlDB` signatures defined in `design.md`.
- [x] 3.5 Rewrite `CacheOps`, `RemoteOps`, and `GcOps` to the explicit `db: DmlDB` signatures defined in `design.md`.
- [ ] 3.6 Rewrite `IndexOps` to explicit `db: DmlDB` plus only the concrete refs/values required by each operation, keeping `project_home` resolution outside `IndexOps`.
- [x] 3.7 Eliminate raw transaction escapes from ops by replacing `txn.txn.*` usage with named `DmlDB` facade methods.

## 4. Dml Orchestration

- [ ] 4.1 Rewrite `_internal/dml.py` to construct explicit `DmlDB` contexts instead of using `with_db`, `make_index_ops`, or constructor-injected DB state, and keep pointer/path orchestration outside `IndexOps`.
- [ ] 4.2 Route write workflows through `DmlDB.run_with_resize(...)` and plain read workflows through explicit readonly `DmlDB` contexts.

## 5. Verification

- [ ] 5.1 Update direct `_db` and typed DB tests to cover the new context-manager and PID-failure behavior.
- [ ] 5.2 Update ops and runtime tests for explicit `db: DmlDB` APIs and removal of `BaseOps`.
- [ ] 5.3 Add or update a concurrency-focused test that exercises concurrent runtime calls without shared hidden DB state.
