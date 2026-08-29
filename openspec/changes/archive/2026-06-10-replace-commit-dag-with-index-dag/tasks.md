## 1. Data Model And Context Loading

- [x] 1.1 Remove `dag` from `Commit`, add `dag` to `Index`, and update validation/serialization helpers in `src/daggerml/_core/types.py`.
- [x] 1.2 Update commit/index context loading so index refs resolve the working DAG from `Index.dag` while commit refs no longer synthesize a commit-owned current DAG.

## 2. Index Mutation And Finalization

- [x] 2.1 Rewrite `IndexOps.create()` and every mutable index update path in `src/daggerml/_core/index.py` to persist the current DAG on the index instead of on the head commit.
- [x] 2.2 Update import, builtin, adapter, and final commit flows to read the current DAG from the index, return `(dag_ref, commit_ref | None)` from `IndexOps.commit()`, publish named DAGs through `Tree.dags` only when `name is not None`, and use the finalized DAG for execution completion side effects.

## 3. Commit And Inspection Payload Cleanup

- [x] 3.1 Remove `dag` from `CommitOps` commit descriptions and any related commit/log/show payload assembly in `src/daggerml/_core/commit.py`.
- [x] 3.2 Update runtime/index inspection in `src/daggerml/_core/dml.py` so open-index descriptions still expose the current DAG through `Index.dag`, `runtime.commit(...)` returns the DAG ref, and `HEAD` only advances when `commit_ref is not None`.

## 4. Tests And Verification

- [x] 4.1 Update `_core` contract and integration tests that currently assert `Commit.dag`, including remote roundtrip coverage and any commit/log/show expectations.
- [x] 4.2 Add or adjust targeted tests for the new ownership boundary so index mutation uses `Index.dag`, unnamed finalization leaves the tree and `HEAD` unchanged, named finalization creates a commit, and commit-facing payloads omit `dag`.
- [x] 4.3 Run the relevant targeted tests plus the required finish checks and fix any remaining hidden dependency on `Commit.dag`.
