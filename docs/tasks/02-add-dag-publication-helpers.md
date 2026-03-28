# Task 02 - Add DAG publication helpers

## Goal

Add the new manifest-building and DAG publication helpers alongside the old pointer helpers, but do not switch existing callers yet.

## Current code anchors

- The existing pointer helpers that remain in place during this task are `RemoteOps.put_ptr(...)` and `RemoteOps.put_local_manifest(...)`: `src/daggerml/_internal/ops/remote.py:618`, `src/daggerml/_internal/ops/remote.py:625`.
- Local manifest roots still come from `ref.ns()` and `ref.id()`: `src/daggerml/_internal/ops/base_ops.py:326`.
- Current remote manifest construction already uses canonical JSON and manifest OIDs derived from SHA256: `src/daggerml/_internal/ops/remote.py:570`, `src/daggerml/_internal/ops/remote.py:613`, `src/daggerml/_internal/ops/remote.py:635`.
- Current ref creation already uses `RefAlreadyExists` for create-only semantics: `src/daggerml/_internal/ops/remote.py:343`.
- Planning constraint: this task doc is aligned to the existing code shape above. Do not edit those code files as part of the docs-only planning pass; only edit files under `docs/tasks/`.

## Implement

- Add `RemoteOps._ensure_dag_ref(dag_ref: Ref) -> bool`.
- Add `RemoteOps.put_ref_manifest(root_ref: Ref) -> str`.
- Reuse existing local snapshotting and CAS upload helpers where possible.
- Detect DAG cycles during recursive DAG publication.
- Keep the public helper signature simple: no optional `txn` or `stack` parameters on `_ensure_dag_ref(...)`.
- If recursive state is needed internally, keep it in private nested helpers or local implementation state.
- Keep `put_ptr(...)` and `put_local_manifest(...)` available in this task.

## Inputs and outputs

- `RemoteOps._ensure_dag_ref(dag_ref: Ref) -> bool`
  - input: `Ref` whose namespace is exactly `dag`
  - output: `True` when `refs/dags/<dag_id>.json` already exists or is created successfully
  - errors:
    - raise `ValueError` if `dag_ref.ns() != "dag"`
    - raise `ValueError` if the local manifest root namespace for `dag_ref` is not `dag`
    - raise `DmlRepoError` on cycle detection
    - raise `RemoteError`, `InvalidRef`, `InvalidManifest`, `MissingCasObject`, or `ShaMismatch` on remote integrity/read failures that occur during publish-on-miss
- `RemoteOps.put_ref_manifest(root_ref: Ref) -> str`
  - input: root `Ref`
  - output: top-level manifest OID as a 64-char lowercase hex string
  - errors:
    - raise `ValueError` on invalid local manifest shape
    - raise `DmlRepoError` on DAG cycle detection reached through `_ensure_dag_ref(...)`
    - raise `RemoteError`, `InvalidRef`, `InvalidManifest`, `MissingCasObject`, or `ShaMismatch` on remote integrity/read failures

## IO

- `_ensure_dag_ref(...)`
  - reads: local readonly transaction for `dag_ref`; remote `refs/dags/<dag_id>.json`; remote CAS presence
  - writes on miss: raw CAS objects for the DAG closure, DAG manifest CAS object, `refs/dags/<dag_id>.json`
  - write order on miss:
    1. upload any missing raw CAS objects for the DAG closure
    2. upload DAG manifest CAS bytes for the computed manifest OID
    3. create `refs/dags/<dag_id>.json`
- `put_ref_manifest(...)`
  - reads: local readonly transaction for `root_ref`; remote `refs/dags/<dag_id>.json`; remote CAS presence
  - writes: missing raw CAS objects for the full root closure; top-level manifest CAS object
  - does not write tag/cache refs in this task

## Expected behavior to test

- `_ensure_dag_ref(...)` fast-path:
  - if `refs/dags/<dag_id>.json` exists, return `True`
  - do not upload CAS or rebuild the manifest on that fast path
- `_ensure_dag_ref(...)` miss path:
  - builds a DAG manifest with `root-ns == "dag"`
  - recursively ensures child DAG refs first
  - uploads raw CAS before writing the DAG ref
  - writes `meta={"dag": {"id": dag_id}}`
- `_ensure_dag_ref(...)` handles `RefAlreadyExists` on DAG ref creation by reading back the ref and returning `True`.
- `_ensure_dag_ref(...)` detects DAG cycles and raises `DmlRepoError`.
- `_ensure_dag_ref(...)` treats an existing DAG ref as success without checking whether its target CAS exists.
- `put_ref_manifest(...)`:
  - computes canonical manifest bytes
  - ensures all closure DAG ids are published through `_ensure_dag_ref(...)`
  - uploads the top-level manifest CAS
  - returns the computed manifest OID
  - is deterministic for identical local input
  - does not write any tag ref or cache ref in this task

## Done when

- New per-DAG publication logic exists behind new helper methods.
- Old pointer helpers still exist so the repo remains incrementally testable.
