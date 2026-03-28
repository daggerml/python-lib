# Task 03 - Add new reader paths

## Goal

Teach remote readers to materialize manifests that reference DAG ids through `refs/dags/...`, while old publication helpers still exist.

## Current code anchors

- Current pointer loading entrypoints are `RemoteOps.load_ptr(...)` and `RemoteOps.load_ptr_in_txn(...)`: `src/daggerml/_internal/ops/remote.py:640`, `src/daggerml/_internal/ops/remote.py:646`.
- Current pull behavior is strict for the top-level manifest and currently requires a `commit` root: `src/daggerml/_internal/ops/remote.py:852`, `src/daggerml/_internal/ops/remote.py:880`.
- Current manifest decoding already validates closure ids as 64 lowercase hex strings: `src/daggerml/_internal/ops/remote.py:415`.
- Planning constraint: these notes align the task with the current implementation. Do not edit `src/daggerml/_internal/ops/remote.py` in this planning pass; only edit files under `docs/tasks/`.

## Implement

- Update `RemoteOps.load_ptr_in_txn(...)` to resolve `closure["dag"]` by DAG id through `refs/dags/<dag_id>.json`.
- Update `RemoteOps.load_ptr(...)` and `RemoteOps.pull(...)` to use that recursive DAG-resolution path.
- Keep behavior strict: missing or malformed refs/manifests/CAS must fail.
- Deduplicate recursive DAG loads within one top-level load so the same DAG manifest is not fetched and loaded repeatedly.
- Transitional coexistence requirement: because Task 04 has not switched writers yet, readers in this task MUST preserve compatibility with old inline-writer output. If `closure["dag"]` contains a DAG id and `refs/dags/<dag_id>.json` is absent, readers MUST fall back to the pre-existing behavior of treating that DAG id as a direct CAS object id for the raw DAG object. If neither the DAG ref nor the raw DAG CAS object exists, load must fail.
- Do not remove the old helper names yet.

## Inputs and outputs

- `RemoteOps.load_ptr_in_txn(manifest_oid: str, txn, *, expected_root_ns: str | None = None) -> Ref`
  - input: top-level manifest OID, writable transaction, optional expected root namespace
  - output: materialized root `Ref`
  - errors:
    - raise `InvalidOid` if `manifest_oid` is not 64-char lowercase hex
    - raise `InvalidManifest` on malformed manifest bytes
    - raise `InvalidRef` on malformed DAG ref bytes
    - raise `MissingCasObject` if any referenced manifest or raw CAS object is missing
    - raise `ShaMismatch` if any fetched CAS bytes hash to a different OID
    - raise `ValueError` on root namespace mismatch
- `RemoteOps.pull(ref_path: str) -> None`
  - input: tag/cache ref path string
  - output: none
  - side effect: materializes the manifest closure locally and updates the local remote-tracking head
  - errors:
    - raise `RemoteError` if the tag/cache ref does not exist
    - propagate `InvalidRef`, `InvalidManifest`, `MissingCasObject`, `ShaMismatch`, and `ValueError` from strict load behavior

## IO

- Reads remote tag/cache refs.
- Reads remote manifest CAS objects.
- Reads remote DAG refs under `refs/dags/...`.
- Reads remote raw CAS objects for all non-DAG closure entries.
- During the temporary coexistence window, may also read raw CAS objects for `closure["dag"]` entries when the DAG ref is absent.
- Writes local DB objects and local remote-tracking head state.
- Within one load, already-materialized local objects may be skipped, but missing referenced DAG refs/manifests/CAS must still fail.

## Expected behavior to test

- `load_ptr_in_txn(...)` loads a top manifest whose `closure["dag"]` contains DAG ids by:
  - resolving each DAG id through `refs/dags/<dag_id>.json`
  - loading each referenced DAG manifest recursively
  - materializing all non-DAG objects into the local transaction
  - deduplicating repeated DAG ids within one traversal
- During the coexistence window, if a DAG ref is missing but the old raw DAG CAS object exists, readers load that DAG object directly instead of failing.
- `pull(...)` succeeds when all DAG refs and CAS objects exist.
- `pull(...)` fails if:
  - a DAG ref is missing and the old raw DAG CAS object is also missing
  - a DAG ref is malformed
  - a DAG manifest CAS object is missing
  - any raw CAS object is missing
  - any CAS object hash mismatches its OID
- Existing top-level non-DAG closure entries still materialize correctly.
- `load_ptr(...)` and `pull(...)` must both enforce the same strict DAG-ref resolution rules.

## Done when

- Readers understand the new per-DAG remote shape.
- The old writer path can still coexist temporarily.
