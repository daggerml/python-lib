# Task 04 - Switch publishers to the new flow

## Goal

Switch production call paths to the new per-DAG manifest writer flow while temporary old helpers still exist in the codebase.

## Current code anchors

- `RemoteOps.push(...)` is the existing tag publication path: `src/daggerml/_internal/ops/remote.py:798`.
- `RemoteOps.put_cache_ref(...)` is the existing cache ref writer and currently does not accept `targets`: `src/daggerml/_internal/ops/remote.py:699`.
- The current cache publication call path still uses `put_local_manifest(...)`: `src/daggerml/_internal/ops/cache.py:70`, `src/daggerml/_internal/ops/cache.py:71`.
- The current index/adapter envelope still uses `put_local_manifest(...)` for `argv_ptr`: `src/daggerml/_internal/ops/index.py:589`.
- Planning constraint: these code references identify the current callers we are intentionally talking about. Do not edit `src/daggerml/_internal/ops/cache.py`, `src/daggerml/_internal/ops/index.py`, or `src/daggerml/_internal/ops/remote.py` in this docs-only planning pass; only edit files under `docs/tasks/`.

## Implement

- Update `RemoteOps.push(ref: Ref) -> str` to publish through `put_ref_manifest(...)` semantics.
- Update cache publication paths to publish through `put_ref_manifest(...)` and write `targets`.
- Update the currently known call sites that publish local manifests:
  - `src/daggerml/_internal/ops/cache.py`
  - `src/daggerml/_internal/ops/index.py`
  - push-related code in `src/daggerml/_internal/ops/remote.py`
- Ensure tag/cache refs include top-level `targets={"dag": [...]}` derived from the authoritative manifest.
- Fail publish if the computed `targets["dag"]` does not exactly match the manifest closure DAG ids.

## Inputs and outputs

- `RemoteOps.push(ref: Ref) -> str`
  - input: head ref
  - output: published tag ref path string
  - errors:
    - raise `ValueError` if `ref.ns() != "head"`
    - raise `RefAlreadyExists` if the destination tag ref already exists
    - propagate `DmlRepoError`, `RemoteError`, `InvalidRef`, `InvalidManifest`, `MissingCasObject`, and `ShaMismatch` from the manifest publication path
- `RemoteOps.put_cache_ref(cache: str, cache_key: str, target: str, *, overwrite: bool = False, targets: dict[str, list[str]]) -> None`
  - input: cache namespace, cache key, manifest OID target, overwrite flag, required `targets`
  - `targets` must be exactly `{"dag": sorted_unique_dag_ids}` where each DAG id matches `^[0-9a-f]{64}$`
  - output: none
  - errors:
    - raise `InvalidOid` if `target` is not 64-char lowercase hex
    - raise `ValueError` if `targets` is not exactly `{"dag": sorted_unique_dag_ids}` using 64-char lowercase hex DAG ids
    - raise `RefAlreadyExists` if the existing ref target differs and `overwrite` is false
    - raise `RemoteError` on remote write/read failure

## IO

- Reads local repo state needed to dump the root closure.
- Reads and writes remote DAG refs.
- Writes remote manifest CAS objects.
- Writes remote tag refs under `refs/tags/...`.
- Writes remote cache refs under `refs/cache/...`.
- This task changes the `put_cache_ref(...)` signature by adding a required keyword-only `targets` argument.

## Expected behavior to test

- `push(...)`:
  - ensures referenced DAG refs before writing the tag ref
  - writes a tag ref with `target=<top_manifest_oid>`
  - writes `targets={"dag": sorted_unique_dag_ids}`
  - fails if `targets` would not match the manifest DAG closure
  - does not trust caller-provided `targets`; it computes authoritative `targets` from the canonical manifest bytes it is publishing
- Cache publication writes `targets` and points to the top-level manifest OID.
- Existing callers migrated in this task no longer use `put_local_manifest(...)` or `put_ptr(...)`.
- Publish order is testable: child DAG manifests and refs are published before the parent tag/cache ref.
- `put_cache_ref(...)` revalidates the supplied `targets` rather than trusting the caller.

## Done when

- New writes are live in normal code paths.
- Old helpers are now unused but still present for a short cleanup window.
