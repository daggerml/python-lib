# Task 05 - Switch GC to DAG-ref reachability

## Goal

Update remote GC to follow DAG reachability through tag/cache refs and `refs/dags/...`, while cleanup-only old helpers still remain.

## Current code anchors

- Current GC entrypoints are `RemoteOps._gc_mark()` and `RemoteOps.gc(...)`: `src/daggerml/_internal/ops/remote.py:1010`, `src/daggerml/_internal/ops/remote.py:1112`.
- Current mark behavior walks only tag/cache refs and directly unions manifest closure OIDs: `src/daggerml/_internal/ops/remote.py:1022`, `src/daggerml/_internal/ops/remote.py:1037`.
- Current GC summary shape is `{"deleted": ..., "kept_live": ..., "kept_young": ...}` from `_gc_sweep(...)`: `src/daggerml/_internal/ops/remote.py:1043`, `src/daggerml/_internal/ops/remote.py:1105`.
- Planning constraint: the task extends those existing semantics but this planning pass must not edit `src/daggerml/_internal/ops/remote.py`; only edit files under `docs/tasks/`.

## Implement

- Update `RemoteOps._gc_mark() -> set[str]` to treat only tag and cache refs as GC roots.
- Allow GC to use `ref.targets["dag"]` as a fast-path for discovering DAG ids.
- Require GC to decode each visited manifest to collect non-`dag` closure OIDs.
- When a manifest references a DAG id, resolve it through `refs/dags/<dag_id>.json` and add the child manifest OID to the mark worklist.
- Add a malformed-object policy flag to the public GC entrypoint:
  - `RemoteOps.gc(min_age_seconds: int = 24 * 3600, *, malformed: Literal["raise", "warn", "ignore"] = "warn") -> dict[str, int]`
- If GC encounters malformed refs/manifests/CAS objects, behavior depends on `malformed`:
  - `raise`: fail immediately with a clear error naming the bad object and the reason
  - `warn` (default): warn clearly, delete the malformed object if present, continue
  - `ignore`: delete the malformed object if present, continue silently
- Missing `refs/dags/<dag_id>.json` remains a skip case, not a malformed-object error.

## Inputs and outputs

- `RemoteOps._gc_mark() -> set[str]`
  - input: none
  - output: live OID set including reachable manifests and reachable raw CAS objects
- `RemoteOps.gc(min_age_seconds: int = 24 * 3600) -> dict[str, int]`
  - input: minimum age in seconds
- `RemoteOps.gc(min_age_seconds: int = 24 * 3600, *, malformed: Literal["raise", "warn", "ignore"] = "warn") -> dict[str, int]`
  - input: minimum age in seconds, malformed-object handling mode
  - output: summary counts dictionary with the existing shape `{"deleted": int, "kept_live": int, "kept_young": int}`

## IO

- Reads remote tag refs and cache refs.
- Reads remote DAG refs under `refs/dags/...`.
- Reads reachable manifest CAS objects.
- Reads the CAS object listing under `cas/sha256/...`.
- Deletes broken refs/CAS objects encountered during GC, plus unreferenced old CAS objects during sweep.
- Broken-object deletion rules for mark:
  - if a tag/cache ref exists but is malformed, apply the `malformed` policy to that ref object and do not traverse it
  - if a tag/cache ref points to a malformed manifest CAS object, apply the `malformed` policy to the manifest CAS object; the ref itself may also be deleted in `warn`/`ignore` mode if desired by implementation, but the error/warning message must name the malformed manifest object
  - if a DAG ref exists but is malformed, apply the `malformed` policy to that DAG ref object and do not traverse it
  - if a DAG ref points to a malformed manifest CAS object, apply the `malformed` policy to the manifest CAS object; the DAG ref itself may also be deleted in `warn`/`ignore` mode if desired by implementation, but the error/warning message must name the malformed manifest object
  - if a manifest CAS object is present but malformed, apply the `malformed` policy to that manifest CAS object and stop traversing it
  - if a referenced raw CAS object is missing, nothing can be deleted for the absent object; traversal treats that reference as broken and continues
- This task does not add new GC summary counters; any broken-object deletions count toward `deleted`.

## Expected behavior to test

- GC marks all non-DAG closure OIDs reachable from tag/cache roots.
- GC follows DAG ids only through `refs/dags/...`.
- `refs/dags/...` alone do not keep DAG manifests alive.
- If `ref.targets` lists a DAG id and the DAG ref is missing, GC skips that DAG id.
- If a manifest decode disagrees with `ref.targets`, GC treats the manifest as authoritative.
- Malformed refs/manifests/CAS encountered during GC follow the selected `malformed` policy.
- GC still decodes each visited manifest even when `ref.targets` is present, because `targets` is only a DAG-discovery hint.

## Done when

- GC reachability matches the per-DAG manifest design.
- Broken-remote cleanup is explicit in the GC path.
