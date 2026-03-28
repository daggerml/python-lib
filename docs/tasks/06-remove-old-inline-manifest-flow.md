# Task 06 - Remove old inline manifest flow

## Goal

Remove the superseded inline sub-DAG manifest path once all readers and writers use the per-DAG manifest design.

## Current code anchors

- The old pointer-style helpers still present today are `RemoteOps.put_ptr(...)` and `RemoteOps.put_local_manifest(...)`: `src/daggerml/_internal/ops/remote.py:618`, `src/daggerml/_internal/ops/remote.py:625`.
- Current cache and index call sites still depend on that old writer flow: `src/daggerml/_internal/ops/cache.py:70`, `src/daggerml/_internal/ops/index.py:589`.
- Planning constraint: these are references for scope only. Do not edit those code files during this task-doc pass; only edit files under `docs/tasks/`.

## Implement

- Remove obsolete helper methods and code paths that only support the old inline-sub-DAG manifest flow.
- Remove call-site fallbacks kept only for incremental rollout.
- Remove the temporary reader compatibility fallback added in Task 03 that treats a missing `refs/dags/<dag_id>.json` entry as permission to load the DAG directly from raw CAS. After this task, readers must require DAG refs for published per-DAG manifests.
- Tighten validation so tag/cache refs that point at manifests require `targets`.
- Remove tests that only validate the old flow.
- This task intentionally removes backward compatibility for old remote manifest layouts.

## Inputs and outputs

- Deleted APIs and code paths include, at minimum:
  - `RemoteOps.put_ptr(...)`
  - `RemoteOps.put_local_manifest(...)`
  - any code path that assumes published sub-DAGs are represented inline in parent manifests rather than by DAG id through `refs/dags/...`
  - any caller fallback in `cache.py`, `index.py`, or `remote.py` that still uses the old inline-manifest writer path
  - the Task 03 transitional reader fallback in `RemoteOps.load_ptr_in_txn(...)` / `RemoteOps.pull(...)` that loads raw DAG CAS objects when DAG refs are absent
- Remaining public/internal methods keep their final signatures and semantics.

## IO

- No new IO shape.
- Final remote IO shape is:
  - tag/cache refs -> top manifest OID + `targets`
  - DAG refs -> DAG manifest OID
  - manifests -> `closure["dag"]` as DAG ids

## Expected behavior to test

- No remaining production code path writes inline published sub-DAG manifests into parent manifests.
- Tag/cache refs without `targets` are rejected at decode/load time and at publish time.
- All tests for new publish, pull, and GC behavior pass with the old code removed.
- Dead helper methods are removed and no caller references them.
- Old remotes that still rely on inline published sub-DAG manifests are no longer supported and must fail explicitly.
- Readers fail explicitly when `closure["dag"]` references a DAG id whose `refs/dags/<dag_id>.json` entry is missing, even if a raw CAS object with that DAG id still exists.

## Done when

- The repository has only the new per-DAG manifest implementation.
