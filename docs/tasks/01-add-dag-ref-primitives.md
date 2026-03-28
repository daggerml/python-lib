# Task 01 - Add DAG ref primitives

## Goal

Add the low-level `RemoteOps` primitives needed for per-DAG manifests without changing existing publish/pull call paths yet.

## Current code anchors

- `Ref.ns()` and `Ref.id()` define namespace/id access for refs today: `src/daggerml/_internal/_db.pyi:41`, `src/daggerml/_internal/_db.pyi:42`.
- Local manifest roots are currently emitted directly from `ref.ns()` and `ref.id()`: `src/daggerml/_internal/ops/base_ops.py:326`.
- Remote ref paths currently accept only `tags/...` and `cache/...`: `src/daggerml/_internal/ops/remote.py:199`.
- Remote ref payloads currently validate `target` as 64 lowercase hex and do not yet validate `targets`: `src/daggerml/_internal/ops/remote.py:384`.
- Remote manifest payloads already validate closure ids as 64 lowercase hex strings, which is the current code-level basis for treating DAG ids the same way in these tasks: `src/daggerml/_internal/ops/remote.py:443`.
- Planning constraint: do not edit `src/daggerml/_internal/_db.pyi`, `src/daggerml/_internal/_db.pyx`, `src/daggerml/_internal/ops/base_ops.py`, or `src/daggerml/_internal/ops/remote.py` as part of this task-doc update. Only edit files under `docs/tasks/` in this planning pass.

## Implement

- Add internal DAG ref path support for `refs/dags/<dag_id>.json`.
- Add dedicated helpers for DAG refs; do not change the existing `RemoteOps._ref_key(ref_path: str) -> str` contract in this task.
- Extend ref decoding/validation so manifest refs can carry top-level `targets`.
- Validate `targets` when present:
  - it must be a JSON object
  - only the `dag` key is allowed
  - `targets["dag"]` must be a sorted unique list of DAG ids
  - each DAG id must be a 64-character lowercase hex string matching `^[0-9a-f]{64}$`
  - an empty `targets["dag"]` list is valid
- Validation failures in `_decode_ref(...)` must raise `InvalidRef`.
- Keep existing tag/cache publish flows unchanged in this task.

## Inputs and outputs

- `RemoteOps._dag_ref_path(dag_id: str) -> str`
  - input: logical DAG id string matching `^[0-9a-f]{64}$`
  - output: `dags/<dag_id>.json`
- `RemoteOps._dag_ref_key(dag_id: str) -> str`
  - input: logical DAG id string matching `^[0-9a-f]{64}$`
  - output: prefixed remote object key string for `refs/dags/<dag_id>.json`
- `RemoteOps._ref_key(ref_path: str) -> str`
  - input: ref path string rooted at `tags/...` or `cache/...`
  - output: prefixed remote object key string
  - note: this task must not extend `_ref_key(...)` to accept `dags/...`
- `RemoteOps._decode_ref(data: bytes) -> dict[str, Any]`
  - input: JSON bytes for a ref object
  - output: validated ref dictionary including optional `targets`
  - errors:
    - raise `InvalidRef` if the payload is not valid ref JSON
    - raise `InvalidRef` if `targets` is present but malformed
    - raise `InvalidRef` if `targets` contains unsupported namespaces or malformed DAG ids

## Error semantics

- `_dag_ref_path(...)` and `_dag_ref_key(...)` must raise `ValueError` if `dag_id` does not match `^[0-9a-f]{64}$`.
- `_decode_ref(...)` must raise `InvalidRef` with a specific message for each malformed `targets` case:
  - `Invalid ref: targets must be an object`
  - `Invalid ref: targets supports only the 'dag' namespace`
  - `Invalid ref: targets.dag must be a sorted unique list of 64 lowercase hex ids`

## IO

- No new remote writes in this task.
- Remote reads/writes through existing callers must keep working for `tags/...` and `cache/...`.
- New DAG ref paths must map to `refs/dags/<dag_id>.json` in S3 via `_dag_ref_key(...)`.
- This task does not add any production reads of DAG refs yet; it only defines the path and validation primitives required by later tasks.

## Expected behavior to test

- `_dag_ref_path(<64-hex dag id>)` returns `dags/<dag_id>.json`.
- `_dag_ref_key(<64-hex dag id>)` maps under `refs/dags/<dag_id>.json`.
- `_dag_ref_path(...)` and `_dag_ref_key(...)` reject non-hex or wrong-length DAG ids with `ValueError`.
- `_ref_key("dags/<dag_id>.json")` still rejects that input exactly as before.
- `_decode_ref(...)` accepts a ref with no `targets`.
- `_decode_ref(...)` accepts `targets={"dag": []}`.
- `_decode_ref(...)` accepts `targets={"dag": [<dag_id_1>, <dag_id_2>]}` when sorted and unique.
- `_decode_ref(...)` rejects:
  - non-object `targets`
  - unsupported target namespaces
  - duplicate DAG ids
  - unsorted DAG ids
  - non-string DAG ids
  - DAG ids that are not 64-character lowercase hex strings
- Existing `tags/...` and `cache/...` ref validation still passes unchanged.

## Done when

- DAG refs and `targets` can be represented and validated internally.
- No existing publish/pull behavior changes yet.
