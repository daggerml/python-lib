# Execution Model

## Status

specified

## Authority

This document is authoritative for the model semantics and invariants described in this document.
If related docs conflict on this scope, this document is the source of truth.


## Purpose

The execution model defines end-to-end function execution: call preparation, runnable resolution, adapter invocation, and result materialization.

## Call Preparation

- First positional arg must resolve to `RunnableDatum`.
- Runtime builds `ArgvNode` with:
  - `argv[0]`: runnable chain,
  - `argv[1:]`: argument datum refs.
- Runtime derives/stores `KwargvNode` for inner-most runnable kwargs in the working DAG.

## Kwarg Resolution

- Resolve keys inner-most to outer-most across runnable `kwargs`.
- Unknown key raises `DmlRepoError("Unknown kwarg: <key>")`.

## Execution Order

1. Attempt builtin execution for supported `daggerml:` URIs.
2. If not builtin, resolve cache via remote refs using argv identity.
3. If cache miss, publish `argv_ptr` via remote manifest upload, acquire the `cache_key` lock, and inspect the active execution pointer for that `cache_key`.
4. If no active execution exists, runtime creates a new `execution_id`, invokes the adapter with `state = null`, and on `running` persists caller-owned `launch_state`, runtime-owned `execution_record`, and an active pointer.
5. If an active execution exists, runtime resumes it by invoking the adapter with `resume_state` from `exec/launch/<execution_id>.json` while reading lifecycle from `exec/state/<execution_id>.json`.
6. On adapter `succeeded`, caller resolves the result DAG from remote cache refs using argv identity.
7. On adapter `failed`, caller materializes a failed DAG, publishes it to cache, and raises the resulting DAG error to the caller.
8. On adapter `running`, caller returns without materializing a result node.

Cache rules:

- execution cache identity and adapter `cache_key` contracts are defined in [adapter-execution-contract.md](adapter-execution-contract.md).
- runtime maintains `dml/active/<cache_key>` as the authoritative mapping from computation identity to the current in-flight `execution_id`.
- runtime stores caller-owned resumable launch data at `dml/exec/launch/<execution_id>.json` and runtime-owned lifecycle data at `dml/exec/state/<execution_id>.json`.
- `launch_state` stores only `execution_id`, `cache_key`, `resume_state`, and `created_at`.
- `execution_record` stores only `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `spawned_execution_ids`, and `cancellation_requested_by`.
- live caller edges under `dml/exec/edges/<callee>/<caller>.json` are caller-owned and are used for orphan detection and invalidation.
- `execution_record.spawned_execution_ids` is runtime-owned historical cancellation traversal state and is not removed when live caller edges are dropped.
- cancellation is best-effort across `spawned_execution_ids`; descendants behind already-terminal intermediates may remain running.
- adapter `remote` context contracts are defined in [adapter-execution-contract.md](adapter-execution-contract.md).
- `remote` context MUST be populated for non-builtin adapter execution and SHOULD be propagated in nested calls.
- remote cache-ref layout and cache-key path constraints are defined in [remote-data-model.md](remote-data-model.md).
- non-builtin adapter execution MUST fail deterministically when remote context is unavailable.
- non-builtin function execution without a configured remote cache context MUST fail deterministically.
- cache lookup/write policy is caller-owned; adapters/executors return adapter status/output per [adapter-execution-contract.md](adapter-execution-contract.md).
- stale-lock recovery keeps the active `execution_id` when the corresponding `launch_state` and non-terminal `execution_record` still exist.

## Adapter Path

- Input contract is defined in [adapter-execution-contract.md](adapter-execution-contract.md).
- Output contract is defined in [adapter-execution-contract.md](adapter-execution-contract.md).

## Result Materialization

- Runtime creates `FnNode` linking call-site nodes and function DAG.
- If function DAG contains `error`, error is raised to caller.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
