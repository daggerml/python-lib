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
4. If no active execution exists, runtime allocates the next `execution_number` for the `cache_key`, creates a new `execution_id`, invokes the adapter with `state = null`, and on `running` persists an immutable execution record plus an active pointer.
5. If an active execution exists, runtime resumes it by invoking the adapter with the immutable stored launch-time `state` from `fn-exec/records/<cache_key>/<execution_number>.json`.
6. On adapter `succeeded`, caller resolves the result DAG from remote cache refs using argv identity.
7. On adapter `failed`, caller materializes a failed DAG, publishes it to cache, and raises the resulting DAG error to the caller.
8. On adapter `running`, caller returns without materializing a result node.

Cache rules:

- execution cache identity and adapter `cache_key` contracts are defined in [adapter-execution-contract.md](adapter-execution-contract.md).
- runtime maintains `fn-exec/active/<cache_key>` as the authoritative mapping from computation identity to the current in-flight `execution_number`.
- runtime stores immutable execution records at `fn-exec/records/<cache_key>/<execution_number>.json`; each record contains both `execution_number` and `execution_id`, and only the first `running` result persists adapter state.
- adapter `remote` context contracts are defined in [adapter-execution-contract.md](adapter-execution-contract.md).
- `remote` context MUST be populated for non-builtin adapter execution and SHOULD be propagated in nested calls.
- remote cache-ref layout and cache-key path constraints are defined in [remote-data-model.md](remote-data-model.md).
- non-builtin adapter execution MUST fail deterministically when remote context is unavailable.
- non-builtin function execution without a configured remote cache context MUST fail deterministically.
- cache lookup/write policy is caller-owned; adapters/executors return adapter status/output per [adapter-execution-contract.md](adapter-execution-contract.md).
- stale-lock recovery keeps the active `execution_id` when the corresponding immutable execution record still exists.

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
