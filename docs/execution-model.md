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
3. If cache miss, invoke adapter via stdin JSON envelope with remote context.
4. On adapter `succeeded`, caller resolves the result DAG from remote cache refs using argv identity.

Cache rules:

- execution cache identity and adapter `cache_key` contracts are defined in [adapter-execution-contract.md](adapter-execution-contract.md).
- adapter `remote` context contracts are defined in [adapter-execution-contract.md](adapter-execution-contract.md).
- `remote` context MUST be populated for non-builtin adapter execution and SHOULD be propagated in nested calls.
- remote cache-ref layout and cache-namespace constraints are defined in [remote-data-model.md](remote-data-model.md).
- non-builtin adapter execution MUST fail deterministically when remote context is unavailable.
- non-builtin function execution without a configured remote cache context MUST fail deterministically.
- cache lookup/write policy is caller-owned; adapters/executors return adapter status/output per [adapter-execution-contract.md](adapter-execution-contract.md).

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
