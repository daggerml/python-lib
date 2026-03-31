---
status: specified
doc_type: spec
---

# Adapter Execution Contract

## Authority

This document is authoritative for adapter process invocation payloads, adapter output schema, runnable-chain invocation handoff semantics, and cache-key/argv-pointer execution boundary behavior.

## Scope

This doc defines runnable payload shape at adapter boundary, stdin/stdout adapter schema, invocation rules, and adapter output constraints.

## Purpose

Define the canonical external adapter invocation contract used by runtime execution paths.

## Glossary

- Adapter: The external executable invoked by the runtime.
- Runnable: The data model representing an execution payload.
- Uri: The location or identifier of the execution target.

## Contract

### Interfaces

- **Adapter Invocation Payload**
  - **Location/Name**: Adapter stdin payload.
  - **Signature/Schema**: JSON object containing:
    - `argv_ptr`
    - `cache_key`
    - `remote` (`root`, `cache`)
    - `runnable`
  - **Constraints**:
    - `remote` is required for adapter invocation.
    - Adapter stdin payload normalization is JSON-compatible and lossy (`Uri` values become plain URI strings, `Runnable` values become recursive JSON objects).
    - Consumers needing internal typed/runtime objects use `argv[0]` via `argv_ptr`, not stdin object typing.
    - Unspecified fields behavior: not specified.

- **Runnable Model at Execution Boundary**
  - **Signature/Schema**:
    - `target` (`Uri`),
    - `sub` (`None` or nested runnable),
    - `kwargs` (`dict[str, Any]`),
    - `adapter` (string).

- **Adapter Output**
  - **Location/Name**: Adapter stdout payload.
  - **Signature/Schema**: JSON object containing exactly `status` and `error` keys:
    - `status` in `pending|running|succeeded|failed`,
    - `error` present only for `failed`.

### Invariants

- `argv_ptr` is opaque at adapter/executor boundary and is forwarded unchanged.
- Adapter output keys are exactly `status` and `error`.
- Adapter/executor invocation is kickoff-or-poll and must be bounded.
- Long-running execution is asynchronous and resumed/polled by repeated invocations using `cache_key`.
- On `succeeded`, adapters/executors MUST publish the result DAG into remote cache for the execution cache identity before returning `succeeded`.
- Runtime result resolution on `succeeded` is cache-driven via execution cache identity, not adapter-returned commit pointers.
- Cache key basis is `argv_ref.id()` and is authoritative for adapter/executor state lookup.
- Adapter payload `cache_key` is a helper token for deduplication/correlation and MUST NOT override canonical cache identity derived from `argv`.

### Error Semantics

- **Failed Status**: Adapter output contains `error` key only when `status` is `failed`.

### Authority Handoffs

- Subcall behavior: if `runnable.sub` exists, current adapter invokes `sub.adapter` with same `argv_ptr`, same `cache_key`, same `remote`, and `runnable=sub`.
- Local contrib runtime may route adapter payload through a supervisor process/module; adapter output contract remains unchanged.

## Compatibility

- Backward compatibility with legacy `commit_ptr` adapter output is not supported.

## References

- [execution-model.md](execution-model.md)
- [remote-protocol.md](remote-protocol.md)
- [remote-data-model.md](remote-data-model.md)
- [internal/ops/index-ops.md](internal/ops/index-ops.md)
- [errors.md](errors.md)
