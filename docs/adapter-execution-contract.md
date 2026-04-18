---
status: specified
doc_type: spec
---

# Adapter Execution Contract

## Authority

This document is authoritative for adapter-boundary payloads, adapter output schema, runnable-chain handoff semantics, and cache-key and argv-pointer execution-boundary behavior.

## Scope

This doc defines runnable payload shape at the adapter boundary, stdin and stdout schema, invocation rules, and adapter output constraints.

## Purpose

Define the canonical external adapter invocation contract used by runtime execution paths.

## Glossary

- Adapter: the external executable invoked by the runtime.
- Runnable: the data model representing an execution payload.
- Uri: the location or identifier of the execution target.

## Contract

### Interfaces

- Adapter invocation payload:
  - location: adapter stdin payload,
  - schema: JSON object containing `argv_ptr`, `cache_key`, `remote`, and `runnable`,
  - `remote` MUST contain `root`.
- Runnable model at the execution boundary:
  - `target` (`Uri`),
  - `sub` (`None` or nested runnable),
  - `kwargs` (`dict[str, Any]`),
  - `adapter` (`str`).
- Adapter output:
  - location: adapter stdout payload,
  - schema: JSON object containing exactly `status` and `error`,
  - `status` is one of `pending|running|succeeded|failed`,
  - `error` is required only for `failed`.

### Invariants

- `argv_ptr` is opaque at the adapter and executor boundary and is forwarded unchanged.
- Adapter output keys are exactly `status` and `error`.
- Internal execution-state `done` is not an adapter-boundary status and MUST NOT be emitted by adapters.
- Adapter and executor invocation is kickoff-or-poll and must be bounded.
- Long-running execution is asynchronous and resumed by repeated invocations using `cache_key`.
- Built-in adapters and executors MUST NOT publish remote cache refs directly.
- `IndexOps.start_fn` publishes cache entries after observing terminal execution state.
- Runtime result resolution on success is cache-driven via execution cache identity, not adapter-returned commit pointers.
- Cache key basis is `argv_ref.id()` and is authoritative for execution-state lookup.
- Adapter payload `cache_key` is a helper token and MUST NOT override canonical cache identity derived from `argv`.

### Error Semantics

- `error` MUST be present only when `status == "failed"`.

### Authority Handoffs

- If `runnable.sub` exists, the current adapter invokes `sub.adapter` with the same `argv_ptr`, same `remote`, and `runnable=sub`; the nested invocation reuses the current `cache_key` unless the selected executor contract defines a persisted child execution identity for nested transport.
- Local contrib runtime may route adapter payload through a supervisor; adapter output shape remains unchanged.

## Compatibility

- Backward compatibility with legacy `commit_ptr` adapter output is not supported.

## References

- [execution-model.md](execution-model.md)
- [remote-protocol.md](remote-protocol.md)
- [remote-data-model.md](remote-data-model.md)
- [internal/ops/index-ops.md](internal/ops/index-ops.md)
- [errors.md](errors.md)
