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
  - schema: JSON object containing `argv_ptr`, `cache_key`, `execution_id`, `remote`, `runnable`, and `state`,
  - `remote` MUST contain `root`.
- Runnable model at the execution boundary:
  - `target` (`Uri`),
  - `sub` (`None` or nested runnable),
  - `kwargs` (`dict[str, Any]`),
  - `adapter` (`str`).
- Adapter output:
  - location: adapter stdout payload,
  - `running` schema: JSON object containing exactly `status`, `error`, and `state`,
  - `succeeded` schema: JSON object containing exactly `status`, `error`, and `dag_id`,
  - `failed` schema: JSON object containing exactly `status` and `error`,
  - `status` is one of `running|succeeded|failed`.

### Invariants

- `argv_ptr` is opaque at the adapter and executor boundary and is forwarded unchanged.
- `execution_id` identifies the current in-flight execution attempt and is runtime-assigned.
- `state` in the adapter payload is `null` on first launch and the immutable stored launch-time state on later polls.
- Internal execution-state `done` is not an adapter-boundary status and MUST NOT be emitted by adapters.
- Adapter and executor invocation is kickoff-or-poll and must be bounded.
- Long-running execution is asynchronous and resumed by repeated invocations using `execution_id` plus the immutable stored launch-time `state`.
- Built-in adapters and executors MUST NOT publish remote cache refs directly.
- `IndexOps.start_fn` publishes cache entries after observing terminal execution state.
- Runtime result resolution on success is cache-driven via execution cache identity, not adapter-returned commit pointers.
- Cache key basis is `argv_ref.id()` and is authoritative for execution-state lookup.
- Adapter payload `cache_key` is a helper token and MUST NOT override canonical cache identity derived from `argv`.
- Adapters MUST return all durable resume handles needed for later polling in the first `running` result.
- Runtime ignores replacement `state` returned by later `running` results after it has created `fn-exec/records/<cache_key>/<execution_number>.json`.

### Error Semantics

- `error` MUST be present only when `status == "failed"`.
- `state` MUST be present only when `status == "running"`.
- `dag_id` MUST be present only when `status == "succeeded"`.

### Authority Handoffs

- If `runnable.sub` exists, the current adapter invokes `sub.adapter` with the same `argv_ptr`, same `cache_key`, same `execution_id`, same `remote`, `state = null`, and `runnable=sub` unless the selected executor contract defines a different child execution identity.
- Local contrib runtime may route adapter payload through a supervisor; adapter output shape remains unchanged.

## Compatibility

- Backward compatibility with legacy `commit_ptr` adapter output is not supported.

## References

- [execution-model.md](execution-model.md)
- [remote-protocol.md](remote-protocol.md)
- [remote-data-model.md](remote-data-model.md)
- [internal/ops/index-ops.md](internal/ops/index-ops.md)
- [errors.md](errors.md)
