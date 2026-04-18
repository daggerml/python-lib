---
status: specified
doc_type: spec
---

# Contrib Runtime Contract

## Authority

This document is authoritative for contrib runtime role boundaries, shared adapter and executor lifecycle contracts, and contrib supervisor payload behavior.

## Scope

This document does not define:

- the adapter-boundary payload or output schema; refer to [../adapter-execution-contract.md](../adapter-execution-contract.md),
- state-record field shape or serialization details; refer to [executor-state.md](executor-state.md),
- per-executor kwargs schemas or executor-specific runtime details beyond shared lifecycle requirements; refer to [executor-catalog.md](executor-catalog.md).

## Glossary

- Adapter Boundary: the runtime boundary defined by [../adapter-execution-contract.md](../adapter-execution-contract.md).
- Contrib Adapter: the contrib CLI or process surface named by `runnable.adapter`.
- Contrib Executor: the runtime component selected by a contrib adapter to perform execution behavior.
- Supervisor: the helper process exposed by `daggerml.contrib.supervisor`.

## Contract

### Interfaces

- Contrib adapter and executor composition:
  - for a contrib execution surface, the selected adapter and executor together satisfy the adapter-boundary contract,
  - adapters own ingress, payload parsing, and executor selection,
  - executors own execution behavior and execution-state transitions.
- Required contrib adapter class surface:
  - `name`,
  - `send(*, runnable, argv_ptr, cache_key, remote)`,
  - `resolve_runnable(uri, kwargs, sub)`,
  - `cli(argv)`.
- Adapter behavior:
  - `cli(argv)` MUST parse adapter payload input into `send(...)` arguments,
  - `cli(argv)` MUST support polling mode by repeatedly calling `send(...)` until terminal when requested,
  - `send(...)` MUST perform one bounded runtime step,
  - built-in adapters MUST preserve the adapter payload fields required by the selected executor.
- Built-in contrib adapters:
  - `local` dispatches to a local executor and performs one bounded lifecycle step,
  - `lambda` forwards the runtime payload to the configured Lambda function and returns canonical adapter output.
- Required contrib executor class surface:
  - `name`,
  - `adapter`,
  - `resolve_runnable(uri, kwargs, sub)`,
  - `start(*, cache_key, state, runnable, argv_ptr, remote)`,
  - `poll(*, cache_key, state)`,
  - `cleanup(*, cache_key, state)`.
- Shared executor lifecycle:
  - runtime coordination MUST read `ExecutionState` and dispatch by current status,
  - `pending` dispatch MUST first atomically claim the execution in `ExecutionState` before calling `start(...)`,
  - `running` dispatches to `poll(...)`,
  - `succeeded` and `failed` dispatch to `cleanup(...)`,
  - `done` is terminal; `IndexOps.start_fn` MUST short-circuit it before executor dispatch and MUST NOT relaunch,
  - kickoff and poll invocations MUST be bounded,
  - stateful executors MUST resume existing work for the same `cache_key` and MUST NOT relaunch duplicate jobs,
  - `poll(...)` MAY be a no-op for supervisor-backed executors,
  - `cleanup(...)` MUST be idempotent.
- Shared state handling:
  - executors MUST use `ExecutionState` for in-flight transitions and metadata,
  - built-in launch coordination MUST use the atomic pending-to-running claim so concurrent callers do not launch duplicate work,
  - executors MAY write executor-specific metadata only under `state["metadata"]`,
  - built-in adapters and executors MUST NOT publish cache refs directly,
  - built-in adapters and executors MUST NOT write terminal `done`; `start_fn` owns that write.
- Result publication:
  - `IndexOps.start_fn` MUST publish cache entries after it observes `succeeded` or `failed`,
  - `cache_key` is a deduplication and correlation helper and MUST NOT override canonical argv-derived cache identity.
- Supervisor payload:
  - canonical payload version is `2`,
  - `Supervisor.run(payload)` MUST accept:
    - `version`: `2`,
    - `cache_key`: non-empty string,
    - `cmd`: non-empty `list[str]`,
    - `remote`: object with `root` string,
    - `env`: optional `dict[str, str]`,
  - unknown top-level fields MUST be rejected,
  - `python -m daggerml.contrib.supervisor` MUST accept the same payload from stdin or a file,
  - worker success reporting MUST include `dag_id` in `result.json` as `{status,error,dag_id}` so the supervisor can mark success,
  - worker failure reporting MUST remain `{status,error}`,
  - `pending` and `running` worker results MUST be rejected once the worker process has exited,
  - the supervisor MUST refresh heartbeats while the worker is running.

### Invariants

- built-in runtime state machine is `pending -> running -> succeeded|failed -> done`.
- `done` means cleanup and cache publication are already complete for that execution identity.
- state-record shape and field ownership MUST remain consistent with [executor-state.md](executor-state.md).
- built-in executor definitions MUST remain consistent with [executor-catalog.md](executor-catalog.md).
- backward compatibility with legacy `commit_ptr` success payloads is not supported.

### Error Semantics

- Adapter routing failures are terminal for the current invocation.
- Executor lifecycle failures are retryable only when the selected executor defines them as recoverable through repeated polling.
- State backend or locking failures are retryable only when caused by transient contention or backend unavailability.
- Supervisor payload validation failures are terminal until the caller fixes the payload shape.

### Observability

- While `Supervisor.run(payload)` is active, the supervisor MUST update heartbeat state through `ExecutionState`.
- Executors SHOULD preserve enough metadata to identify runtime handles needed for polling, cleanup, and debugging.
- Runtime status and plugin discovery remain authoritative in [status.md](status.md).

### Authority Handoffs

- Adapter-boundary payload and output schema are authoritative in [../adapter-execution-contract.md](../adapter-execution-contract.md).
- State-record shape and backend reference behavior are authoritative in [executor-state.md](executor-state.md).
- Per-executor kwargs and runtime details are authoritative in [executor-catalog.md](executor-catalog.md).
- Contrib registry and discovery contracts are authoritative in [registries.md](registries.md).
- Runtime diagnostics and status reporting are authoritative in [status.md](status.md).

## Compatibility

- Supervisor payload versioning is explicit through `payload.version`; this document defines canonical version `2`.
- Supervisor payload handling is not forward-compatible with unknown top-level fields.

## References

- [../adapter-execution-contract.md](../adapter-execution-contract.md)
- [executor-state.md](executor-state.md)
- [executor-catalog.md](executor-catalog.md)
- [registries.md](registries.md)
- [status.md](status.md)
- [../errors.md](../errors.md)
