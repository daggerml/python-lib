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
  - `send(*, runnable, argv_ptr, cache_key, execution_id, remote, state)`,
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
  - `start(*, cache_key, execution_id, runnable, argv_ptr, remote)`,
  - `poll(*, cache_key, execution_id, state, remote)`,
  - `cleanup(*, cache_key, execution_id, state, remote)`.
- Shared executor lifecycle:
  - runtime coordination MUST acquire the `cache_key` mutex, inspect `dml/active/<cache_key>`, and dispatch either a first `start(...)` call or a resumed `poll(...)` call,
  - first launch uses `state = null`,
  - resumed execution dispatches to `poll(...)` with the immutable stored launch-time `state`,
  - kickoff and poll invocations MUST be bounded,
  - stateful executors MUST resume existing work for the same active `execution_id` and MUST NOT relaunch duplicate jobs,
  - `poll(...)` MAY be a no-op for supervisor-backed executors,
  - `cleanup(...)` MUST be idempotent.
- Shared state handling:
  - runtime owns `dml/active/<cache_key>`, caller-owned `dml/exec/launch/<execution_id>.json`, and runtime-owned `dml/exec/state/<execution_id>.json`,
  - active execution pointers identify the current `execution_id` for the cache key,
  - built-in launch coordination MUST use the active pointer plus mutex so concurrent callers do not launch duplicate work,
  - executors MUST return all durable resume handles in the first `running` result,
  - later executor `running` results MAY include `state`, but runtime MAY ignore replacement state after creating `launch_state`,
  - built-in adapters and executors MUST NOT publish cache refs directly,
  - built-in adapters and executors MUST NOT write terminal `done`; `start_fn` owns terminal cache publication.
- Result publication:
  - `IndexOps.start_fn` MUST publish cache entries after it observes `succeeded` or `failed`,
  - `cache_key` is a deduplication and correlation helper and MUST NOT override canonical argv-derived cache identity.
- Supervisor payload:
  - canonical payload version is `2`,
  - `Supervisor.run(payload)` MUST accept:
    - `version`: `2`,
  - `cache_key`: non-empty string,
  - `execution_id`: non-empty string,
  - `cmd`: non-empty `list[str]`,
    - `remote`: object with `root` string,
    - `env`: optional `dict[str, str]`,
  - unknown top-level fields MUST be rejected,
  - `python -m daggerml.contrib.supervisor` MUST accept the same payload from stdin or a file,
  - worker success reporting MUST include `dag_id` in `result.json` as `{status,error,dag_id}` so the supervisor can mark success,
  - worker failure reporting MUST remain `{status,error}`,
  - supervisor-managed worker `stdout` and `stderr` MUST be captured into local `stdout.log` and `stderr.log` files in the supervisor workdir,
  - supervisor-managed worker `stdout` and `stderr` MUST also be streamed best-effort to CloudWatch Logs group `dml` using streams `/run/{cache_key}/stdout` and `/run/{cache_key}/stderr`,
  - each CloudWatch stream MUST receive a start lifecycle event containing `execution_id`, `cache_key`, and stream kind before worker output, and an end lifecycle event containing the same metadata plus terminal status after worker exit,
  - CloudWatch initialization or delivery failures MUST disable further CloudWatch writes for the affected stream without changing the supervisor terminal result,
  - non-terminal worker results MUST be rejected once the worker process has exited.

### Invariants

- built-in runtime state machine is `running -> succeeded|failed` at the adapter boundary.
- runtime coordination state is `no active execution` or `active execution_id` for a `cache_key`.
- state-record shape and field ownership MUST remain consistent with [executor-state.md](executor-state.md).
- built-in executor definitions MUST remain consistent with [executor-catalog.md](executor-catalog.md).
- backward compatibility with legacy `commit_ptr` success payloads is not supported.

### Error Semantics

- Adapter routing failures are terminal for the current invocation.
- Executor lifecycle failures are retryable only when the selected executor defines them as recoverable through repeated polling.
- State backend or locking failures are retryable only when caused by transient contention or backend unavailability.
- Supervisor payload validation failures are terminal until the caller fixes the payload shape.

### Observability

- Executors SHOULD preserve enough launch-time metadata to identify runtime handles needed for polling, cleanup, and debugging.
- Supervisor-backed executions preserve local worker log files for fallback debugging even when CloudWatch streaming is unavailable.
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
