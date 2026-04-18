# Executor State

## Status

specified

## Authority

This document is authoritative for the contrib `ExecutionRecord` shape and `ExecutionState` API.

Lifecycle ownership is authoritative in [runtime-contract.md](runtime-contract.md).

## Purpose

Define the single execution-state record used by built-in contrib runtimes.

## Scope

This document defines:

- the `ExecutionRecord` shape,
- the `ExecutionState` public API,
- lock and transition rules,
- ownership of terminal `done` writes.

This document does not define adapter payloads, executor-specific behavior, or cache publication rules.

## Execution Record

Built-in contrib runtimes use one DynamoDB-backed record per `cache_key`.

```python
class ExecutionRecord(TypedDict):
    cache_key: str
    argv_ptr: str
    status: Literal["pending", "running", "succeeded", "failed", "done"]
    lock_token: str | None
    lock_expires_ts: float | None
    dag_id: str | None
    error: str | None
    heartbeat_ts: float | None
    metadata: dict[str, Any]
    updated_ts: float
```

Rules:

- `cache_key` is the execution identity.
- `argv_ptr` is the remote manifest pointer for the invocation argv.
- `status` is the canonical lifecycle field.
- `dag_id` is required for `succeeded` records published by built-in runtimes.
- `error` is populated for `failed` records.
- `metadata` stores executor-owned runtime handles and debug data.
- `updated_ts` is refreshed on every successful mutation.
- `heartbeat_ts` is refreshed by heartbeats and terminal transitions.

## Public API

```python
LOCK_TTL = 15.0

class ExecutionState:
    cache_key: str
    lock_token: str | None

    def __init__(self, cache_key: str, *, table_name: str | None = None) -> None: ...

    @classmethod
    def upsert(cls, cache_key: str, argv_ptr: str, *, table_name: str | None = None) -> ExecutionRecord: ...

    def get(self) -> ExecutionRecord | None: ...
    def claim_running(self) -> bool: ...
    def lock(self, ttl: float = LOCK_TTL) -> bool: ...
    def unlock(self) -> bool: ...
    def heartbeat(self, duration: float = LOCK_TTL) -> bool: ...
    def update_metadata(self, data: dict[str, Any]) -> bool: ...
    def mark_running(self) -> bool: ...
    def mark_succeeded(self, dag_id: str) -> bool: ...
    def mark_failed(self, error: str) -> bool: ...
    def mark_done(self) -> bool: ...
```

Rules:

- `upsert(...)` creates the initial `pending` record and returns the existing record unchanged on conflicts.
- `claim_running()` atomically transitions only `pending -> running` and is the built-in launch-claim primitive.
- all mutating instance methods require a currently held, unexpired lock.
- `update_metadata(...)` merges keys into `metadata`.
- `mark_running()` transitions only `pending -> running`.
- `mark_succeeded(...)` transitions only `running -> succeeded`.
- `mark_failed(...)` transitions only `running -> failed`.
- `mark_done()` transitions only `succeeded|failed -> done`.

## Locking

- locks are advisory and identified by `lock_token`.
- `lock(ttl=...)` succeeds only when the record is unlocked or the stored lock has expired.
- `unlock()` succeeds only for the caller that still holds the valid lock.
- `heartbeat(duration=...)` extends `lock_expires_ts` and refreshes `heartbeat_ts`.

## Ownership

- adapters and executors own only in-flight transitions and metadata updates.
- `IndexOps.start_fn` owns cache publication for terminal states.
- `done` is a terminal tombstone written only by `start_fn` after cache publication or failed-result materialization.
- built-in runtimes do not define legacy per-backend state classes or parent-comms state mirroring.

## References

- [runtime-contract.md](runtime-contract.md)
- [../adapter-execution-contract.md](../adapter-execution-contract.md)
