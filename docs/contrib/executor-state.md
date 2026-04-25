# Executor State (Mutex)

## Status

specified

## Authority

This document is authoritative for the contrib `ExecutionState` mutex API.

Lifecycle ownership is authoritative in [runtime-contract.md](runtime-contract.md).

## Purpose

Define the S3-backed advisory mutex used by built-in contrib runtimes to coordinate concurrent execution of the same `cache_key`.

## Scope

This document defines:

- the lock record shape,
- the `ExecutionState` public API,
- lock acquire and release rules.

This document does not define adapter payloads, execution-record payloads, call-edge indexes, or cache publication rules.

## Lock Record

The mutex stores a minimal JSON object at `{remote_root_prefix}/fn-exec/locks/{cache_key}.json`:

```python
class LockRecord(TypedDict):
    lock_token: str          # random UUID identifying the current lock holder
    lock_expires_ts: float   # unix timestamp after which the lock may be stolen
```

Rules:

- The lock file is created with `If-None-Match: *` (conditional PUT) so only one caller succeeds when two race.
- The lock file is released with a plain DELETE.
- No updates are ever written — the file is created once and deleted once per lock cycle.
- `lock_expires_ts` is a crash-safety TTL only; it is not polled continuously.

## Public API

```python
LOCK_TTL = 300.0

class ExecutionState:
    def __init__(self, cache_key: str, *, remote_root: str | None = None) -> None: ...

    def lock(self, ttl: float = LOCK_TTL) -> bool:
        """Acquire the mutex.

        Returns True on success. Returns False (does not raise) when:
        - lock is already held by another caller (non-expired),
        - S3 conditional PUT returns 412 PreconditionFailed.

        Steals an expired lock (DELETE + re-PUT) in a single call.
        """

    def unlock(self) -> None:
        """Release the mutex. No-op if not currently held."""
```

Rules:

- `lock()` returns `True` when the mutex is successfully acquired.
- `lock()` returns `False` when the lock is held by another caller or a concurrent PUT raced and lost.
- `lock()` steals expired locks (DELETE + re-PUT) rather than forcing the caller to retry on the next cycle.
- `unlock()` is idempotent — safe to call even if the lock file is already absent.
- The mutex protects runtime mutation of the active execution for a `cache_key`; it does not define execution-record or lineage payloads.

## Ownership

- `IndexOps.start_fn` owns the mutex lifecycle: it acquires before inspecting or mutating the active execution and releases after the adapter returns.
- Runtime owns `fn-exec/active/<cache_key>` and `fn-exec/records/<cache_key>/<execution_number>.json`; adapters and executors no longer own a mutable S3 execution-state prefix.
- `done` tombstone semantics are removed; terminal result flows back via adapter stdout and cache publication.

## References

- [runtime-contract.md](runtime-contract.md)
- [../adapter-execution-contract.md](../adapter-execution-contract.md)
