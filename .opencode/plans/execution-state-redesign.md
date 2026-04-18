# Execution State Redesign — Implementation Plan

## Overview

Replace the current executor state and adapter lock/poll/cache system with a DynamoDB-backed execution state machine. `start_fn` becomes the orchestrator (seeds state, calls adapter, publishes cache, marks done). Adapters own in-flight state transitions only. Named caches are removed in favor of a single unnamed cache namespace.

## Design summary

```
start_fn flow
   │
   ▼
check cache
   │
   ├─ hit  ──► return cached dag
   │
   └─ miss ──► state = upsert(cache_key, argv_ptr)
               │
               ├─ pending / running
               │    └─ call adapter
               │
               ├─ succeeded / failed
               │    └─ call adapter (final cleanup)
               │         then populate cache
               │         then set status="done"
               │         then return / raise
               │
               └─ done
                    └─ terminal no-op
```

```
state machine
   │
   ▼
pending ──mark_running────► running
running ──mark_succeeded──► succeeded ──► done (start_fn only)
running ──mark_failed─────► failed    ──► done (start_fn only)
```

```
locking model
   │
   ▼
lock_token = uuid4, lock_expires_ts = now + ttl
all mutations require valid lock
lock/unlock are separate from state transitions
```

---

## Task 01 — New Execution State API (DynamoDB-only)

### Objective

Replace `executor_state.py` with a new `ExecutionState` class backed exclusively by DynamoDB. Delete `LocalState`, `DynamoState`, `StateBase`, `StateRecord`, `state_from_comms`, `lock_from_comms`, `is_stale`.

### Scope

- Only `src/daggerml/contrib/executor_state.py` and its tests.
- No changes to adapters, executors, `start_fn`, or cache logic.

### Record shape

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

### Public API

```python
LOCK_TTL = 15.0

class ExecutionState:
    cache_key: str
    lock_token: str | None  # populated by lock()

    def __init__(self, cache_key: str, *, table_name: str | None = None) -> None: ...

    @classmethod
    def upsert(cls, cache_key: str, argv_ptr: str, *, table_name: str | None = None) -> ExecutionRecord: ...

    def get(self) -> ExecutionRecord | None: ...

    def lock(self, ttl: float = LOCK_TTL) -> bool: ...

    def unlock(self) -> bool: ...

    def heartbeat(self, duration: float = LOCK_TTL) -> bool: ...

    def update_metadata(self, data: dict[str, Any]) -> bool: ...

    def mark_running(self) -> bool: ...

    def mark_succeeded(self, dag_id: str) -> bool: ...

    def mark_failed(self, error: str) -> bool: ...
```

`mark_done` is intentionally absent — only `start_fn` (task 05) may write `done`.

### Deleted symbols

`StateBase`, `LocalState`, `DynamoState`, `StateRecord`, `state_from_comms`, `lock_from_comms`, `is_stale`, `HEARTBEAT_STALENESS`.

### Tests

File: `tests/contrib/test_executor_state.py` (rewrite). Use moto or DynamoDB local.

- `upsert` creates pending record; second call returns existing unchanged; different `argv_ptr` does not overwrite
- `lock` succeeds on unlocked record; fails on locked; succeeds on expired lock
- `unlock` succeeds with matching token; fails with wrong token
- `heartbeat` extends `lock_expires_ts`; fails with wrong/expired token
- `update_metadata` merges; requires valid lock
- `mark_running` from pending only; fails from other states; fails without valid lock
- `mark_succeeded` from running only with `dag_id`; fails from other states
- `mark_failed` from running only with `error`; fails from other states
- every mutation fails without valid lock

### Commit

One commit after tests pass.

---

## Task 02 — ExecutorBase shared dispatch

### Objective

Replace `ExecutorBase` (stub) and `_ExecutorBase` (dead code) with a shared `handle()` classmethod that dispatches to `start`/`poll`/`cleanup` based on current state.

### Depends on

Task 01.

### Scope

- `src/daggerml/contrib/executors/_base.py` only.

### Public API

```python
class ExecutorBase:
    adapter: str   # set by subclass
    name: str      # set by subclass

    def start(self, *, cache_key: str, state: ExecutionRecord,
              runnable: Runnable, argv_ptr: str, remote: dict[str, str]) -> None:
        raise NotImplementedError

    def poll(self, *, cache_key: str, state: ExecutionRecord) -> None:
        pass  # no-op default for supervisor-backed executors

    def cleanup(self, *, cache_key: str, state: ExecutionRecord) -> None:
        raise NotImplementedError

    @classmethod
    def handle(cls, *, cache_key: str, runnable: Runnable,
               argv_ptr: str, remote: dict[str, str]) -> dict[str, Any]:
        """Read state, dispatch to start/poll/cleanup.
        Lock is acquired only for the initial state read, then released.
        Sub-methods acquire their own locks as needed.
        After cleanup on succeeded/failed, re-acquires lock and sets status='done'
        via direct DynamoDB update (not exposed on ExecutionState public API).
        Returns {'status': ..., 'error': ...}."""
```

### Deleted symbols

`_ExecutorBase` (the entire dataclass).

### Dispatch logic

```
read state via ExecutionState(cache_key).get()
├─ pending   -> start()
├─ running   -> poll()
├─ succeeded -> cleanup() then mark done
├─ failed    -> cleanup() then mark done
└─ done      -> no-op, return current status
```

Note: `mark_done` is implemented as an internal conditional DynamoDB write within `handle()`, not exposed on `ExecutionState`.

### Tests

File: `tests/contrib/test_executor_base.py` (new).

- Mock executor subclass; verify `start` called on pending, `poll` on running, `cleanup` on succeeded/failed
- Verify `done` returned after cleanup completes
- Verify `done` state short-circuits without calling any method

### Commit

One commit after tests pass.

---

## Task 03 — Migrate executors to new base

### Objective

Migrate `ScriptExecutor`, `DockerExecutor`, `CfnExecutor`, `SshExecutor`, `BatchExecutor` to subclass the new `ExecutorBase` and use `ExecutionState` for state mutations.

### Depends on

Tasks 01, 02.

### Scope

- `src/daggerml/contrib/executors/script.py`
- `src/daggerml/contrib/executors/docker.py`
- `src/daggerml/contrib/executors/cfn.py`
- `src/daggerml/contrib/executors/ssh.py`
- `src/daggerml/contrib/executors/batch.py`
- `src/daggerml/contrib/executors/_lambda.py`
- `src/daggerml/contrib/executors/__init__.py`

### Contract per executor

Each executor implements `start`, `poll`, `cleanup` with the signatures from task 02.

- `start`: acquires lock via `ExecutionState.lock()`, calls `mark_running()`, performs side effects, releases lock. For supervisor-backed executors, the supervisor takes over heartbeat/status from here.
- `poll`: default no-op for supervisor-backed (Script, Docker, Batch). `CfnExecutor` overrides to inspect CloudFormation and update state only when warranted. `SshExecutor` is synchronous — `start` completes the execution inline.
- `cleanup`: kills/removes resources (containers, processes, jobs), idempotent.

### Deleted symbols

- `LambdaExecutorBase._release_lease`
- `LambdaExecutorBase._handle_once`
- All direct `state.put_if_absent`, `state.update`, `state.delete` calls in executors

### Tests

Update `tests/contrib/test_local_runtime.py` and other executor tests to use new API shape. Verify start/poll/cleanup lifecycle for each executor type.

### Commit

One commit after tests pass.

---

## Task 04 — Migrate supervisor to ExecutionState

### Objective

Update `supervisor.py` to use `ExecutionState` for heartbeats and terminal status writes instead of `lock_from_comms`.

### Depends on

Task 01.

### Scope

- `src/daggerml/contrib/supervisor.py`

### Changes

- Import `ExecutionState` instead of `lock_from_comms`.
- Heartbeat loop: `ExecutionState(cache_key).lock()` -> `heartbeat(duration=...)` -> `unlock()`.
- Worker success: `lock()` -> `mark_succeeded(dag_id)` -> `unlock()`.
- Worker failure: `lock()` -> `mark_failed(error)` -> `unlock()`.
- Supervisor must receive `dag_id` from the worker. Worker writes `dag_id` to `result.json` alongside `status`/`error`.
- Remove `comms` from supervisor payload; no longer needed.

### Payload v2

```python
{
    "version": 2,
    "cache_key": str,
    "cmd": list[str],
    "remote": {"root": str},   # "cache" removed (task 06)
    "env": dict[str, str]
}
```

Note: `comms` field is removed entirely.

### Tests

Update supervisor tests to verify:
- Heartbeat refreshes `heartbeat_ts` and `lock_expires_ts` during worker execution
- `mark_succeeded` called with `dag_id` from worker `result.json` on success
- `mark_failed` called with error string on failure
- No references to `lock_from_comms`

### Commit

One commit after tests pass.

---

## Task 05 — Rewrite `start_fn` as orchestrator

### Objective

Rewrite `IndexOps.start_fn` to: check cache first, call `upsert`, dispatch to adapter for non-done states, publish cache and mark done on succeeded/failed.

### Depends on

Tasks 01, 02, 03.

### Scope

- `src/daggerml/_internal/ops/index.py` (`start_fn`, `_call_adapter`, `_prepare_adapter_call`)
- `src/daggerml/contrib/adapters.py` (`LocalAdapter.send`, `LambdaAdapter.send`, `AdapterBase.cli`)

### New `start_fn` flow

```python
def start_fn(self, index_ref, argv, kwargv=None, name=None):
    # ... existing argv/kwargv prep ...
    with self._tx(readonly=False) as txn:
        argv_ref = self._prepare_fn(index_ref, argv, kwargv, txn)
        dag_ref = self._run_builtin(argv_ref, txn)
        if dag_ref is not None:
            return self._finish_fn_result(dag_ref, argv, name, txn, index_ref)
        cops = CacheOps(_db=self._db, remote_root=self.remote_root)
        dag_ref = cops._get(argv_ref, txn)
        if dag_ref is not None:
            return self._finish_fn_result(dag_ref, argv, name, txn, index_ref)
        prepared = self._prepare_adapter_call(argv_ref, txn)

    # Seed execution state
    state = ExecutionState.upsert(prepared.cache_key, prepared.argv_ptr)

    if state["status"] == "done":
        # Terminal tombstone — cache should already be populated
        # (or previous run failed; either way, nothing more to do)
        return None

    # Call adapter for any non-done state
    self._call_adapter(prepared, index_ref)

    # Re-read state
    state = ExecutionState(prepared.cache_key).get()

    if state["status"] in {"succeeded", "failed"}:
        # Call adapter one more time for cleanup
        self._call_adapter(prepared, index_ref)
        # Publish cache
        if state["dag_id"] is not None:
            # ... load dag from dag_id, call cops.put() ...
            pass
        elif state["status"] == "failed":
            # ... commit dag with error ...
            pass
        # Mark done (direct DynamoDB conditional write)
        # ... set status="done" where status in {succeeded, failed} ...
        # Re-check cache
        with self._tx(readonly=False) as txn:
            dag_ref = cops._get(prepared.argv_ref, txn)
            if dag_ref is not None:
                return self._finish_fn_result(dag_ref, argv, name, txn, index_ref)
        if state["status"] == "failed":
            raise DmlRepoError(state["error"] or "Adapter reported failure")

    return None  # still pending/running
```

### Adapter changes

- `LocalAdapter.send`: simplified to just call `ExecutorBase.handle()`; no lock/state management of its own
- `AdapterBase.cli`: remove `_report_parent_comms` calls; remove `comms` from payload parsing
- Remove `_release_lease` from `LocalAdapter`
- Adapter envelope: remove `comms` field; remove `remote.cache` field

### Deleted symbols

- `AdapterBase._report_parent_comms`
- `LocalAdapter._release_lease`
- `comms` field from `_dump_payload` / `_parse_payload`

### Tests

Update `tests/_internal/ops/test_index.py`:
- Cache hit returns immediately without touching state
- Cache miss + no state -> creates pending, calls adapter
- succeeded state -> adapter called for cleanup, cache published, done set
- failed state -> adapter called for cleanup, error raised, done set
- done state -> returns None

### Commit

One commit after tests pass.

---

## Task 06 — Remove named caches

### Objective

Remove the `remote_cache` / `cache_name` parameter from the entire stack. Cache refs move from `refs/cache/<cache_name>/<cache_key>.json` to `refs/cache/<cache_key>.json`.

### Depends on

Task 05 (since `start_fn` is already refactored to own cache publication).

### Scope

Source files:
- `src/daggerml/_internal/ops/remote.py`: `_cache_ref_path`, `_validate_cache_name`, `put_cache_ref`, `get_cache_ref`, `_ref_key`
- `src/daggerml/_internal/ops/cache.py`: remove `remote_cache` field from `CacheOps`; update `_get`/`put`/`get`/`delete`/`list`/`clear`
- `src/daggerml/_internal/ops/index.py`: remove `remote_cache` from `IndexOps`; update `commit()` auto-cache call; update `_prepare_adapter_call` envelope
- `src/daggerml/_internal/ops/__init__.py`: remove `remote_cache` from `DmlOps` factory methods
- `src/daggerml/api.py`: remove `remote_cache` from `Dml`
- `src/daggerml/_config.py`: remove `remote.cache` config key
- `src/daggerml/_cli/base.py`: remove `--remote-cache` CLI arg
- `src/daggerml/contrib/supervisor.py`: remove `remote["cache"]` from payload / env

Test files:
- `tests/_internal/ops/test_index.py`, `test_cache.py`
- `tests/_internal/cli/test_base.py`
- `tests/test_config.py`
- `tests/_internal/fn/*.py` (worker scripts)
- `tests/assets/fns/*.py`
- `tests/contrib/test_local_runtime.py`

Docs:
- `docs/internal/ops/dml-ops.md`
- `docs/remote-data-model.md`
- `docs/adapter-execution-contract.md`
- `docs/contrib/runtime-contract.md`
- `docs/contrib/executor-state.md`
- `docs/configuration.md`

### Contract changes

`RemoteOps._cache_ref_path`:
```python
# before
def _cache_ref_path(self, cache: str, cache_key: str) -> str:
    return f"cache/{cache}/{cache_key}.json"

# after
def _cache_ref_path(self, cache_key: str) -> str:
    return f"cache/{cache_key}.json"
```

`RemoteOps._ref_key`: remove `cache_name` validation segment from `cache` root; expect `cache/<key>.json` (2 segments, not 3).

`CacheOps`: remove `remote_cache` field. All methods take only `argv_ref`.

Adapter envelope `remote` field:
```python
# before
{"root": "s3://...", "cache": "my-cache-ns"}
# after
{"root": "s3://..."}
```

### Tests

- Verify `refs/cache/<cache_key>.json` path (no namespace segment)
- Verify `_ref_key` rejects old 3-segment `cache/<name>/<key>.json` paths
- Verify `CacheOps.put` / `_get` work without `remote_cache`
- Verify config loading without `remote.cache` key
- Verify CLI without `--remote-cache` flag

### Commit

One commit after tests pass.

---

## Task 07 — Update docs

### Objective

Update all affected documentation to reflect the new execution state model, removed named caches, and revised adapter/executor contracts.

### Depends on

Tasks 01–06.

### Scope

- `docs/contrib/executor-state.md` — rewrite for `ExecutionState` API and `ExecutionRecord` shape
- `docs/contrib/runtime-contract.md` — update lifecycle, remove comms, update supervisor payload to v2
- `docs/adapter-execution-contract.md` — remove `remote.cache` from envelope; update cache publication rule (adapters no longer publish cache; `start_fn` does)
- `docs/remote-data-model.md` — update cache ref path to `refs/cache/<cache_key>.json`; remove cache namespace references
- `docs/internal/ops/dml-ops.md` — remove `remote_cache` references
- `docs/configuration.md` — remove `remote.cache` config key
- `docs/DOC_MAP.md` — update if any new doc paths added

### Commit

One commit.

---

## Plan-level certification

Before the overall work is considered complete:

- `pytest` passes (full suite)
- No references remain to: `LocalState`, `DynamoState`, `StateBase`, `StateRecord`, `state_from_comms`, `lock_from_comms`, `remote_cache`, `cache_name`, `_ExecutorBase`, `_release_lease`
- Lint passes (pre-commit hooks)

## Docs consulted

- `docs/DOC_MAP.md`
- `docs/contrib/executor-state.md`
- `docs/contrib/runtime-contract.md`
- `docs/adapter-execution-contract.md`
- `docs/internal/ops/index-ops.md`
- `docs/remote-data-model.md`
