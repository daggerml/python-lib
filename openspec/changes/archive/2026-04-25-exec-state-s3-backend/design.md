## Context

`ExecutionState` is the distributed mutex for function execution in daggerml. Multiple processes sharing the same `remote_root` may concurrently call `start_fn` for the same `cache_key` — only one should drive the adapter at a time.

Previously backed by DynamoDB with a rich state machine (`pending → running → succeeded/failed → done`), the new design strips this down: the lock file is a pure mutex, adapter-private state lives wherever the adapter chooses, and terminal results are communicated via adapter stdout. S3 is already required for all remote operations via `DML_REMOTE_ROOT`.

S3 conditional writes (`If-None-Match: *` for create, DELETE for release) provide the mutex. S3 has been strongly consistent since December 2020.

## Goals / Non-Goals

**Goals:**
- Implement `ExecutionState` in `daggerml._internal.exec_state` as a pure S3 mutex.
- Lock file stored at `{remote_root_prefix}/exec/{cache_key}.json` containing only `{lock_token, lock_expires_ts}`.
- Lock lifecycle: **create** (`If-None-Match: *`) → **delete**. No updates ever.
- TTL on creation is a safety net for crashed processes only.
- Rewrite `start_fn` in `_internal/ops/index.py` to: check cache → lock → recheck cache → call adapter → unlock → return.
- Adapter stdout carries terminal result `{status, dag_id?, error?}`.
- Remove `DML_DYNAMODB_TABLE` from the execution path entirely.

**Non-Goals:**
- Migrating `dml-util/aws/dynamodb.py`.
- Preserving the old `ExecutionState` API (`upsert`, `heartbeat`, `mark_*`, `claim_running`, etc.).
- Supervisor-based heartbeating (adapters must return quickly; long-running jobs manage their own external state).

## Decisions

### 1. Lock file = pure mutex, not state record

The old design encoded job status, `dag_id`, `error`, `heartbeat_ts`, and metadata into the lock record. All of that is removed. Adapters that need persistent state (e.g. a batch job ID to poll) store it themselves in S3 under their own keys. Terminal result flows back via stdout, not the lock file.

**Why:** Decouples the mutex from adapter-specific concerns. Each adapter is free to store whatever it needs without a shared schema.

### 2. Lock lifecycle is create/delete only

`lock()` = `PUT If-None-Match: *`. `unlock()` = `DELETE`. No ETag-based update, no heartbeat writes. The `lock_expires_ts` written at creation is the only TTL mechanism — if a process crashes, the next caller that sees an expired file can delete it and re-lock.

**Alternatives considered:**
- ETag-based update for heartbeat — rejected; adapters return quickly, no heartbeat needed.
- Separate lock + state files — rejected; unnecessary complexity given the stripped model.

### 3. `start_fn` rewritten around the mutex

```
1. check cache → hit? return node
2. lock()  → FAILED? return None  (another process is driving this cycle)
3. check cache again (post-lock)
   → HIT? delete lock file, return node
4. call_adapter() → stdout: {status, dag_id?, error?}
5. if succeeded: publish to cache, delete lock
   if failed:    delete lock, raise
   if running:   delete lock, return None  (adapter still working)
```

Steps 2–5 replace the old `upsert` + `claim_running` + `_mark_execution_done` dance.

### 4. `contrib/executor_state.py` removed, not deprecated

Since the API changes entirely (no `mark_*`, no `upsert`, no `heartbeat`), re-exporting a compatible shim is not meaningful. Existing contrib executors that used the old API must be rewritten to manage their own state. The module is deleted.

## Risks / Trade-offs

- **S3 conditional write requires boto3 ≥ 1.35.36** — `If-None-Match: *` silently dropped on older SDKs. Mitigation: document minimum version; moto always supports it in tests.
- **No TTL auto-expiry** — stale lock files persist until a caller checks and finds them expired. Mitigation: acceptable; next `lock()` call steals it.
- **Adapter must return quickly** — the lock is held for the duration of `call_adapter()`. A slow adapter blocks all other callers for that `cache_key`. Mitigation: this is a contract on adapter authors, documented explicitly.
- **Contrib executors need rewriting** — `batch.py`, `docker.py`, `script.py`, `cfn.py`, `_lambda.py`, `ssh.py`, `supervisor.py` all used the old `ExecutionState` API and must manage their own state going forward.

## Open Questions

- Should `lock()` steal an expired lock file via DELETE + re-PUT, or should it just DELETE and return `False` (forcing the caller to retry on the next `start_fn` invocation)? Recommendation: DELETE + re-PUT in one `lock()` call so the caller doesn't lose a cycle.
