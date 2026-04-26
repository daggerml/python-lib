## 1. New Module: `_internal/exec_state.py`

- [x] 1.1 Create `src/daggerml/_internal/exec_state.py` with `LockRecord` TypedDict `{lock_token: str, lock_expires_ts: float}` and `LOCK_TTL` constant
- [x] 1.2 Implement `ExecutionState.__init__(cache_key, *, remote_root)` — parse `s3://bucket/prefix`, derive key `{prefix}/exec/{cache_key}.json`; raise `DmlRepoError` if invalid
- [x] 1.3 Implement `ExecutionState.lock(ttl)` — GET existing file; if absent: PUT with `If-None-Match: *`; if expired: DELETE then PUT; if held: return `False`; return `True` on success
- [x] 1.4 Implement `ExecutionState.unlock()` — DELETE the lock file; no-op if already absent
- [x] 1.5 Handle `412 PreconditionFailed` from S3 as `False` return (not an exception) in `lock()`

## 2. Tests for `exec_state.py`

- [x] 2.1 Create `tests/test_exec_state.py` with moto S3 fixture (reuse pattern from `tests/conftest.py`)
- [x] 2.2 Test `lock()` creates file when absent, returns `True`
- [x] 2.3 Test `lock()` returns `False` when non-expired lock exists
- [x] 2.4 Test `lock()` steals expired lock (DELETE + re-PUT), returns `True`
- [x] 2.5 Test `lock()` returns `False` on `412` concurrent conflict
- [x] 2.6 Test `unlock()` deletes the file
- [x] 2.7 Test `unlock()` is idempotent when file absent
- [x] 2.8 Test missing/invalid `remote_root` raises `DmlRepoError`

## 3. Rewrite `start_fn` in `_internal/ops/index.py`

- [x] 3.1 Remove lazy import of `contrib.executor_state.ExecutionState`; import from `daggerml._internal.exec_state`
- [x] 3.2 Replace `upsert` + `_call_adapter` + `get` + `_publish_terminal_state` + `_mark_execution_done` with the new mutex-gated flow:
  - check cache → lock → recheck cache → call_adapter → handle result
- [x] 3.3 Update `_call_adapter` to parse stdout `{status, dag_id?, error?}` and return it
- [x] 3.4 On `succeeded`: call `CacheOps.put`, then `unlock()`
- [x] 3.5 On `failed`: call `unlock()`, raise `DmlRepoError`
- [x] 3.6 On `running`: call `unlock()`, return `None`
- [x] 3.7 Post-lock cache hit: DELETE lock file, return node

## 4. Rewrite contrib executors to manage their own state

- [x] 4.1 `contrib/executors/batch.py` — remove `ExecutionState` usage; store job state (job ID, status) in adapter-owned S3 key; return `{status, dag_id?, error?}` via stdout
- [x] 4.2 `contrib/executors/docker.py` — same as 4.1
- [x] 4.3 `contrib/executors/script.py` — same as 4.1
- [x] 4.4 `contrib/executors/cfn.py` — same as 4.1
- [x] 4.5 `contrib/executors/_lambda.py` — same as 4.1
- [x] 4.6 `contrib/executors/ssh.py` — same as 4.1
- [x] 4.7 `contrib/supervisor.py` — remove `ExecutionState` usage; supervisor result written to a local file read by adapter on next call
- [x] 4.8 Remove `DML_DYNAMODB_TABLE` forwarding from `batch.py` and `docker.py`

## 5. Delete `contrib/executor_state.py`

- [x] 5.1 Delete `src/daggerml/contrib/executor_state.py`
- [x] 5.2 Remove `ExecutionRecord`, `ExecutionState` imports from all contrib files

## 6. Update tests and fixtures

- [x] 6.1 Remove DynamoDB moto fixture from `tests/conftest.py` (`test-dml-state` table, `DML_DYNAMODB_TABLE` env var)
- [x] 6.2 Delete `tests/contrib/test_executor_state.py` (superseded by `tests/test_exec_state.py`)
- [x] 6.3 Remove DynamoDB fixture dependency from `tests/contrib/test_executor_base.py`
- [x] 6.4 Remove `DML_DYNAMODB_TABLE` propagation from `tests/contrib/test_ssh_integration.py`
- [x] 6.5 Update `tests/contrib/test_docker_executor.py`: remove `DML_DYNAMODB_TABLE` assertion

## 7. Update documentation

- [x] 7.1 Update `docs/contrib/executor-state.md`: describe mutex-only model, remove DynamoDB table requirement
- [x] 7.2 Update `docs/contrib/executor-catalog.md`: remove `DML_DYNAMODB_TABLE` from required env vars; document adapter stdout contract
- [x] 7.3 Update `docs/contrib/execution-graph.md`: reflect S3-only backend and simplified lock lifecycle
