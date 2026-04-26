## Why

Execution state currently requires a DynamoDB table, adding operational overhead to every deployment. By moving execution state to S3 — which is already required for all other remote operations — the entire infrastructure footprint becomes a single S3 prefix, simplifying setup and enabling atomic cache semantics via S3 conditional writes.

## What Changes

- **New module** `daggerml._internal.exec_state`: S3-backed `ExecutionState` with advisory locking via S3 conditional writes (`If-None-Match`/`If-Match` on ETags), replacing the DynamoDB implementation.
- **State objects** are stored at `{remote_root_prefix}/exec/{cache_key}.json`, sibling to `refs/`, since they reference internal DAG refs in their payload.
- **`daggerml._internal.ops.index`** updated to import `ExecutionState` from `_internal.exec_state` instead of `daggerml.contrib.executor_state`.
- **`daggerml.contrib.executor_state`** deprecated; callers in `contrib` updated to use the new internal module.
- **Removed dependency** on `DML_DYNAMODB_TABLE` environment variable and DynamoDB boto3 client in the execution path.
- **Test infrastructure** updated: moto DynamoDB table fixture replaced with moto S3 fixture (already present for other tests).

## Capabilities

### New Capabilities

- `execution-state`: S3-backed execution state record with advisory locking, heartbeat, and status transitions (`pending → running → succeeded/failed → done`), accessed via `remote_root` string rather than a DynamoDB table name.

### Modified Capabilities

<!-- none -->

## Impact

- **`src/daggerml/_internal/ops/index.py`**: change import source for `ExecutionState`; pass `remote_root` instead of table name.
- **`src/daggerml/contrib/executor_state.py`**: deprecated (kept for backwards compatibility or removed).
- **`src/daggerml/contrib/executors/batch.py`**, **`docker.py`**: no longer need to forward `DML_DYNAMODB_TABLE` to containers.
- **Tests**: `tests/conftest.py`, `tests/contrib/test_executor_state.py`, `tests/contrib/test_executor_base.py` — swap DynamoDB moto fixtures for S3.
- **Docs**: `docs/contrib/executor-state.md`, `executor-catalog.md`, `execution-graph.md` — update infra requirements.
- **No public API change** for callers of `IndexOps`; the `ExecutionState` type is internal.
