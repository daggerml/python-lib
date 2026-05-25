# Runtime Surfaces

## Built-in adapters

| Adapter | Registry name | Executable | Behavior |
| --- | --- | --- | --- |
| Local adapter | `local` | `dml-local-adapter` | Calls a local executor's `handle(...)` method. |
| Lambda adapter | `lambda` | `dml-lambda-adapter` | Invokes an AWS Lambda function and validates canonical JSON output. |

All adapters exchange the same logical payload fields:

- `runnable`
- `argv_ptr`
- `cache_key`
- `execution_id`
- `remote`
- `state`
- `execution_status`
- `cancel_requested_by`

Canonical adapter results are:

- `{"status": "running", "error": null, "state": {...}}`
- `{"status": "succeeded", "error": null, "dag_id": "<hex>"}`
- `{"status": "failed", "error": "..."}`
- `{"status": "cancelled", "error": null}`

## Built-in executors

### `script`

- Adapter: `local`
- Requires `sub is None`
- Accepted kwargs: `fn`, `prepop`, `extra_objs`, `extra_lines`
- Serializes the callable source into S3 and runs it through the contrib supervisor

### `docker`

- Adapter: `local`
- Requires a nested `sub` runnable
- Accepted kwargs: `image`, optional `flags`
- Starts a detached container, then polls container state and reads the nested adapter result from S3

### `ssh`

- Adapter: `local`
- Requires a nested `sub` runnable
- Accepted kwargs: `host`, optional `flags`, optional `env_files`
- Runs the nested adapter synchronously over SSH

### `batch`

- Adapter: `lambda`
- Requires a nested `sub` runnable
- Accepted kwargs: `lambda_uri`, `image`, optional `cpu`, optional `memory`, optional `gpu`
- Uses Lambda as the adapter boundary and AWS Batch as the executor backend

### `cfn`

- Adapter: `local`
- Accepts CloudFormation-oriented stack data
- Creates or updates a stack, then commits outputs back into a DAG result

## Registries and plugin discovery

Contrib keeps adapters and executors in separate registries.

| Registry | Module | Entry point group | Lookup key |
| --- | --- | --- | --- |
| Adapter registry | `daggerml.contrib.adapter_registry` | `daggerml.contrib.adapters` | adapter name |
| Executor registry | `daggerml.contrib.executor_registry` | `daggerml.contrib.executors` | `(adapter, executor)` |

Plugin entry points may return:

- one registration object,
- an iterable of registration objects,
- or a callable that returns either of those.

Registration objects are validated before being accepted.

## `status()` report

`daggerml.contrib.status.status()` returns one JSON-safe snapshot of:

- effective adapters,
- effective executors,
- registered codecs,
- duplicate-key warnings,
- plugin load or validation failures.

Its top-level schema is currently:

```python
{
    "schema_version": 0,
    "summary": {...},
    "adapters": [...],
    "executors": [...],
    "codecs": [...],
    "diagnostics": [...],
}
```

The report is meant for structured introspection, not for human-friendly formatting.

## Execution-state helpers

Contrib runtime state is coordinated outside the adapter and executor objects themselves.

Important pieces:

- the runtime acquires a mutex per `cache_key`,
- launch state and execution records are stored under the remote root,
- `ExecutionState.adapter_io(...)` derives stable S3 input and output paths for detached backends like Docker and Batch.

For the internal flow behind those helpers, see [../architecture/supervisor-and-state.md](../architecture/supervisor-and-state.md).
