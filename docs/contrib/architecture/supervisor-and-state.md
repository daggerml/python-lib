# Supervisor And State

Two pieces make contrib runtime execution resumable in practice: the script supervisor and runtime-owned remote state.

## Script supervisor

The `script` executor starts `python -m daggerml.contrib.supervisor` in a detached process.

The supervisor then:

- validates the payload,
- creates a temporary workdir,
- initializes a worker repo under that workdir,
- launches the actual worker command,
- collects `stdout.log`, `stderr.log`, and `result.json`,
- streams stdout and stderr to CloudWatch best-effort.

The worker result must be terminal after process exit:

- success requires `status`, `error`, and a real `dag_id`,
- failure requires `status` and `error`,
- non-terminal worker results are rejected once the worker has exited.

## Runtime-owned state model

Contrib no longer treats executor instances as the owners of live state. The runtime stores execution coordination under the configured remote root.

The current model revolves around:

- a mutex per `cache_key`,
- an active execution pointer for the current `cache_key`,
- launch state for a specific `execution_id`,
- execution records for lifecycle and cancellation tracking.

This is why later polls can resume with immutable launch-time state instead of depending on the launching process to stay alive.

## `ExecutionState.adapter_io(...)`

Detached backends such as Docker and Batch need stable transport locations for a nested adapter payload.

`adapter_io(exec_id, name)` derives those locations from:

- `cache_key`
- `execution_id`
- a caller-chosen name such as `local:docker` or `lambda:batch`

The executor can then:

- write the nested input once,
- pass the input and output URIs to the remote worker,
- reconstruct the same paths during `poll(...)` without storing them in executor state.

## Why this split matters

- The supervisor isolates author code from the launcher process.
- Remote state keeps detached backends resumable and deduplicated.
- Centralized runtime ownership prevents adapters and executors from publishing terminal cache state independently.

See also: [execution flow](execution-flow.md)
