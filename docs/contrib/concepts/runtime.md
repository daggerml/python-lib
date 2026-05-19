# Runtime Model

Contrib runtime execution is split between adapters, executors, and runtime-owned state.

## Adapters choose the boundary

An adapter is the outer transport boundary. The built-in adapters are:

- `local`: dispatches to a local executor by calling its `handle(...)` path.
- `lambda`: sends the runtime payload to an AWS Lambda function and expects canonical JSON back.

Adapters are responsible for:

- parsing or emitting the adapter payload,
- selecting the concrete executor target,
- performing one bounded runtime step,
- returning one of the canonical result shapes.

The canonical statuses are:

- `running`
- `succeeded`
- `failed`
- `cancel-detached`

## Executors own execution behavior

Executors implement the actual backend behavior after adapter routing.

Built-in executors in this repo are:

- `script`: runs serialized Python through the contrib supervisor.
- `docker`: runs a nested adapter inside a Docker container.
- `ssh`: runs a nested adapter synchronously over SSH.
- `batch`: submits a nested adapter run to AWS Batch through Lambda.
- `cfn`: creates or updates an AWS CloudFormation stack and turns terminal outputs back into a DAG result.

Some executors are synchronous in practice, but the runtime still treats execution in terms of `start`, `poll`, and `cleanup` behavior.

## State belongs to the runtime, not the executor

The runtime coordinates resumable work around two identifiers:

- `cache_key`: identifies the computation and the active execution slot.
- `execution_id`: identifies one execution attempt.

Runtime-owned state lives under the configured remote root. The main pieces are:

- an advisory mutex for a `cache_key`,
- an active execution pointer for a `cache_key`,
- launch state for a specific `execution_id`,
- execution records for a specific `execution_id`.

Executors return durable launch-time state in the first `running` result, and later polls resume from that immutable state.

## Fire-and-monitor backends use S3 handoff

`docker` and `batch` cannot rely on direct stdin and stdout piping once the child process is remote or detached. They use `ExecutionState.adapter_io(...)` to derive stable S3 input and output locations from `(cache_key, execution_id, name)`.

That lets `start(...)` and `poll(...)` agree on the same payload locations without storing those URIs in executor state.

## Script execution uses a supervisor

The `script` executor does not run your serialized function directly in the launching process.

Instead it:

- writes a supervisor payload,
- starts `python -m daggerml.contrib.supervisor` in a detached process,
- lets the supervisor create an isolated repo and workdir,
- runs the script worker there,
- collects `result.json`, `stdout.log`, and `stderr.log`.

The supervisor also streams worker stdout and stderr to CloudWatch on a best-effort basis while preserving the local log files.

## Registries keep the system open-ended

Adapters and executors are discovered from Python entry points as well as runtime registration calls.

- Adapter entry point group: `daggerml.contrib.adapters`
- Executor entry point group: `daggerml.contrib.executors`

That is why contrib can expose a small built-in catalog while still acting as a plugin surface.

Next: [Run workloads outside the local process](../guides/run-workloads-outside-the-local-process.md)
