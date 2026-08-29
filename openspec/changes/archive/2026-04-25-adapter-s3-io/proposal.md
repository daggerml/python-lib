## Why

Fire-and-monitor executors launch sub-adapters in environments where stdin/stdout piping is not possible — the sub-adapter runs as a detached process (Docker container, AWS Batch job, etc.) and the executor cannot hold open a pipe to it. Currently there is no standard way to pass the input payload to the sub-adapter or receive its output result. The `batch` executor works around this with ad-hoc S3 logic that is not generalized, not scoped to `fn-exec/`, and broken on the output side. The `docker` executor works around it with a local tmpdir volume mount, which is fragile and carries unnecessary state. Both are instances of the same problem.

## What Changes

- Add an `AdapterIO` class to `exec_state.py` that provides a scoped S3-backed stdin/stdout surrogate for a specific `(cache_key, exec_id, name)` triple.
- Add `ExecutionState.adapter_io(exec_id, name)` factory method returning an `AdapterIO` instance.
- Add S3 write support to `AdapterBase._write_output()` so sub-adapters running inside remote environments can write their result to an S3 URI passed via `-o`.
- Migrate the `docker` executor to use `AdapterIO`, replacing the local tmpdir volume mount approach and removing `workdir` and `output_path` from its state.
- Migrate the `batch` executor to use `AdapterIO` instead of its current ad-hoc S3 I/O logic, removing `input_uri` and `output_uri` from its state.
- Update `docs/contrib/executor-state.md` to document `AdapterIO`.

Among current executors, `docker` and `batch` are the two that use `AdapterIO`. It is the intended standard pattern for any future fire-and-monitor executor (EMR, Glue, ECS, SageMaker, etc.).

## Capabilities

### New Capabilities

- `adapter-s3-io`: S3-backed stdin/stdout surrogate (`AdapterIO`) for fire-and-monitor executors that cannot pipe data to/from a sub-adapter process directly.

### Modified Capabilities

- `execution-state`: `ExecutionState` gains a new `adapter_io()` factory method; the `fn-exec/io/` sub-namespace is added to the S3 layout it owns.

## Impact

- `src/daggerml/_internal/exec_state.py` — new `AdapterIO` class, new `ExecutionState.adapter_io()` method.
- `src/daggerml/contrib/adapters.py` — `_write_output()` gains S3 URI support.
- `src/daggerml/contrib/executors/docker.py` — migrated to `AdapterIO`; `workdir`, `output_path`, and tmpdir machinery removed from state and cleanup.
- `src/daggerml/contrib/executors/batch.py` — migrated to `AdapterIO`; `input_uri`, `output_uri`, and `S3Store.cd("jobs")` usage removed from state.
- `docs/contrib/executor-state.md` — updated to cover `AdapterIO` and `fn-exec/io/` namespace.
