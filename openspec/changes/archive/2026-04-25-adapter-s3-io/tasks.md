## 1. AdapterIO and ExecutionState

- [x] 1.1 Add `AdapterIO` class to `src/daggerml/_internal/exec_state.py` with `input_uri`, `output_uri` properties, `write_input(data: bytes) -> str`, and `read_output() -> bytes | None`
- [x] 1.2 Add `ExecutionState.adapter_io(exec_id: str, name: str) -> AdapterIO` factory method
- [x] 1.3 Add tests for `AdapterIO` path derivation, `write_input`, and `read_output`

## 2. Adapter CLI S3 Output Support

- [x] 2.1 Add S3 write support to `AdapterBase._write_output()` in `src/daggerml/contrib/adapters.py` (parallel to existing S3 read support in `_read_input`)
- [x] 2.2 Add tests for `_write_output` with an S3 URI

## 3. Migrate docker Executor

- [x] 3.1 Update `DockerExecutor.start()` in `src/daggerml/contrib/executors/docker.py` to use `AdapterIO.write_input()` for the sub-adapter payload and pass `io.input_uri` / `io.output_uri` to the container command instead of mounting a local tmpdir
- [x] 3.2 Update `DockerExecutor.poll()` to reconstruct `AdapterIO` and use `io.read_output()` instead of reading a local `output_path` from state
- [x] 3.3 Remove `workdir` and `output_path` from `DockerExecutor` state; make `_prepare_image` tmpdir ephemeral (created and removed within `start()`)
- [x] 3.4 Add or update tests for `DockerExecutor` covering S3-backed I/O and the simplified state shape

## 4. Migrate batch Executor

- [x] 4.1 Update `BatchExecutor.start()` in `src/daggerml/contrib/executors/batch.py` to use `AdapterIO.write_input()` for the sub-adapter payload and pass `io.input_uri` / `io.output_uri` to the Batch container command
- [x] 4.2 Update `BatchExecutor.poll()` to reconstruct `AdapterIO` from `(cache_key, execution_id, name)` and use `io.read_output()` instead of reading `output_uri` from state
- [x] 4.3 Remove `input_uri`, `output_uri`, and `S3Store.cd("jobs")` usage from the batch executor
- [x] 4.4 Add or update tests for `BatchExecutor` covering S3-backed I/O and the simplified state shape

## 5. Documentation

- [x] 5.1 Update `docs/contrib/executor-state.md` to document `AdapterIO`, the `fn-exec/io/` sub-namespace, and the `adapter_io()` factory
- [x] 5.2 Update `docs/contrib/executor-catalog.md` entries for `docker` and `batch` to reflect S3-backed I/O and simplified state
