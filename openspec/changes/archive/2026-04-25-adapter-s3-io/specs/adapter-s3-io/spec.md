## ADDED Requirements

### Requirement: AdapterIO provides scoped S3 stdin/stdout surrogate
The system SHALL provide an `AdapterIO` class that acts as an S3-backed surrogate for stdin/stdout between a fire-and-monitor adapter and the sub-adapter it launches. `AdapterIO` SHALL be constructed only via `ExecutionState.adapter_io(exec_id, name)` and SHALL scope all S3 paths under `{fn-exec-prefix}/io/{cache_key}/{exec_id}/{name}/`.

#### Scenario: Paths scoped correctly
- **WHEN** `state.adapter_io("exec-uuid", "lambda:batch")` is called on an `ExecutionState` with `cache_key="ck"` and `remote_root="s3://bucket/pfx"`
- **THEN** `input_uri` is `s3://bucket/pfx/fn-exec/io/ck/exec-uuid/lambda:batch/input.json` and `output_uri` is `s3://bucket/pfx/fn-exec/io/ck/exec-uuid/lambda:batch/output.json`

### Requirement: input_uri and output_uri are pure derivations
`AdapterIO.input_uri` and `AdapterIO.output_uri` SHALL be properties that return S3 URIs without performing any S3 operation.

#### Scenario: No S3 call on property access
- **WHEN** `io.input_uri` or `io.output_uri` is accessed
- **THEN** no S3 API call is made

### Requirement: write_input writes payload and returns input URI
`AdapterIO.write_input(data: bytes)` SHALL PUT `data` to the input S3 key and return `input_uri`.

#### Scenario: write_input stores payload at input key
- **WHEN** `io.write_input(b'{"payload": 1}')` is called
- **THEN** the bytes are written to the input S3 key and `input_uri` is returned

### Requirement: read_output returns output bytes or None
`AdapterIO.read_output()` SHALL GET the output S3 key and return the raw bytes. If the object does not yet exist, it SHALL return `None` without raising.

#### Scenario: read_output returns None when not yet written
- **WHEN** `io.read_output()` is called before the sub-adapter has written output
- **THEN** `None` is returned

#### Scenario: read_output returns bytes when written
- **WHEN** the sub-adapter has written its result to the output S3 key and `io.read_output()` is called
- **THEN** the raw bytes are returned

### Requirement: name is caller-defined with a conventional format
The `name` parameter passed to `ExecutionState.adapter_io()` SHALL be chosen by the caller. Built-in executors SHALL use the convention `"{adapter-shorthand}:{executor-name}"` (e.g. `"local:docker"`, `"lambda:batch"`). `AdapterIO` SHALL NOT validate or interpret the `name` value.

#### Scenario: name is incorporated into S3 path verbatim
- **WHEN** `state.adapter_io(exec_id, "local:docker")` is called
- **THEN** the resulting paths contain `local:docker` as a path component under `fn-exec/io/{cache_key}/{exec_id}/`

### Requirement: AdapterIO is only for fire-and-monitor executors
`AdapterIO` SHALL only be used by executors that launch a sub-adapter as a detached process where direct stdin/stdout piping is not possible. Among current executors, `docker` and `batch` use `AdapterIO`. Executors that can pipe stdin/stdout directly (`script`, `ssh`) SHALL NOT use `AdapterIO`. `cfn` has no sub-adapter and SHALL NOT use `AdapterIO`.

#### Scenario: docker executor uses AdapterIO
- **WHEN** `DockerExecutor.start()` is called
- **THEN** it uses `AdapterIO.write_input()` to write the sub-adapter payload and passes `io.input_uri` and `io.output_uri` to the container command; no local tmpdir is created for I/O

#### Scenario: batch executor uses AdapterIO
- **WHEN** `BatchExecutor.start()` is called
- **THEN** it uses `AdapterIO.write_input()` to write the sub-adapter payload and passes `io.input_uri` and `io.output_uri` to the Batch container command

### Requirement: Sub-adapter output written via _write_output S3 support
`AdapterBase._write_output` SHALL support S3 URIs as the output path, writing the result payload directly to the specified S3 key via `put_object`.

#### Scenario: Sub-adapter writes to S3 output URI
- **WHEN** the adapter CLI is invoked with `-o s3://bucket/key` and execution completes
- **THEN** the result JSON is written to `s3://bucket/key`
