## ADDED Requirements

### Requirement: ExecutionState exposes adapter_io factory
`ExecutionState` SHALL provide an `adapter_io(exec_id: str, name: str) -> AdapterIO` method that returns a scoped `AdapterIO` instance for the given execution attempt and adapter/executor name.

#### Scenario: adapter_io returns AdapterIO with correct scope
- **WHEN** `ExecutionState(cache_key, remote_root=...).adapter_io(exec_id, name)` is called
- **THEN** the returned `AdapterIO` instance derives all paths from `(cache_key, exec_id, name)` under the `fn-exec/io/` sub-namespace

### Requirement: fn-exec/io/ sub-namespace is owned by ExecutionState
The system SHALL use `{fn-exec-prefix}/io/{cache_key}/{exec_id}/{name}/` as the standard S3 path for adapter I/O objects. This sub-namespace SHALL be owned by `ExecutionState`.

#### Scenario: Adapter I/O paths are within fn-exec/
- **WHEN** `AdapterIO` writes or derives any S3 key
- **THEN** all keys are prefixed with `{fn-exec-prefix}/io/`
