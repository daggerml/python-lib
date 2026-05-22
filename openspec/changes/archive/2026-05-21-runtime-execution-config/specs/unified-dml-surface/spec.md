## MODIFIED Requirements

### Requirement: Shared `Dml` constructor uses root runtime override inputs
The shared `Dml` constructor SHALL accept the root runtime override inputs already threaded through callers for project-home, remote-uri, user, config-home, and execution identity context.

#### Scenario: Execution-aware worker maps execution identity directly to constructor
- **WHEN** an execution-aware worker or adapter entrypoint has an `execution_id`
- **THEN** it can pass that value directly to the shared `Dml` constructor as a runtime override
- **AND** it does not need a separate ambient execution-context setup step

### Requirement: `Dml` delegates repository behavior to the relevant ops classes
The shared `Dml` class SHALL orchestrate workflows by delegating repository actions to the relevant subsystem ops classes rather than re-implementing those mechanics inline. Module-level helper functions in `daggerml._internal.dml` SHALL construct the owning concrete ops classes directly and SHALL NOT route calls through a facade object or string-dispatch proxy layer.

#### Scenario: Runtime workflow passes explicit execution identity to IndexOps
- **WHEN** a shared `Dml` runtime workflow needs execution-aware behavior such as runnable DAG publication or nested execution lineage
- **THEN** the `Dml` runtime layer passes explicit execution identity into `IndexOps`
- **AND** `IndexOps` does not read that identity from a process-local ambient execution context

#### Scenario: Runtime start_fn falls back to root index identity
- **WHEN** `dml.runtime.start_fn(index_id, ...)` runs without resolved `config.execution.id`
- **THEN** the runtime layer passes `caller_execution_id = index_id`
- **AND** `IndexOps.start_fn` treats that root execution record as the caller identity
