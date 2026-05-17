## ADDED Requirements

### Requirement: Direct user cancellation SHALL use configured user identity
When `dml.runtime.cancel(index_id)` is invoked without an active runtime execution context, the workflow SHALL still proceed as an out-of-band cancellation operation. In that case, the runtime SHALL record `cancellation_requested_by` from the configured user identity.

#### Scenario: User-triggered cancel records configured user without active execution
- **WHEN** a user directly invokes `dml.runtime.cancel("idx1")`
- **AND** there is no active caller `execution_id`
- **THEN** the runtime SHALL set `cancellation_requested_by` to `config.user`

#### Scenario: Missing configured user still fails cancel
- **WHEN** a user invokes `dml.runtime.cancel("idx1")`
- **AND** no configured user identity is available
- **THEN** the runtime SHALL fail the request rather than persisting an empty cancellation requester

### Requirement: Runtime cancellation SHALL be out-of-band control-plane behavior
`dml.runtime.cancel(index_id)` SHALL operate as an out-of-band control-plane workflow rather than as a continuation of a running execution. The workflow SHALL freeze the target index, remove caller-owned live edges, orphan eligible callees, and request detached cancellation without requiring an active caller execution context.

#### Scenario: Direct cancel freezes index before cancellation traversal
- **WHEN** a user invokes `dml.runtime.cancel("idx1")`
- **THEN** the runtime SHALL freeze the index before removing live caller edges or requesting callee cancellation
