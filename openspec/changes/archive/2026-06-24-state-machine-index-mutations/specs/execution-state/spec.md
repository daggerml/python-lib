## ADDED Requirements

### Requirement: ExecutionState SHALL expose a public mutation lifecycle guard
The runtime SHALL expose `ExecutionState.require_mutation(execution_id, db, mode="activation" | "mutation")` as the canonical public guard for mutation eligibility. The guard SHALL read `exec/state/<execution_id>.json`, classify the stored lifecycle for the requested mode, and either return the execution record unchanged or raise a typed execution-status error.

For `mode = "activation"`, only `lifecycle = "pending"` SHALL be accepted.

For `mode = "mutation"`, only `lifecycle = "running"` SHALL be accepted.

If the lifecycle is `cancel-pending`, the guard SHALL call `cancel(execution_id, None, db, mode="drive")` before raising `CanceledExecutionError`.

If the lifecycle is `cancel-ready` or `canceled`, the guard SHALL raise `CanceledExecutionError` without driving cancellation.

If the lifecycle is any other non-accepted value for the requested mode, the guard SHALL raise `BadExecutionStatusError`.

#### Scenario: Activation guard accepts pending execution
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `pending`
- **THEN** it returns the stored execution record without mutation

#### Scenario: Mutation guard accepts running execution
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `running`
- **THEN** it returns the stored execution record without mutation

#### Scenario: Activation guard rejects non-pending non-cancel states
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `running`, `succeeded`, or `failed`
- **THEN** it raises `BadExecutionStatusError`

#### Scenario: Mutation guard rejects non-running non-cancel states
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `pending`, `succeeded`, or `failed`
- **THEN** it raises `BadExecutionStatusError`

#### Scenario: Cancel-pending drives cancellation before raising
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` or `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `cancel-pending`
- **THEN** it calls `ExecutionState.cancel("e1", None, db, mode="drive")`
- **AND** it raises `CanceledExecutionError`

#### Scenario: Terminal cancel states raise without driving
- **WHEN** `ExecutionState.require_mutation("e1", db, mode="activation")` or `ExecutionState.require_mutation("e1", db, mode="mutation")` reads `exec/state/e1.json`
- **AND** the lifecycle is `cancel-ready` or `canceled`
- **THEN** it raises `CanceledExecutionError`
