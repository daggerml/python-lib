## REMOVED Requirements

### Requirement: Local indexes SHALL expose a durable cancellation tombstone lifecycle
**Reason**: Execution records in S3 now own cancellation and mutation lifecycle state, so a second local lifecycle authority on `Index` is no longer needed.
**Migration**: Remove `Index.lifecycle` persistence and route all mutation eligibility checks through `ExecutionState.require_mutation(...)`.

### Requirement: Mutating index workflows SHALL check local lifecycle inside each transaction
**Reason**: Mutating workflows now gate on execution lifecycle from S3 rather than on a local LMDB lifecycle tombstone.
**Migration**: Replace in-transaction local lifecycle checks with `ExecutionState.require_mutation(execution_id, db, mode=...)` before each write transaction boundary.

## ADDED Requirements

### Requirement: Mutating index workflows SHALL check execution lifecycle before each write transaction
Every mutating `IndexOps` workflow SHALL instantiate `ExecutionState`, use execution lifecycle state in S3 as the sole mutation authority, and call `ExecutionState.require_mutation(execution_id, db, mode=...)` before each LMDB write transaction that can change index or DAG state.

Execution-aware `IndexOps.create(cache_key, execution_id)` SHALL use `mode = "activation"`.

`put_literal`, `put_import`, `set_node_name`, `start_fn`, and `commit` SHALL use `mode = "mutation"`.

#### Scenario: Execution-aware create activates only from pending
- **WHEN** `IndexOps.create(cache_key="ck1", execution_id="e1")` begins activation
- **THEN** it SHALL call `ExecutionState.require_mutation("e1", db, mode="activation")` before creating local index state

#### Scenario: Mutating index op writes only from running
- **WHEN** `put_literal`, `put_import`, `set_node_name`, `start_fn`, or `commit` begins a write transaction for execution `e1`
- **THEN** that workflow SHALL call `ExecutionState.require_mutation("e1", db, mode="mutation")` before performing mutation work

#### Scenario: Multi-transaction mutation workflow rechecks execution lifecycle
- **WHEN** one mutating workflow performs more than one LMDB write transaction
- **THEN** it SHALL call `ExecutionState.require_mutation(...)` again before each later write transaction boundary

### Requirement: Local Index objects SHALL not own mutation lifecycle state
The system SHALL NOT rely on a persisted `Index.lifecycle` field to determine whether an execution can still be mutated. Local `Index` objects MAY still be read for structural commit and DAG state, but execution lifecycle ownership SHALL remain in the execution record.

#### Scenario: Mutation eligibility ignores local lifecycle tombstones
- **WHEN** a mutating workflow decides whether execution `e1` may continue
- **THEN** it uses the execution record for `e1` as the lifecycle authority
- **AND** it does not require a local `Index.lifecycle` value to make that decision
