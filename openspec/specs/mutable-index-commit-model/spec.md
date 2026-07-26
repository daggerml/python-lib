## ADDED Requirements

### Requirement: Index is a mutable commit model
The system SHALL model `Index` as a mutable subtype of `Commit` that carries the full commit-shaped history state plus the additional mutable DAG state needed during runtime staging.

#### Scenario: Index exposes commit-shaped fields
- **WHEN** internal runtime code reads an `Index` object
- **THEN** it can access the commit-shaped fields needed to finalize or merge that staged state without reconstructing a separate `Commit` shell

### Requirement: Index creation accepts commit-shaped base state
The system SHALL create new indexes from commit-shaped base state rather than from a bespoke head-only payload.

#### Scenario: Index starts from existing commit
- **WHEN** runtime staging starts from an existing branch or detached commit
- **THEN** the new `Index` records that base state as commit-shaped data and preserves current runtime behavior

#### Scenario: Index starts from explicit empty commit state
- **WHEN** a later workflow needs runtime staging without an existing head commit
- **THEN** index creation can be driven from explicit empty commit-shaped state without requiring a separate index-only model

### Requirement: Runtime commit flow semantics remain unchanged
The system SHALL preserve the current external runtime and history behavior while refactoring the internal model.

#### Scenario: Existing runtime commit flow remains stable
- **WHEN** current runtime workflows create an index, stage DAG work, and finalize it
- **THEN** they produce the same externally visible results as before this model refactor

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

### Requirement: DML runtime mutation entrypoints retry only CAS conflicts
Runtime mutation entrypoints on the DML surface (`put_literal`, `put_import`, `set_node_name`, `start_fn`, `commit`, and other methods decorated with the runtime-mutation retry wrapper) SHALL retry the full orchestration path only when the raised error is a remote CAS conflict (`CasItemConflict`). The DML-layer retry wrapper SHALL NOT retry `DmlDbMapFullError` or other LMDB/db-env failures; map-full recovery remains owned by the DB environment layer.

#### Scenario: Runtime commit retries full orchestration after CAS conflict
- **WHEN** `runtime.commit(...)` fails during post-index orchestration with `CasItemConflict`
- **THEN** the DML-layer retry wrapper re-runs the full commit orchestration path from the beginning
- **AND** a subsequent successful attempt returns the committed DAG ref

#### Scenario: Runtime commit does not retry map-full at the DML layer
- **WHEN** `runtime.commit(...)` fails during post-index orchestration with `DmlDbMapFullError`
- **THEN** the DML-layer retry wrapper does not re-run the orchestration path
- **AND** the error propagates to the caller
