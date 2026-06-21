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

### Requirement: Local indexes SHALL expose a durable cancellation tombstone lifecycle
The system SHALL persist local `Index` objects with a local lifecycle that distinguishes mutable, canceling, and terminal-canceled states. The local lifecycle SHALL be `active`, `inactive`, or `canceled`.

`active` SHALL allow normal mutation. `inactive` SHALL mean cancellation is in progress and local mutators must not continue normal writes. `canceled` SHALL be the terminal local tombstone for a canceled index and SHALL remain available for late callers until separate cleanup removes it.

#### Scenario: F2 marks the local index inactive before adapter driving
- **WHEN** runtime cancellation begins `F2(ex0)` for a local index
- **THEN** the system SHALL persist that local index lifecycle as `inactive` before driving direct child cancellation

#### Scenario: Finished local cancellation leaves a tombstone
- **WHEN** the runtime reaches terminal local cancellation state for an index
- **THEN** the system SHALL persist that local index lifecycle as `canceled`

### Requirement: Mutating index workflows SHALL check local lifecycle inside each transaction
Every mutating index workflow SHALL check the local index lifecycle from inside the LMDB transaction that would perform the mutation. If one workflow spans multiple transactions, it SHALL perform that check at the start of each transaction.

Execution-aware `IndexOps.create(cache_key, execution_id)` SHALL also be treated as a mutation operation for cancellation and activation gating. Before it creates or mutates local index state, it SHALL read the existing execution record for `execution_id` and:

- proceed only when `lifecycle = "pending"`
- call `ExecutionState.cancel(execution_id, None, db, mode="drive")` and then raise `CancellationError` when `lifecycle = "cancel-pending"`
- raise `CancellationError` without local mutation when `lifecycle = "cancel-ready"` or `lifecycle = "canceled"`
- raise `DmlRepoError` without local mutation when the lifecycle is `running`, `succeeded`, or `failed`, or when the execution record is missing

#### Scenario: Single-transaction mutation checks local lifecycle in-txn
- **WHEN** `put_literal`, `put_import`, `set_node_name`, `start_fn`, or `commit` begins its mutating transaction
- **THEN** that workflow SHALL read the local index lifecycle from inside that transaction before performing mutation work

#### Scenario: Execution-aware create activates only from pending
- **WHEN** `IndexOps.create(cache_key="ck1", execution_id="e1")` begins activation
- **THEN** it SHALL create local index state only if `exec/state/e1.json` currently has `lifecycle = "pending"`

#### Scenario: Execution-aware create drives cancel-pending before failing
- **WHEN** `IndexOps.create(cache_key="ck1", execution_id="e1")` reads `exec/state/e1.json`
- **AND** the lifecycle is `cancel-pending`
- **THEN** it SHALL stop local activation work
- **AND** it SHALL call `ExecutionState.cancel("e1", None, db, mode="drive")`
- **AND** it SHALL raise `CancellationError`

#### Scenario: Execution-aware create raises on terminal cancel without drive
- **WHEN** `IndexOps.create(cache_key="ck1", execution_id="e1")` reads `exec/state/e1.json`
- **AND** the lifecycle is `cancel-ready` or `canceled`
- **THEN** it SHALL raise `CancellationError`
- **AND** it SHALL NOT create or mutate local index state

#### Scenario: Multi-transaction workflow rechecks at each transaction boundary
- **WHEN** one mutating workflow performs more than one LMDB transaction
- **THEN** it SHALL recheck local index lifecycle at the start of each transaction before continuing
