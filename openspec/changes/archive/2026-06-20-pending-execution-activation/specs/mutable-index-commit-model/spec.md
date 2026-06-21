## MODIFIED Requirements

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
