## ADDED Requirements

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

#### Scenario: Single-transaction mutation checks local lifecycle in-txn
- **WHEN** `put_literal`, `put_import`, `set_node_name`, `start_fn`, or `commit` begins its mutating transaction
- **THEN** that workflow SHALL read the local index lifecycle from inside that transaction before performing mutation work

#### Scenario: Multi-transaction workflow rechecks at each transaction boundary
- **WHEN** one mutating workflow performs more than one LMDB transaction
- **THEN** it SHALL recheck local index lifecycle at the start of each transaction before continuing
