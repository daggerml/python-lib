## MODIFIED Requirements

### Requirement: Environments are leased per active operation
The DB layer SHALL treat LMDB environments as leased resources for active transactions and other short-lived env users, incrementing a slot refcount on acquisition and decrementing it on release. During an explicit resize for a canonical path, new acquisitions for that slot SHALL wait until the resize succeeds or fails; existing leases SHALL remain valid until their holders release them.

#### Scenario: Transaction open acquires an env lease
- **WHEN** a transaction is opened for a registry slot with no active env and no resize is pending
- **THEN** the DB layer opens an env for that slot and increments the slot refcount before beginning the transaction

#### Scenario: Transaction close releases the final env lease
- **WHEN** the last active transaction for a registry slot closes
- **THEN** the DB layer decrements the slot refcount to zero and closes the slot env

#### Scenario: Transaction request waits for pending resize
- **WHEN** a caller requests a transaction for a canonical path whose registry slot is resizing
- **THEN** the request SHALL wait until the resize completes or fails
- **AND THEN** it SHALL resume normal environment acquisition

#### Scenario: Resize fails
- **WHEN** an explicit resize cannot reopen the environment at its requested map size
- **THEN** the resize requester SHALL receive the open failure
- **AND THEN** later transaction requests SHALL be allowed to perform normal environment acquisition

#### Scenario: Existing transaction completes during resize
- **WHEN** an explicit resize begins while a transaction already holds an environment lease
- **THEN** the existing transaction SHALL remain valid until it closes
- **AND THEN** the resize SHALL wait for that lease to release before reopening the environment

### Requirement: Map-full recovery reopens with a larger map size
The DB layer SHALL recover from map-full conditions for growth-aware writes by closing the failed transaction, completing an explicit blocking resize at a larger map size, and retrying the write. The resize operation SHALL be distinct from ordinary transaction open: a `map_size` supplied while an environment is already open SHALL remain ignored.

#### Scenario: Map-full retries after explicit resize
- **WHEN** a growth-aware write fails because the current env map is full
- **THEN** the DB layer SHALL reopen the env at a larger map size before retrying the write

#### Scenario: Ordinary transaction open does not trigger resize
- **WHEN** a caller opens a transaction with a map size while an environment for the canonical path is already open
- **THEN** the DB layer SHALL reuse the open environment without resizing it

### Requirement: Registry adopts a map resized by another process
The DB layer SHALL recover transaction acquisition when LMDB reports that another process resized the backing map by draining local leases, reopening the local environment at the backing map's current size, and retrying acquisition once.

#### Scenario: Reader observes external map resize
- **WHEN** a transaction acquisition observes LMDB's map-resized condition after another process grows the same database
- **THEN** the DB layer SHALL reopen the local environment at the current backing map size and yield a working transaction if the reopen succeeds
