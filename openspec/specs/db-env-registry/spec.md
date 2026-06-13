## ADDED Requirements

### Requirement: Canonical-path registry deduplicates same-process DB access
The DB layer SHALL canonicalize each requested DB path and use a process-local registry so all callers targeting the same canonical path reuse the same registry slot.

#### Scenario: Same-path callers reuse one slot
- **WHEN** two DB handles are opened in the same process for paths that canonicalize to the same on-disk DB location
- **THEN** the DB layer assigns both handles to the same registry slot

#### Scenario: Different paths use different slots
- **WHEN** two DB handles are opened in the same process for paths that canonicalize to different on-disk DB locations
- **THEN** the DB layer assigns them to different registry slots

### Requirement: Registry invalidates inherited state on PID change
The DB layer SHALL store the active PID on the registry and clear all registry slots before further env acquisition when the PID does not match the current process.

#### Scenario: Child process clears inherited registry state
- **WHEN** a process fork occurs and the child attempts to acquire a DB env through the inherited registry
- **THEN** the child clears the inherited registry state and continues with a fresh registry PID

### Requirement: Environments are leased per active operation
The DB layer SHALL treat LMDB environments as leased resources for active transactions and other short-lived env users, incrementing a slot refcount on acquisition and decrementing it on release.

#### Scenario: Transaction open acquires an env lease
- **WHEN** a transaction is opened for a registry slot with no active env
- **THEN** the DB layer opens an env for that slot and increments the slot refcount before beginning the transaction

#### Scenario: Transaction close releases the final env lease
- **WHEN** the last active transaction for a registry slot closes
- **THEN** the DB layer decrements the slot refcount to zero and closes the slot env

### Requirement: Map-full recovery reopens with a larger map size
The DB layer SHALL recover from map-full conditions by closing the failed transaction, releasing the current env lease, reopening the slot env with a larger map size, and retrying the operation.

#### Scenario: Map-full retries without live resize
- **WHEN** a write operation fails because the current env map is full
- **THEN** the DB layer reopens the env at a larger map size before retrying the write

### Requirement: Registry capacity is bounded and explicit
The DB layer SHALL enforce a fixed maximum number of distinct canonical DB paths in the registry at one time and fail with a dedicated registry-capacity error when no slot is available.

#### Scenario: Registry-full returns a dedicated error
- **WHEN** a caller opens a DB handle for a new canonical path and all registry slots are already occupied by different paths
- **THEN** the DB layer fails with a dedicated registry-capacity error
