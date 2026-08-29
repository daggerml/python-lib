## ADDED Requirements

### Requirement: Replayable local writes grow the map until terminal outcome
The typed DB layer SHALL provide an internal `write_with_growth(fn)` operation for replayable local write functions. It SHALL execute `fn` in a write transaction and return its result only after that transaction commits.

#### Scenario: Map-full during a write operation
- **WHEN** `fn` or its transaction commit reports map-full before the configured maximum map size is reached
- **THEN** the DB layer SHALL abort the failed transaction, complete an explicit map resize, and rerun `fn` in a new write transaction

#### Scenario: Write requires multiple growth attempts
- **WHEN** a retried write again reports map-full and the map can still grow
- **THEN** the DB layer SHALL continue growing and rerunning `fn` until the write commits or no larger permitted map size remains

#### Scenario: Map growth uses persisted map size
- **WHEN** a growth-aware write reports map-full
- **THEN** native resize SHALL read LMDB's persisted map size
- **AND THEN** it SHALL grow by the configured headroom, capped at the configured maximum

#### Scenario: Map-full at configured maximum
- **WHEN** a write reports map-full and the current map is already at the configured maximum map size
- **THEN** the DB layer SHALL raise a terminal capacity error that identifies the database path, current map size, and configured maximum

### Requirement: Retried write functions are local and replayable
The `fn` passed to `write_with_growth` SHALL contain only deterministic local database work that is safe to repeat after its transaction aborts. Core mutation flows SHALL keep external side effects outside `fn`.

#### Scenario: Adapter-backed function start
- **WHEN** an adapter-backed function call needs local DAG writes and remote execution coordination
- **THEN** its local preparation and local DAG-attachment writes SHALL use `write_with_growth` separately from remote locking, cache updates, and adapter invocation

#### Scenario: Remote object materialization
- **WHEN** a remote object graph is materialized into the local database
- **THEN** the local object writes SHALL use `write_with_growth` without replaying remote fetch or publication effects
