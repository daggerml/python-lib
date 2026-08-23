## MODIFIED Requirements

### Requirement: Replayable local writes grow the map until terminal outcome
The typed DB layer SHALL provide `write_with_growth(fn)` as the sole growth-aware write operation for replayable local write functions. It SHALL execute `fn` in a write transaction and return its result only after that transaction commits. The DB layer SHALL NOT expose a `call_with_resize` alias or another compatibility name for this operation.

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

#### Scenario: Removed resize alias is unavailable
- **WHEN** a caller inspects the supported typed or raw DB facade
- **THEN** `write_with_growth` is the only growth-aware write entry point
- **AND** `call_with_resize` is not exposed
