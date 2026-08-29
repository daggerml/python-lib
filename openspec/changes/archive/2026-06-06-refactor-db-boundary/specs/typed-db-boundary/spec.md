## ADDED Requirements

### Requirement: Raw DB access is exposed only as a transactional context manager
The system SHALL expose raw DB access from `_internal/_db.pyx` only through a `dmldb(...)` context manager that opens an env and transaction together and yields one raw transaction object for the active context.

#### Scenario: Caller enters raw DB context
- **WHEN** application code enters `with dmldb(path, readonly=False, create=False, ...) as txn`
- **THEN** `_db.pyx` opens the env, opens one transaction in that env, and yields one raw transaction object for the duration of the context

#### Scenario: Caller exits raw DB context successfully
- **WHEN** the raw DB context exits without an exception
- **THEN** `_db.pyx` commits the active transaction and closes both the transaction and env before returning control

#### Scenario: Caller exits raw DB context with an exception
- **WHEN** the raw DB context exits because an exception was raised
- **THEN** `_db.pyx` aborts the active transaction and closes both the transaction and env before re-raising control to the caller

### Requirement: Raw DB layer fails closed on PID changes
The raw DB layer SHALL track the owning process ID for an active transaction and SHALL invalidate the active env/transaction immediately if the process ID changes.

#### Scenario: PID changes during active transaction
- **WHEN** a forked or otherwise different process attempts to use an active raw transaction
- **THEN** `_db.pyx` fails the operation and invalidates the active env/transaction instead of reopening or repairing it

### Requirement: Typed DB facade owns persistence validation
The system SHALL centralize persistence validation in `daggerml._internal.types.DmlDB.put` rather than relying on distributed per-type persistence validation hooks.

#### Scenario: Caller stores invalid graph object
- **WHEN** a caller attempts to store an object whose refs, namespaces, or graph shape violate DaggerML persistence invariants
- **THEN** `DmlDB.put` rejects the write before the raw DB layer persists the object

#### Scenario: Caller stores runnable value
- **WHEN** a caller stores a runnable value
- **THEN** `DmlDB.put` validates and persists that runnable through the unified typed DB facade without requiring a separate `RunnableDatum` storage wrapper

### Requirement: Typed DB facade is reusable but only active within its context manager
The system SHALL allow a `DmlDB` instance to be instantiated and re-entered multiple times, but typed DB operations SHALL only be valid while the `DmlDB` instance is inside an active context-manager scope.

#### Scenario: Re-enter reusable typed DB facade
- **WHEN** a caller reuses the same `DmlDB` instance in a later `with db:` block
- **THEN** the typed DB facade opens a fresh raw env+transaction for that entry and closes it on exit

#### Scenario: Caller uses typed DB facade outside active scope
- **WHEN** a caller invokes DB access methods on `DmlDB` while it is not inside an active context-manager scope
- **THEN** the typed DB facade rejects the operation instead of using a stale or implicit transaction

### Requirement: Typed DB facade provides shared typed helpers
The system SHALL expose typed DB helpers on `DmlDB`, including `get`, `put`, `require`, `exists`, `delete`, `iter`, `get_raw`, `put_raw`, `list_orphans`, `get_ctx`, and `run_with_resize`.

#### Scenario: Caller validates ref shape without fetching
- **WHEN** a caller invokes `DmlDB.require` with namespace expectations
- **THEN** the helper validates the ref shape and namespace constraints without reading from the DB

#### Scenario: Caller loads commit/tree/dag context
- **WHEN** a caller invokes `DmlDB.get_ctx(commit_ref)`
- **THEN** the helper returns the typed commit/tree/dag context associated with that commit ref

#### Scenario: Write path hits map-full error
- **WHEN** a write workflow is executed through `DmlDB.run_with_resize(...)` and the DB reports map-full
- **THEN** the helper resizes the map and retries the workflow until it succeeds or reaches its configured limit
