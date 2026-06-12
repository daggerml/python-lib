## ADDED Requirements

### Requirement: DB handle operations SHALL recover from fork invalidation
The system SHALL transparently replace an inherited DB handle and retry once when a handle-level DB operation receives a fork-invalidated return code from the C DB layer.

Handle-level operations include DB size inspection, resize, transaction open, and other operations that act on `DmlDb._handle` before a transaction object is established.

#### Scenario: Child process opens a transaction with an inherited DB facade
- **WHEN** a child process calls `tx()` on a `DmlDb` or typed `DmlDB` facade inherited across `fork()`
- **THEN** the system replaces the stale DB handle automatically
- **AND** retries transaction open once
- **AND** yields a working transaction without requiring the caller to construct a new DB facade

#### Scenario: Child process resizes using an inherited DB facade
- **WHEN** a child process invokes a handle-level resize-related DB operation on an inherited DB facade after `fork()`
- **THEN** the system replaces the stale DB handle automatically
- **AND** retries the handle-level operation once
- **AND** does not surface a handle-level fork error if reopen succeeds

### Requirement: Inherited transaction objects SHALL remain invalid after fork
The system SHALL treat transaction objects created before `fork()` as invalid in the child process and SHALL NOT transparently recreate them.

#### Scenario: Child process reuses an inherited transaction object
- **WHEN** a transaction object opened in the parent process is used in the child process after `fork()`
- **THEN** the operation fails with a transaction-level fork error
- **AND** the system does not silently substitute a different transaction object

### Requirement: DB Python APIs SHALL not expose raw payload read or write helpers
The system SHALL not expose Python-level raw payload `get` or `put` operations from the DB transaction surfaces.

#### Scenario: Typed transaction surface exposes only typed object reads and writes
- **WHEN** callers use the supported Python DB transaction APIs
- **THEN** reads decode persisted payloads through normal typed object handling
- **AND** writes persist values through normal typed object serialization
- **AND** no supported `get_raw`, `put_raw`, or `raw=True` transaction API is required
