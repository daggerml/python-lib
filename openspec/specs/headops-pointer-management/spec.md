## ADDED Requirements

### Requirement: HeadOps owns branch and index pointer persistence
The system SHALL route all branch and index storage creation, lookup, update, listing, and deletion through `HeadOps` public methods.

#### Scenario: Non-HeadOps caller needs branch commit
- **WHEN** an internal caller needs the commit for a branch
- **THEN** it obtains that commit through a `HeadOps` public method instead of reading a `Head` object or head ref directly

#### Scenario: Non-HeadOps caller needs index commit
- **WHEN** an internal caller needs the commit for an index
- **THEN** it obtains that commit through a `HeadOps` public method instead of reading an `Index` object or index ref directly

### Requirement: HeadOps hides head and index refs from callers
The system SHALL keep branch and index refs internal to `HeadOps` and SHALL expose branch names, opaque index ids, and commit refs to non-`HeadOps` callers.

#### Scenario: Branch-targeted workflow uses branch name
- **WHEN** an internal caller targets a branch
- **THEN** the caller interacts with `HeadOps` using the branch name rather than a head ref

#### Scenario: Index-targeted workflow uses opaque index id
- **WHEN** an internal caller targets an index
- **THEN** the caller interacts with `HeadOps` using an opaque index id rather than an index ref

### Requirement: HeadOps supports atomic commit updates for pointers
The system SHALL update branch and index commits through `update_branch_commit` and `update_index_commit` methods that require the caller to provide the expected current commit.

#### Scenario: Expected commit matches
- **WHEN** a caller requests a branch or index commit update with the correct current commit
- **THEN** `HeadOps` stores the new commit atomically

#### Scenario: Expected commit is stale
- **WHEN** a caller requests a branch or index commit update with an outdated current commit
- **THEN** `HeadOps` rejects the update and raises a dedicated conflict error

### Requirement: Conflict error reports current commit for retries
The system SHALL raise a dedicated `DmlRepoError` subclass for stale branch/index updates, and that exception SHALL expose the correct `current_commit`.

#### Scenario: Caller retries after stale index update
- **WHEN** `update_index_commit` fails because the stored commit changed
- **THEN** the raised conflict error includes the current stored commit for the caller to inspect and retry from

### Requirement: HeadOps public methods support caller-owned transactions
The system SHALL keep transaction-aware behavior limited to `create_branch`, and all other public `HeadOps` pointer-management methods SHALL operate without caller-owned transactions.

#### Scenario: Caller provides transaction to create_branch
- **WHEN** a caller invokes `create_branch(..., txn=...)`
- **THEN** `HeadOps` uses that transaction only for bootstrap commit creation, closes it only if `HeadOps` opened it, and does not create the branch file until the transaction that created the commit has been closed successfully

#### Scenario: Caller invokes non-bootstrap pointer method
- **WHEN** a caller invokes any `HeadOps` pointer lookup, listing, update, create-index, or delete-index method other than `create_branch`
- **THEN** the method performs only `.dml/refs/**` file I/O and stale-write checks without accepting or requiring a transaction or validating commit existence in LMDB

#### Scenario: Index deletion remains plain HeadOps cleanup
- **WHEN** a caller asks `HeadOps` to delete an index ref
- **THEN** `HeadOps` removes the index file as a plain file operation and does not require compare-and-delete semantics
