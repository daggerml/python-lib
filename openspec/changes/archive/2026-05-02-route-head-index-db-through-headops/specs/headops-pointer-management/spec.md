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
The system SHALL allow callers to pass an existing transaction into `HeadOps` public methods, and SHALL create a transaction internally when one is not provided.

#### Scenario: Caller provides transaction
- **WHEN** a caller invokes a `HeadOps` public method with `txn=`
- **THEN** the method performs its work within that transaction

#### Scenario: Caller omits transaction
- **WHEN** a caller invokes a `HeadOps` public method without `txn=`
- **THEN** the method creates and uses its own transaction
