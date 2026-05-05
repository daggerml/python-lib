## MODIFIED Requirements

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
