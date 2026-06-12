## MODIFIED Requirements

### Requirement: HeadOps public methods support caller-owned transactions
The system SHALL remove constructor-owned DB state from `HeadOps`. `HeadOps` public methods that need database access SHALL accept an explicit `db` argument, and pointer-management methods that only operate on `.dml/refs/**` files SHALL operate without opening or capturing DB state.

#### Scenario: Caller performs bootstrap branch creation
- **WHEN** a caller invokes the `HeadOps` branch-creation workflow that must create the initial commit state
- **THEN** the workflow uses the explicit caller-provided `db` context for DB writes
- **AND** it does not rely on constructor-injected DB state

#### Scenario: Caller invokes non-bootstrap pointer method
- **WHEN** a caller invokes a `HeadOps` pointer lookup, listing, update, create-index, or delete-index method that only needs `.dml/refs/**` file I/O
- **THEN** the method performs only file I/O and stale-write checks without requiring hidden constructor-owned DB state

#### Scenario: Caller resolves project-specific pointer path
- **WHEN** a `HeadOps` method needs repository pointer paths
- **THEN** the caller provides project context explicitly rather than relying on `HeadOps` to recover project layout from an injected DB handle
