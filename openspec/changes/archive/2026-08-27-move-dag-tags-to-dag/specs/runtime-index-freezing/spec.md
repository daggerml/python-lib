## MODIFIED Requirements

### Requirement: User runtimes can be frozen and unfrozen
The system SHALL allow a user-owned runtime index to transition to a frozen form with an optional message, and SHALL allow that frozen form to transition back to an active index. Both transitions SHALL preserve the runtime's ID, parent history state, partial DAG reference, and intrinsic DAG tags.

#### Scenario: Freeze preserves partial DAG identity
- **WHEN** a user freezes an active runtime after it records an `implementation` node
- **THEN** the frozen runtime has the same ID and partial DAG reference as the active runtime
- **AND** it stores the supplied optional message
- **AND** the partial DAG retains its tags

#### Scenario: Unfreeze is the inverse transition
- **WHEN** a user unfreezes a frozen runtime
- **THEN** the resulting active runtime has the same ID, parent history state, partial DAG reference, and tags as the frozen runtime

#### Scenario: Function runtime cannot be frozen
- **WHEN** a freeze request targets a runtime created for an adapter-coordinated function execution
- **THEN** the system SHALL reject the request
- **AND** it SHALL leave the runtime unchanged
