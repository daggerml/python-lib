## Purpose

Enable interactive DAG workflows to durably freeze a live runtime for inspection and resume it later without creating a terminal DAG result.

## Requirements

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

### Requirement: Frozen runtime DAGs remain inspectable
The system SHALL expose a frozen runtime's partial DAG through existing read-only DAG and node inspection operations. A frozen runtime SHALL not report a terminal DAG result solely because it is frozen.

#### Scenario: Inspect named intermediate output
- **WHEN** a user reads the partial DAG of a frozen runtime containing a named `implementation` node
- **THEN** the user can resolve and inspect that node and its projections
- **AND** requesting the DAG terminal result reports that no terminal result exists

### Requirement: Runtime inspection includes frozen runtimes
The system SHALL include active and frozen runtimes in runtime list and describe operations. Frozen runtime inspection SHALL expose its state, partial DAG reference, and optional freeze message.

#### Scenario: List distinguishes frozen runtime
- **WHEN** a repository contains one active runtime and one frozen runtime
- **THEN** runtime listing includes both runtimes
- **AND** the frozen runtime entry identifies its frozen state and message

### Requirement: Frozen runtimes remain live execution roots
The system SHALL treat a frozen runtime as live for execution cancellation, execution-graph inspection, cache invalidation lineage, and local garbage-collection reachability.

#### Scenario: Cancel targets a frozen runtime
- **WHEN** a user cancels a frozen runtime
- **THEN** cancellation uses the frozen runtime's preserved execution ID
- **AND** it traverses that runtime's execution descendants as it would for the active form

#### Scenario: Garbage collection retains frozen runtime data
- **WHEN** local garbage collection runs while a frozen runtime exists
- **THEN** the frozen runtime and its partial DAG remain reachable
