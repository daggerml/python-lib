## ADDED Requirements

### Requirement: Index is a mutable commit model
The system SHALL model `Index` as a mutable subtype of `Commit` that carries the full commit-shaped history state plus the additional mutable DAG state needed during runtime staging.

#### Scenario: Index exposes commit-shaped fields
- **WHEN** internal runtime code reads an `Index` object
- **THEN** it can access the commit-shaped fields needed to finalize or merge that staged state without reconstructing a separate `Commit` shell

### Requirement: Index creation accepts commit-shaped base state
The system SHALL create new indexes from commit-shaped base state rather than from a bespoke head-only payload.

#### Scenario: Index starts from existing commit
- **WHEN** runtime staging starts from an existing branch or detached commit
- **THEN** the new `Index` records that base state as commit-shaped data and preserves current runtime behavior

#### Scenario: Index starts from explicit empty commit state
- **WHEN** a later workflow needs runtime staging without an existing head commit
- **THEN** index creation can be driven from explicit empty commit-shaped state without requiring a separate index-only model

### Requirement: Runtime commit flow semantics remain unchanged
The system SHALL preserve the current external runtime and history behavior while refactoring the internal model.

#### Scenario: Existing runtime commit flow remains stable
- **WHEN** current runtime workflows create an index, stage DAG work, and finalize it
- **THEN** they produce the same externally visible results as before this model refactor
