## Purpose

Makes source-based funks reliable in interactive authoring environments by preserving their executable script while source is available.

## ADDED Requirements

### Requirement: Funkification SHALL capture canonical source eagerly
When a Python callable is funkified for script execution, the system SHALL obtain and normalize its function source and explicit source dependencies during funkification. Later staging and execution SHALL use that captured canonical script without inspecting the authoring function again.

#### Scenario: Interactive function is funkified
- **WHEN** an interactive environment exposes a function's source at decoration time
- **THEN** funkification succeeds and preserves a canonical executable script for later staging

#### Scenario: Authoring source becomes unavailable later
- **WHEN** source lookup succeeds during funkification but becomes unavailable before the funk is called or executed
- **THEN** staging and execution use the eagerly captured script and do not repeat source lookup

#### Scenario: Source is unavailable at funkification
- **WHEN** the callable's source cannot be obtained during funkification
- **THEN** funkification fails immediately with a source-serialization error

### Requirement: Eager capture SHALL preserve source-only isolation
Eager capture SHALL include only the function source and explicitly supplied source dependencies. It SHALL NOT implicitly serialize module globals, closures, or the authoring interpreter state.

#### Scenario: Function references an uninjected global
- **WHEN** a funkified function references a global absent from its function body and explicit source dependencies
- **THEN** later execution fails rather than transferring that global from the authoring process

### Requirement: Eager capture SHALL preserve script execution semantics
The captured script SHALL preserve configured function selection, explicit prelude and postlude source, DAG arguments, cache identity inputs, and result commit behavior.

#### Scenario: Existing file-defined funk executes
- **WHEN** an existing valid file-defined funk is decorated, staged, and executed
- **THEN** it produces the same generated program and result contract as source-based script execution
