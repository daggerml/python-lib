## ADDED Requirements

### Requirement: CLI handlers are transport-only
The CLI command layer SHALL be limited to parsing command inputs, invoking domain interfaces, and serializing outputs, and SHALL NOT contain business workflow or domain decision logic.

#### Scenario: CLI parses and delegates
- **WHEN** a user invokes any CLI command
- **THEN** the handler parses flags/arguments, calls a domain entrypoint, and formats the returned result without domain branching in the CLI layer

### Requirement: Domain logic resides outside CLI modules
Any behavior that determines domain outcomes (state transitions, merge/reconcile rules, execution sequencing, or validation beyond input shape/type checks) MUST execute in API/internal modules rather than `src/daggerml/_cli/**`.

#### Scenario: Decision logic extraction
- **WHEN** a command path requires branching based on repository or execution state
- **THEN** the branching logic executes in a non-CLI module and CLI code only forwards parsed inputs and surfaces returned outcomes

### Requirement: CLI output contract remains stable through refactor
Refactoring to enforce a thin CLI boundary MUST preserve existing user-visible command semantics, including success output structure and failure signaling, unless a change is explicitly documented as a compatibility update.

#### Scenario: Refactor preserves command behavior
- **WHEN** CLI logic is moved into domain modules
- **THEN** command outputs and exit outcomes remain equivalent for existing supported invocations

### Requirement: CLI tests focus on interface behavior
CLI-focused tests SHALL validate input parsing, delegation wiring, output serialization, and exit signaling, while domain behavior assertions SHALL be covered in non-CLI test suites.

#### Scenario: Test responsibility split
- **WHEN** adding or updating tests for a refactored command
- **THEN** CLI tests assert transport concerns only and domain behavior checks appear in API/internal tests
