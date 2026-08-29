## MODIFIED Requirements

### Requirement: CLI output contract remains stable through documented compatibility changes
Refactoring to enforce a thin CLI boundary MUST preserve documented user-visible command semantics, including success output structure and failure signaling, except where a change explicitly defines a breaking CLI compatibility update.

#### Scenario: Refactor preserves behavior outside documented breaks
- **WHEN** CLI logic is moved into domain modules for commands whose public contract is unchanged by an approved change
- **THEN** command outputs and exit outcomes remain equivalent for existing supported invocations

#### Scenario: Approved CLI redesign may replace old command contracts
- **WHEN** an approved change explicitly redefines the public CLI grammar and JSON payloads
- **THEN** the implementation MAY remove prior command names and prior output payload shapes for the affected commands
