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

### Requirement: CLI output contract remains stable through documented compatibility changes
Refactoring to enforce a thin CLI boundary MUST preserve documented user-visible command semantics, including success output structure and failure signaling, except where a change explicitly defines a breaking CLI compatibility update.

#### Scenario: Refactor preserves behavior outside documented breaks
- **WHEN** CLI logic is moved into domain modules for commands whose public contract is unchanged by an approved change
- **THEN** command outputs and exit outcomes remain equivalent for existing supported invocations

#### Scenario: Approved CLI redesign may replace old command contracts
- **WHEN** an approved change explicitly redefines the public CLI grammar and JSON payloads
- **THEN** the implementation MAY remove prior command names and prior output payload shapes for the affected commands

### Requirement: CLI tests focus on interface behavior
CLI-focused tests SHALL validate input parsing, delegation wiring, output serialization, and exit signaling, while domain behavior assertions SHALL be covered in non-CLI test suites.

#### Scenario: Test responsibility split
- **WHEN** adding or updating tests for a refactored command
- **THEN** CLI tests assert transport concerns only and domain behavior checks appear in API/internal tests

### Requirement: CLI project and remote override flags use canonical config-shaped names
The CLI SHALL expose explicit project and remote override flags using the canonical configuration naming represented by the shared resolver, rather than frontend-specific aliases.

#### Scenario: Top-level project override uses canonical name
- **WHEN** a user passes an explicit project directory to any command
- **THEN** the CLI accepts `--project-home <path>` as the top-level override flag
- **AND** the CLI does not advertise `--repo` as the supported flag name

#### Scenario: Top-level remote override uses canonical name
- **WHEN** a user passes an explicit remote project root to any command
- **THEN** the CLI accepts `--remote-root <uri>` as the top-level override flag
- **AND** the CLI does not advertise `--remote-uri` as the supported flag name

### Requirement: CLI guidance uses canonical flag names consistently
CLI help text, examples, and normalized user-facing recovery hints SHALL use the same canonical flag names as the parser surface.

#### Scenario: Help examples show canonical overrides
- **WHEN** a user opens top-level or subcommand help for commands that mention explicit config overrides
- **THEN** the examples and help text refer to `--project-home` and `--remote-root` instead of legacy aliases

#### Scenario: Missing project-home hint uses canonical flag name
- **WHEN** command execution fails because no local project path can be resolved
- **THEN** the structured error hint instructs the user to pass `--project-home PATH` or set `DML_PROJECT_HOME`

### Requirement: Shared public flag names do not create ambiguous CLI behavior
When the CLI uses the same canonical public flag spelling in different parser scopes, command dispatch SHALL preserve the intended meaning for each command path.

#### Scenario: Init keeps its own remote-root input without shadowing the top-level override
- **WHEN** the CLI exposes both a top-level `--remote-root` option and `init --remote-root`
- **THEN** parsing and command execution keep those inputs distinguishable
- **AND** `init` continues to forward its own `--remote-root` value to bootstrap project remote configuration
