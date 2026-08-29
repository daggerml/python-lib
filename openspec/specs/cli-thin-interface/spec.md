# cli-thin-interface Specification

## Purpose
Define the thin transport-only contract for the generated public CLI surface.
## Requirements
### Requirement: CLI handlers are transport-only
The CLI command layer SHALL be limited to discovering command shape from the public `Dml` surface, parsing command inputs, invoking domain interfaces, and serializing outputs, and SHALL NOT contain business workflow or domain decision logic.

#### Scenario: CLI parses and delegates
- **WHEN** a user invokes any CLI command
- **THEN** the handler parses flags and arguments, calls a domain entrypoint, and formats the returned result without domain branching in the CLI layer

#### Scenario: Exact Any transport uses shared serde helpers only
- **WHEN** a generated CLI command includes an exact `Any` parameter or return annotation
- **THEN** the CLI limits itself to file/stdin text IO plus `daggerml._internal.dml_loads` and `daggerml._internal.dml_dumps`
- **AND** it still delegates all domain behavior to the underlying `Dml` method

#### Scenario: Generated command discovery remains transport-only
- **WHEN** the CLI inspects `Dml` signatures, annotations, docstrings, and transport metadata to build commands
- **THEN** that inspection is used only to derive transport behavior and not to re-implement domain workflow rules in the CLI layer

#### Scenario: Ordered parser selection remains transport-only
- **WHEN** a generated command includes an input union parameter
- **THEN** the CLI only derives parser families, allowed type subsets, and priority order from the annotation
- **AND** it accepts the first parsed value whose runtime type matches that parser's allowed subset for the parameter
- **AND** any semantic validation of the resulting value still occurs in the underlying `Dml` method

#### Scenario: Ordered serializer selection remains transport-only
- **WHEN** a generated command has a union-annotated return type
- **THEN** the CLI only derives serializer families, allowed type subsets, and priority order from the annotation plus runtime value type
- **AND** it does not inspect domain semantics to choose among union return members

### Requirement: One generated CLI module owns the public transport surface
The `dml` CLI SHALL be implemented through a single generated transport module rather than a package of hand-maintained per-command parser modules.

#### Scenario: Public CLI entrypoint resolves through one module
- **WHEN** the `dml` script entrypoint is loaded
- **THEN** it imports one CLI module that generates and dispatches the public command surface

### Requirement: Generated CLI command exposure follows the public `Dml` surface
The CLI SHALL expose the public CLI-generatable `Dml` surface directly rather than maintaining a smaller curated command subset.

#### Scenario: Runtime workflows become CLI-visible when generatable
- **WHEN** a public `dml.runtime` method uses only CLI-generatable parameter types
- **THEN** the generated CLI exposes that runtime workflow as a command

#### Scenario: JSON output is uniform across generated commands
- **WHEN** any generated CLI command succeeds or fails
- **THEN** the CLI emits JSON rather than mixing JSON and plain-text command modes

### Requirement: Domain logic resides outside CLI modules
Any behavior that determines domain outcomes (state transitions, merge/reconcile rules, execution sequencing, or validation beyond input shape/type checks) MUST execute in API/internal modules rather than `src/daggerml/_cli/**`.

#### Scenario: Decision logic extraction
- **WHEN** a command path requires branching based on repository or execution state
- **THEN** the branching logic executes in a non-CLI module and CLI code only forwards parsed inputs and surfaces returned outcomes

### Requirement: CLI output contract remains stable through documented compatibility changes
Refactoring to enforce a thin CLI boundary MUST preserve documented user-visible command semantics, including failure signaling and structured error payload shape, except where a change explicitly defines a breaking CLI compatibility update.

#### Scenario: Refactor preserves behavior outside documented breaks
- **WHEN** CLI logic is moved into domain modules for commands whose public contract is unchanged by an approved change
- **THEN** command outputs and exit outcomes remain equivalent for existing supported invocations

#### Scenario: Approved CLI redesign may replace targeted command contracts
- **WHEN** an approved change explicitly redefines the public CLI grammar or successful output serialization rules for affected commands
- **THEN** the implementation MAY remove prior command spellings and prior successful output serialization forms for those affected commands only
- **AND** it preserves the established structured error payload shape unless the approved change says otherwise

#### Scenario: Approved serde-priority redesign removes explicit union transport forms
- **WHEN** the ordered serde-priority change is implemented for generated commands
- **THEN** affected commands MAY remove `--<name>-type` selectors and typed union option variants
- **AND** union member transport choice is driven only by parser-family subset maps plus global priority order
- **AND** union return serializer choice is driven only by serializer-family subset maps plus global priority order
- **AND** commands unaffected by union annotations preserve their existing public grammar

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
