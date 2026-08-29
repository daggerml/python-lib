## MODIFIED Requirements

### Requirement: CLI handlers are transport-only
The CLI command layer SHALL be limited to discovering command shape from the public `Dml` surface, parsing command inputs, invoking domain interfaces, and serializing outputs, and SHALL NOT contain business workflow or domain decision logic.

#### Scenario: CLI parses and delegates
- **WHEN** a user invokes any CLI command
- **THEN** the handler parses flags and arguments, calls a domain entrypoint, and formats the returned result without domain branching in the CLI layer

#### Scenario: Generated command discovery remains transport-only
- **WHEN** the CLI inspects `Dml` signatures, annotations, and docstrings to build commands
- **THEN** that inspection is used only to derive transport behavior and not to re-implement domain workflow rules in the CLI layer

## ADDED Requirements

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
