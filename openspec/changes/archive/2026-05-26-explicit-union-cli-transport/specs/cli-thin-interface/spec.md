## MODIFIED Requirements

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
- **WHEN** the CLI inspects `Dml` signatures, annotations, docstrings, and transport-map metadata to build commands
- **THEN** that inspection is used only to derive transport behavior and not to re-implement domain workflow rules in the CLI layer

#### Scenario: Union selector conversion remains transport-only
- **WHEN** a generated command includes an input union parameter
- **THEN** the CLI only uses mapped short deserializer names, mapped deserializer functions, and first-member defaults to choose the transport path
- **AND** any semantic validation of the resulting value still occurs in the underlying `Dml` method

#### Scenario: Union return serialization remains transport-only
- **WHEN** a generated command has a union-annotated return type
- **THEN** the CLI only uses the serializer map and the first non-`None` return member in annotation order to choose output transport
- **AND** it does not inspect domain semantics to choose among union return members

### Requirement: CLI output contract remains stable through documented compatibility changes
Refactoring to enforce a thin CLI boundary MUST preserve documented user-visible command semantics, including failure signaling and structured error payload shape, except where a change explicitly defines a breaking CLI compatibility update.

#### Scenario: Refactor preserves behavior outside documented breaks
- **WHEN** CLI logic is moved into domain modules for commands whose public contract is unchanged by an approved change
- **THEN** command outputs and exit outcomes remain equivalent for existing supported invocations

#### Scenario: Approved CLI redesign may replace targeted command contracts
- **WHEN** an approved change explicitly redefines the public CLI grammar or successful output serialization rules for affected commands
- **THEN** the implementation MAY remove prior command spellings and prior successful output serialization forms for those affected commands only
- **AND** it preserves the established structured error payload shape unless the approved change says otherwise

#### Scenario: Approved union transport redesign removes inferred forms
- **WHEN** the explicit union transport change is implemented for generated commands
- **THEN** affected commands MAY expose `--<name>-type` selectors only for positional input unions with multiple distinct short deserializer names
- **AND** affected commands MAY replace prior untyped options such as `--dag` with typed options such as `--dag-str` and `--dag-ref` only when an input union has multiple distinct short deserializer names
- **AND** union member transport choice is driven only by the three transport maps
- **AND** union return serializer choice is driven only by the first non-`None` return member in annotation order
- **AND** commands unaffected by union annotations preserve their existing public grammar
