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
- **AND** it does not inspect domain semantics to choose output transport

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
