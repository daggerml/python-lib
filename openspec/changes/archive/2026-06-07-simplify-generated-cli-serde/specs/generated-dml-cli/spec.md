## MODIFIED Requirements

### Requirement: CLI only exposes methods with generatable parameter types
The CLI SHALL omit any public `Dml` or namespace method whose parameter annotations cannot be generated from command-line input.

For non-`None` union input annotations, a parameter is CLI-generatable when every member can be grouped into at least one supported parser family and every such family has a registered parser plus subset-matching rule.

#### Scenario: Unsupported parameter type omits method
- **WHEN** a public method includes a parameter annotated with a type that has no registered CLI transport family and is not exactly `Any`
- **THEN** the CLI does not expose that method

#### Scenario: Exact Any parameter remains exposed through DML-backed transport
- **WHEN** a public method includes a parameter annotated as exactly `Any`
- **THEN** the CLI exposes that method
- **AND** the generated argument uses the registered `dml` transport
- **AND** omitting a file path still allows the CLI to read the serialized value from `stdin`
- **AND** the CLI deserializes the text with `daggerml._internal.dml_loads` before invoking the method

#### Scenario: Union with supported parser families remains exposed
- **WHEN** a public method parameter is annotated with a non-`None` union whose members all map to supported parser families and subset-matching rules
- **THEN** the CLI exposes that method using one generated argument form for that parameter

#### Scenario: Union with unsupported member omits method
- **WHEN** a public method parameter is annotated with a non-`None` union containing a member that maps to no supported parser family
- **THEN** the CLI does not expose that method

### Requirement: Generated arguments follow signature-driven CLI rules
The CLI SHALL derive argument shape from runtime-visible signatures, defaults, and resolved annotations.

Required parameters SHALL become positional arguments unless an existing caller path renders them as required options via `required_as_options=True`. Defaulted parameters SHALL become options. Boolean defaults SHALL continue to use positive or negative flags. Union input parameters SHALL not add typed selector grammar; instead they SHALL use one generated argument form whose parsing behavior comes from the parameter's ordered parser-family subset map.

#### Scenario: Required parameters become positional arguments
- **WHEN** a public method parameter has no default value
- **THEN** the generated CLI exposes it as a positional argument using the snake_case parameter name

#### Scenario: Defaulted parameters become options
- **WHEN** a public method parameter has a default value
- **THEN** the generated CLI exposes it as an option using the kebab-case parameter name

#### Scenario: Boolean defaults preserve behavior
- **WHEN** a boolean parameter default is `False`
- **THEN** the generated CLI exposes a positive `--<name>` flag
- **AND** when a boolean parameter default is `True`
- **THEN** the generated CLI exposes a negative `--no-<name>` flag

#### Scenario: Union option keeps one option form
- **WHEN** a generated option parameter is a non-`None` union
- **THEN** the CLI exposes one option form for that parameter rather than `--<name>-type` or typed union option variants

#### Scenario: Union positional keeps one positional form
- **WHEN** a generated positional parameter is a non-`None` union
- **THEN** the CLI exposes one positional argument for that parameter
- **AND** it does not add `--<name>-type`

### Requirement: Generated parsing uses ordered parser families and documented help metadata
The CLI SHALL parse supported argument types from resolved annotations and ordered parser families, and SHALL use docstrings plus `Annotated` metadata to generate command help.

For each generated parameter, the CLI SHALL derive a `parser -> allowed type subset` map from the annotation and accept the first parsed value that matches that parser's allowed subset for the parameter.

The parser-family order SHALL be:

1. `None`
2. `Any/Error`
3. collections
4. `str` when present in the allowed subset for that parameter
5. remaining scalar constructors in family order

#### Scenario: Literal annotations constrain choices
- **WHEN** a parameter is annotated with `Literal[...]`
- **THEN** the generated CLI restricts accepted values to those literals

#### Scenario: Ref annotations parse as refs and serialize with ref.to
- **WHEN** a parameter is annotated as `Ref`
- **THEN** the generated CLI parses the input string into a `Ref` value before calling the method
- **AND** when serializing that value for CLI transport it uses `ref.to`

#### Scenario: Error annotations use DML serde transport
- **WHEN** a parameter is annotated as `Error`
- **THEN** the generated CLI uses the registered `dml` transport backed by `daggerml._internal.dml_loads` and `daggerml._internal.dml_dumps`

#### Scenario: JSON-backed structured annotations use JSON transport
- **WHEN** a parameter is annotated as `dict`, `list`, or a `TypedDict` family
- **THEN** the generated CLI uses the registered `json` transport backed by JSON loads/dumps

#### Scenario: Ref-or-string union prefers string transport
- **WHEN** a generated parameter is annotated as `Ref | str`
- **THEN** the CLI derives `str -> {str}` and `ref -> {Ref}` for that parameter
- **AND** it tries `str` before `Ref`
- **AND** the invoked method receives a string when normal string parsing succeeds

#### Scenario: Nullable string-or-int keeps non-null scalar tokens as strings
- **WHEN** a generated parameter is annotated as `str | int | None`
- **THEN** the CLI accepts `null` as `None`
- **AND** it tries `str` before `int` for non-null scalar tokens
- **AND** the invoked method receives a string when normal string parsing succeeds

#### Scenario: Ref-or-error union tries DML before ref construction
- **WHEN** a generated parameter is annotated as `Ref | Error`
- **THEN** the CLI derives `dml -> {Error}` and `ref -> {Ref}` for that parameter
- **AND** it first attempts DML deserialization
- **AND** it only accepts that result when the parsed value matches `Error`
- **AND** otherwise it falls back to `Ref(...)`

#### Scenario: Any-sharing union accepts DML values by subset
- **WHEN** a generated parameter is annotated as `Any | Error | Ref`
- **THEN** the CLI derives `dml -> {Any, Error}` and `ref -> {Ref}` for that parameter
- **AND** it accepts any successfully deserialized DML value through the `dml` parser because `Any` is in that subset

#### Scenario: Collection-or-ref union tries JSON before ref construction
- **WHEN** a generated parameter is annotated as `list[Ref] | Ref`
- **THEN** the CLI derives `json -> {list[Ref]}` and `ref -> {Ref}` for that parameter
- **AND** it first attempts JSON deserialization
- **AND** it only accepts that result when the parsed value matches the collection subset

#### Scenario: Positional arguments are documented in help text
- **WHEN** a generated command includes positional arguments
- **THEN** the command help includes positional argument documentation derived from annotations or doc metadata rather than relying only on default `argparse` positional rendering

### Requirement: Generated CLI output and errors follow serializer-map rules
The generated CLI SHALL serialize successful results using ordered serializer families and normalized failures using the established structured error payload.

For each return annotation, the CLI SHALL derive a `serializer -> allowed type subset` map, ignore `None` for serializer selection, and choose the highest-priority serializer whose allowed subset matches the runtime value using the same global priority order as input parsing.

#### Scenario: Non-union return uses mapped serializer
- **WHEN** a command return annotation is a non-union type with a registered serializer
- **THEN** the CLI serializes the result with that mapped serializer

#### Scenario: None return prints nothing
- **WHEN** a command returns `None`
- **THEN** the CLI prints nothing to `stdout`

#### Scenario: Union return uses runtime-compatible serializer subset
- **WHEN** a command return annotation is `Ref | Error`
- **THEN** the CLI derives `dml -> {Error}` and `ref -> {Ref}` for that return type
- **AND** it serializes an `Error` value with the DML serializer
- **AND** it serializes a `Ref` value with the ref serializer

#### Scenario: Any-sharing return prefers DML serializer
- **WHEN** a command return annotation is `Any | Error | Ref`
- **THEN** the CLI derives `dml -> {Any, Error}` and `ref -> {Ref}` for that return type
- **AND** it uses the DML serializer for any runtime value accepted by the `dml` subset

#### Scenario: Incompatible runtime value fails instead of falling back outside the subset map
- **WHEN** a command return annotation has no serializer subset that matches the actual runtime value
- **THEN** the CLI fails with the established structured serialization error payload

#### Scenario: Failed command emits structured error payload
- **WHEN** a generated command raises or output serialization fails
- **THEN** the CLI emits a structured error payload instead of an unstructured traceback
