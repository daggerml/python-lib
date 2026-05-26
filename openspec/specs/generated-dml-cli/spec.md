# generated-dml-cli Specification

## Purpose
Define the generated public DML CLI surface, its input transports, and its output serialization rules.
## Requirements
### Requirement: CLI surface is generated from the public `Dml` API
The system SHALL generate the `dml` command tree from the public `Dml` class and its public namespaces rather than from a hand-maintained set of per-command parser modules.

#### Scenario: Top-level public methods become commands
- **WHEN** a public callable exists on `Dml` and its parameters are CLI-generatable
- **THEN** the CLI exposes a top-level command for that method

#### Scenario: Public namespaces become command groups
- **WHEN** a public namespace object is reachable from `Dml`
- **THEN** the CLI exposes that namespace as a subcommand group and exposes its public CLI-generatable methods as leaf commands

### Requirement: CLI only exposes methods with generatable parameter types
The CLI SHALL omit any public `Dml` or namespace method whose parameter annotations cannot be generated from command-line input.

For non-`None` union input annotations, a parameter is CLI-generatable when every member type has a registered short deserializer name and every such name resolves to a registered deserializer function.

#### Scenario: Unsupported parameter type omits method
- **WHEN** a public method includes a parameter annotated with a type that has no registered CLI transport and is not exactly `Any`
- **THEN** the CLI does not expose that method

#### Scenario: Exact Any parameter remains exposed through DML-backed transport
- **WHEN** a public method includes a parameter annotated as exactly `Any`
- **THEN** the CLI exposes that method
- **AND** the generated argument uses the registered `dml` transport
- **AND** omitting a file path still allows the CLI to read the serialized value from `stdin`
- **AND** the CLI deserializes the text with `daggerml._internal.dml_loads` before invoking the method

#### Scenario: Union with mapped transport remains exposed
- **WHEN** a public method parameter is annotated with a non-`None` union whose members all have registered short deserializer names and registered deserializer functions
- **THEN** the CLI exposes that method using direct or typed union transport grammar derived from those names

#### Scenario: Union with missing type-to-name mapping omits method
- **WHEN** a public method parameter is annotated with a non-`None` union containing a member with no registered short deserializer name
- **THEN** the CLI does not expose that method

#### Scenario: Union with missing short-name deserializer omits method
- **WHEN** a public method parameter is annotated with a non-`None` union containing a member whose short deserializer name has no registered deserializer function
- **THEN** the CLI does not expose that method

### Requirement: Generated arguments follow signature-driven CLI rules
The CLI SHALL derive argument shape from runtime-visible signatures, defaults, and resolved annotations.

Required parameters SHALL become positional arguments unless an existing caller path renders them as required options via `required_as_options=True`. Defaulted parameters SHALL become options. Boolean defaults SHALL continue to use positive or negative flags. Union input parameters SHALL use transport grammar derived from mapped short deserializer names instead of inferred parsing.

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

#### Scenario: Multi-name union option becomes typed mutually exclusive options
- **WHEN** a generated parameter is a non-`None` union emitted as an option and its members resolve to more than one distinct short deserializer name
- **THEN** the CLI exposes one `--<name>-<short-name> VALUE` option per distinct short name
- **AND** all such options write to the same destination parameter
- **AND** those options are mutually exclusive
- **AND** omitting all such options preserves the default value

#### Scenario: Required-as-option multi-name union requires one typed option
- **WHEN** a generated parameter is a non-`None` union emitted as an option because `required_as_options=True` and its members resolve to more than one distinct short deserializer name
- **THEN** the CLI exposes one `--<name>-<short-name> VALUE` option per distinct short name
- **AND** those options are members of one required mutually exclusive group

#### Scenario: Single-name union option uses direct transport
- **WHEN** a generated option parameter is a non-`None` union whose members all resolve to one distinct short deserializer name
- **THEN** the CLI exposes the same direct transport shape it would use for that one short name
- **AND** it does not add typed union option flags for that parameter

#### Scenario: Mixed shared-name union option dedupes by short name
- **WHEN** a generated option parameter is annotated as `Any | Error | Ref`
- **THEN** the CLI exposes `--<name>-dml VALUE` and `--<name>-ref VALUE`
- **AND** it does not expose separate `Any` and `Error` option forms because they share the `dml` short name

#### Scenario: Multi-name positional union exposes an optional type selector
- **WHEN** a generated positional parameter is a non-`None` union whose members resolve to more than one distinct short deserializer name
- **THEN** the CLI keeps the positional argument name unchanged
- **AND** the CLI also exposes an optional `--<name>-type {<short-names...>}` option
- **AND** omitting that selector defaults conversion to the first non-`None` union member in annotation order
- **AND** providing that selector overrides the default member transport before method invocation

#### Scenario: Single-name positional union uses direct transport
- **WHEN** a generated positional parameter is a non-`None` union whose members all resolve to one distinct short deserializer name
- **THEN** the CLI parses the positional token directly through that one deserializer
- **AND** it does not add `--<name>-type` for that parameter

#### Scenario: Mixed shared-name positional union dedupes selector choices
- **WHEN** a generated positional parameter is annotated as `Any | Error | Ref`
- **THEN** the CLI exposes `--<name>-type {dml,ref}`
- **AND** omitting that selector defaults to `dml` because `Any` is the first non-`None` union member

### Requirement: Generated parsing uses mapped transports and documented help metadata
The CLI SHALL parse supported argument types from resolved annotations and the transport maps, and SHALL use docstrings plus `Annotated` metadata to generate command help.

For input unions, the CLI SHALL use map-backed parser selection instead of implicit inference: typed option flags parse through the deserializer associated with their short name during `argparse` parsing, and positional union selectors choose the mapped deserializer during post-parse normalization, defaulting to the first non-`None` member when omitted.

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

#### Scenario: Typed union kwarg parses according to selected short name
- **WHEN** a user passes `--dag-str train` to a generated `dag: str | Ref | None` option
- **THEN** the invoked method receives `dag="train"`
- **AND** when the user passes `--dag-ref dag:abc123`
- **THEN** the invoked method receives `dag=Ref("dag:abc123")`

#### Scenario: Single-name union avoids extra selector syntax
- **WHEN** a positional or option union resolves to one distinct short deserializer name
- **THEN** the CLI does not add extra typed union selector syntax for that parameter

#### Scenario: Positional union selector defaults by union order and is not inferred from the token
- **WHEN** a generated positional parameter is a non-`None` union with multiple distinct short deserializer names
- **THEN** omitting `--<name>-type` selects the first non-`None` union member in annotation order
- **AND** the CLI does not infer the member type from the positional token text

#### Scenario: Explicit selector overrides first-member default
- **WHEN** a generated positional parameter is annotated as `str | Ref`
- **AND** the user passes `--<name>-type ref`
- **THEN** the CLI uses the mapped `Ref` deserializer even if `str` is the first union member

#### Scenario: Positional arguments are documented in help text
- **WHEN** a generated command includes positional arguments
- **THEN** the command help includes positional argument documentation derived from annotations or doc metadata rather than relying only on default `argparse` positional rendering

### Requirement: Overload ambiguity uses one runtime signature
The CLI SHALL generate commands from one runtime-visible signature even when overload declarations describe multiple static variants.

#### Scenario: Overloaded method still generates one command
- **WHEN** a public method has overload declarations and one implementation signature
- **THEN** the CLI uses the implementation signature for generation and does not create multiple command variants

### Requirement: Generated CLI output and errors follow serializer-map rules
The generated CLI SHALL serialize successful results using the serializer map and normalized failures using the established structured error payload.

For union return annotations, the CLI SHALL ignore `None` members and use the serializer associated with the first remaining member in annotation order.

#### Scenario: Non-union return uses mapped serializer
- **WHEN** a command return annotation is a non-union type with a registered serializer
- **THEN** the CLI serializes the result with that mapped serializer

#### Scenario: Union return uses first non-None member serializer
- **WHEN** a command return annotation is `str | Ref`
- **THEN** the CLI serializes the result with the `str` serializer because `str` is the first non-`None` member

#### Scenario: Optional return ignores None for serializer selection
- **WHEN** a command return annotation is `None | Ref`
- **THEN** the CLI serializes non-`None` results with the `Ref` serializer because `None` is ignored for serializer selection

#### Scenario: Any-or-Error return uses first member serializer
- **WHEN** a command return annotation is `Any | Error`
- **THEN** the CLI serializes the result with the `Any` serializer because `Any` is the first non-`None` member

#### Scenario: Incompatible runtime value fails instead of falling back
- **WHEN** a command return annotation is a union and the selected first non-`None` member serializer cannot serialize the actual runtime value
- **THEN** the CLI fails with the established structured serialization error payload
- **AND** it does not fall back to later union members
- **AND** it does not inspect runtime value shape to choose a different serializer

#### Scenario: Failed command emits structured error payload
- **WHEN** a generated command raises or output serialization fails
- **THEN** the CLI emits a structured error payload instead of an unstructured traceback

### Requirement: Generated CLI failures are normalized
The generated CLI SHALL emit normalized failures instead of unstructured tracebacks.

#### Scenario: Failed command emits structured JSON error
- **WHEN** generated command execution raises an exception
- **THEN** the CLI emits a structured JSON error payload instead of an unstructured traceback

