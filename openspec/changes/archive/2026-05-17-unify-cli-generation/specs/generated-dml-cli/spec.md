## ADDED Requirements

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

#### Scenario: Unsupported parameter type omits method
- **WHEN** a public method includes a parameter annotated with an unsupported type such as `Any`
- **THEN** the CLI does not expose that method

#### Scenario: Supported typed method remains exposed
- **WHEN** a public method uses only supported parameter families such as `Ref`, `int`, `float`, `str`, `Literal`, optionals of those types, or JSON-backed container types
- **THEN** the CLI exposes that method

### Requirement: Generated arguments follow signature-driven CLI rules
The CLI SHALL derive argument shape from runtime-visible signatures, defaults, and resolved annotations.

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

### Requirement: Generated parsing uses annotations and documented help metadata
The CLI SHALL parse supported argument types from resolved annotations and SHALL use docstrings plus `Annotated` metadata to generate command help.

#### Scenario: Literal annotations constrain choices
- **WHEN** a parameter is annotated with `Literal[...]`
- **THEN** the generated CLI restricts accepted values to those literals

#### Scenario: Ref annotations parse as refs
- **WHEN** a parameter is annotated as `Ref`
- **THEN** the generated CLI parses the input string into a `Ref` value before calling the method

#### Scenario: Positional arguments are documented in help text
- **WHEN** a generated command includes positional arguments
- **THEN** the command help includes positional argument documentation derived from annotations or doc metadata rather than relying only on default `argparse` positional rendering

### Requirement: Overload ambiguity uses one runtime signature
The CLI SHALL generate commands from one runtime-visible signature even when overload declarations describe multiple static variants.

#### Scenario: Overloaded method still generates one command
- **WHEN** a public method has overload declarations and one implementation signature
- **THEN** the CLI uses the implementation signature for generation and does not create multiple command variants

### Requirement: Generated CLI output and errors are JSON
The generated CLI SHALL emit JSON for successful results and normalized failures.

#### Scenario: Successful command emits JSON
- **WHEN** a generated CLI command returns a value
- **THEN** the CLI serializes that value as JSON using the standard typed-leaf encoder

#### Scenario: Failed command emits structured JSON error
- **WHEN** generated command execution raises an exception
- **THEN** the CLI emits a structured JSON error payload instead of an unstructured traceback
