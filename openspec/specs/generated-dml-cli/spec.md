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

### Requirement: Root classmethods share matching constructor parameters dynamically
The generated CLI SHALL derive root classmethod command arguments from runtime-visible signatures and SHALL intersect classmethod parameters with constructor parameters when the parameter names match and their resolved base types match.

Intersected classmethod parameters SHALL be exposed only through the constructor-derived root option surface and SHALL NOT be exposed again on the classmethod command parser. Non-intersecting classmethod parameters SHALL continue to be exposed on the classmethod command parser according to the normal generated argument rules.

#### Scenario: Same-name same-type classmethod parameters are omitted from command-local help
- **WHEN** a root classmethod has parameters with the same names and resolved base types as constructor parameters
- **THEN** the generated classmethod command help omits those parameters from its command-local arguments and options
- **AND** the generated root help continues to expose the corresponding constructor-derived root options

#### Scenario: Same-name different-type classmethod parameters remain command-local
- **WHEN** a root classmethod parameter has the same name as a constructor parameter but a different resolved base type
- **THEN** the generated classmethod command keeps that parameter as a command-local argument or option

#### Scenario: Intersected classmethod values come from root options
- **WHEN** a user invokes a root classmethod command and supplies an intersected parameter through the root option surface
- **THEN** the CLI invokes the classmethod with that parsed value using the classmethod parameter name

#### Scenario: Init remote_root is supplied from root remote-root
- **WHEN** `Dml.__init__` and `Dml.init` both expose `remote_root` with the same resolved base type
- **THEN** `dml init --remote-root <uri>` is not part of the generated command grammar
- **AND** `dml --remote-root <uri> init` invokes `Dml.init(remote_root=<uri>, ...)`

#### Scenario: Init project_home remains command-local
- **WHEN** `Dml.__init__` exposes `project_home` as `str | None` and `Dml.init` exposes `project_home` as `str`
- **THEN** `project_home` does not intersect
- **AND** `dml init --project-home <path>` remains part of the generated command grammar

### Requirement: Constructor option metavars hide internal destinations
The generated CLI SHALL NOT expose internal constructor destination prefixes such as `_init_` in user-visible help or usage metavars. Internal parser destinations MAY remain prefixed or otherwise distinct when needed to avoid parser namespace collisions.

#### Scenario: Root constructor option usage shows public metavar
- **WHEN** a user views root `dml --help`
- **THEN** constructor-derived options show public metavars based on the option name, such as `REMOTE_ROOT`
- **AND** constructor-derived options do not show `_INIT_REMOTE_ROOT` or other internal destination names

#### Scenario: Internal destination choices do not affect public help
- **WHEN** the CLI uses an internal destination to distinguish root constructor options from command-local options
- **THEN** generated help and usage still display only public option names and public metavars

### Requirement: Generated CLI exposes a root version flag
The generated `dml` CLI SHALL expose a root `--version` flag sourced from the package version metadata.

#### Scenario: Root version flag prints a conventional version string
- **WHEN** a user runs `dml --version`
- **THEN** the CLI prints `dml, version <version>` to `stdout`
- **AND** it exits successfully without requiring a command name

### Requirement: Generated help separates commands from namespaces
Any generated parser that exposes both leaf commands and namespace groups SHALL render them in separate help sections.

#### Scenario: Root help lists commands before namespaces
- **WHEN** a user runs `dml --help`
- **THEN** the help output shows a `commands` section for leaf commands
- **AND** the help output shows a distinct `namespaces` section for namespace groups
- **AND** the `commands` section appears before the `namespaces` section

#### Scenario: Nested namespace help also separates commands from namespaces
- **WHEN** a generated namespace parser exposes both leaf commands and nested namespace groups
- **THEN** its help output shows leaf commands in `commands`
- **AND** it shows nested namespace groups in `namespaces`

#### Scenario: Namespace help text remains visible in the namespace list
- **WHEN** a namespace group appears in generated help
- **THEN** the help output includes that namespace name and its generated help text in the `namespaces` section

#### Scenario: Help rendering preserves parser-managed output behavior
- **WHEN** generated help or version output is shown
- **THEN** the CLI uses the same parser-managed output path as other generated parser help
- **AND** it does not replace that path with manual plain-text printing that would bypass existing formatter behavior

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

### Requirement: Overload ambiguity uses one runtime signature
The CLI SHALL generate commands from one runtime-visible signature even when overload declarations describe multiple static variants.

#### Scenario: Overloaded method still generates one command
- **WHEN** a public method has overload declarations and one implementation signature
- **THEN** the CLI uses the implementation signature for generation and does not create multiple command variants

### Requirement: Generated CLI output and errors follow serializer-map rules
The generated CLI SHALL serialize successful results using ordered serializer families and normalized failures using the established structured error payload.

For each return annotation, the CLI SHALL derive a `serializer -> allowed type subset` map, ignore `None` for serializer selection, and choose the highest-priority serializer whose allowed subset matches the runtime value using the same global priority order as input parsing.

When a root `Dml` classmethod command returns a `Dml` instance, the CLI SHALL first project that result to `dml.status()` and SHALL serialize the projected value using the `Dml.status` return contract.

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

#### Scenario: Root classmethod Dml return serializes status payload
- **WHEN** a root classmethod command returns a `Dml` instance
- **THEN** the CLI calls `dml.status()` on that instance before output serialization
- **AND** it serializes the resulting status payload according to the `Dml.status` return contract

#### Scenario: Incompatible runtime value fails instead of falling back outside the subset map
- **WHEN** a command return annotation has no serializer subset that matches the actual runtime value
- **THEN** the CLI fails with the established structured serialization error payload

#### Scenario: Failed command emits structured error payload
- **WHEN** a generated command raises or output serialization fails
- **THEN** the CLI emits a structured error payload instead of an unstructured traceback

### Requirement: Generated CLI failures are normalized
The generated CLI SHALL emit normalized failures instead of unstructured tracebacks.

#### Scenario: Failed command emits structured JSON error
- **WHEN** generated command execution raises an exception
- **THEN** the CLI emits a structured JSON error payload instead of an unstructured traceback

### Requirement: Generated CLI exposes named-remote synchronization commands
The generated CLI SHALL expose named remote lifecycle commands, `fetch [REMOTE]`, no-positional-argument `pull` and `push`, `branch create [--remote REMOTE] [--revision REV] NAME`, and `branch set-upstream REMOTE/BRANCH` from the public API signatures.

#### Scenario: Fetch accepts optional remote name
- **WHEN** a user runs `dml fetch research`
- **THEN** generated parsing passes `research` as the selected remote name

#### Scenario: Pull and push reject positional remotes
- **WHEN** a user runs `dml pull origin` or `dml push origin`
- **THEN** generated parsing rejects the extra positional argument

#### Scenario: Branch create options are exposed
- **WHEN** a user views `dml branch create --help`
- **THEN** help shows required positional `NAME` and optional `--remote` and `--revision` arguments
