## MODIFIED Requirements

### Requirement: Generated CLI output and errors follow serializer-map rules
The generated CLI SHALL serialize successful results using ordered serializer families and normalized failures using the established structured error payload.

For each return annotation, the CLI SHALL derive a `serializer -> allowed type subset` map, ignore `None` for serializer selection, and choose the highest-priority serializer whose allowed subset matches the runtime value using the same global priority order as input parsing.

When a root `Dml` classmethod command returns a `Dml` instance, the CLI SHALL first project that result to `dml.status()` and SHALL serialize the projected payload using the `Dml.status` return contract rather than the classmethod's `Dml` return annotation.

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
- **WHEN** a root `Dml` classmethod command returns a `Dml` instance
- **THEN** the CLI calls `status()` on that instance before serialization
- **AND** it serializes the resulting payload using the serializer families for `Dml.status`

#### Scenario: Incompatible runtime value fails instead of falling back outside the subset map
- **WHEN** a command return annotation has no serializer subset that matches the actual runtime value after applying documented result projections
- **THEN** the CLI fails with the established structured serialization error payload

#### Scenario: Failed command emits structured error payload
- **WHEN** a generated command raises or output serialization fails
- **THEN** the CLI emits a structured error payload instead of an unstructured traceback
