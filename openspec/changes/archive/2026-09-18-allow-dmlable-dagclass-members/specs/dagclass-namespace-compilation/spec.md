## ADDED Requirements

### Requirement: Dagclass compilation transforms only recognized member forms
Dagclass compilation SHALL compile a class-defined Python function as a delayed method, SHALL inject namespace dependencies into a decorated self-method, and SHALL replace a nested dagclass instance with its compiled entrypoint. It SHALL preserve every other evaluated dataclass field or class-defined member as an ordinary namespace value without rejecting the value merely because it is callable or descriptor-backed.

#### Scenario: Plain method is compiled
- **WHEN** a class-defined member is a Python function
- **THEN** dagclass compilation converts it to a delayed method using the inferred namespace dependencies

#### Scenario: Decorated self-method is compiled
- **WHEN** a class-defined member is a delayed decorated function whose first parameter is `self`
- **THEN** dagclass compilation injects its inferred namespace dependencies into the delayed method

#### Scenario: Nested dagclass member is compiled
- **WHEN** a dataclass field or class-defined member evaluates to a compiled dagclass instance
- **THEN** dagclass compilation stores that nested instance's configured compiled entrypoint in the namespace

#### Scenario: Callable value is preserved
- **WHEN** a dataclass field or class-defined member evaluates to a callable value that is not a recognized method form
- **THEN** dagclass compilation preserves it as an ordinary namespace value without reporting an unsupported callable error

#### Scenario: Descriptor-backed value is preserved
- **WHEN** a class-defined member is descriptor-backed and its evaluation produces an ordinary value
- **THEN** dagclass compilation preserves the evaluated value without reporting an unsupported descriptor error

### Requirement: Preserved dagclass values use normal codec staging
A preserved dagclass namespace value SHALL be normalized only when its compiled member graph is staged in a DAG. Dagclass compilation SHALL NOT preflight whether a codec can encode the value, and staging SHALL apply the same codec behavior and contextual restrictions used for the value outside a dagclass.

#### Scenario: Node member is staged
- **WHEN** a compiled dagclass member graph contains a `Node` value that normal DAG codec staging can reuse or import
- **THEN** staging normalizes the member through the built-in node codec

#### Scenario: Projection member is staged
- **WHEN** a compiled dagclass member graph contains a `Projection` value supported by normal DAG codec staging
- **THEN** staging normalizes the member through the built-in projection codec

#### Scenario: Callable custom value is staged
- **WHEN** a compiled dagclass member graph contains a callable value accepted by a registered codec
- **THEN** staging normalizes that value through the registered codec

#### Scenario: Preserved value has no codec
- **WHEN** a compiled dagclass member graph contains a preserved value that normal DAG codec staging cannot encode
- **THEN** dagclass instantiation succeeds and staging reports the normal codec error for that value

#### Scenario: Node remains contextually invalid
- **WHEN** a compiled dagclass member graph contains an uncommitted node from a different runtime index
- **THEN** dagclass instantiation succeeds and staging reports the normal cross-index node codec error
