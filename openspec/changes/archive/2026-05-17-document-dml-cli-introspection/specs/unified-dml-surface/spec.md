## ADDED Requirements

### Requirement: Shared `Dml` surface SHALL be introspection-ready
The shared `Dml` boundary and its public namespaces SHALL expose runtime documentation that explains class purpose, method behavior, and parameter meaning without changing workflow semantics.

#### Scenario: Namespace objects describe their purpose
- **WHEN** a caller inspects `Dml` or any namespace reachable through `dml.config`, `dml.runtime`, `dml.dag`, or `dml.admin`
- **THEN** the class exposes a docstring that describes the purpose of that boundary or namespace

#### Scenario: Public methods describe behavior
- **WHEN** a caller inspects a public top-level or namespaced `Dml` method
- **THEN** the method exposes a docstring that describes the operation behavior and any notable constraints or side effects

### Requirement: Shared `Dml` parameters SHALL expose machine-readable help metadata
Public parameters on the shared `Dml` surface and its public namespace methods SHALL use `typing.Annotated` metadata to describe parameter meaning, while Python signature defaults remain the source of truth for default values.

#### Scenario: Parameter meaning is available from annotations
- **WHEN** a caller inspects annotations for a public `Dml` method or a public method on a `Dml` namespace object with extras included
- **THEN** the parameter annotations include `Annotated` metadata that describes what each user-facing parameter means

#### Scenario: Defaults remain in the signature
- **WHEN** a public `Dml` or namespaced method has a defaulted parameter
- **THEN** the default value remains represented by the Python signature
- **AND** the `Annotated` metadata does not become the source of truth for that default

#### Scenario: Ambiguous selector parameters may include examples
- **WHEN** a public `Dml` parameter accepts potentially confusing selector or URI forms such as revision selectors or remote project identifiers
- **THEN** the `Annotated` metadata MAY include concise examples that clarify accepted forms without redefining the underlying grammar
