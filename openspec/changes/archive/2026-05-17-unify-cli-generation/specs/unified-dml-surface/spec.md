## MODIFIED Requirements

### Requirement: Shared `Dml` surface SHALL be introspection-ready
The shared `Dml` boundary and its public namespaces SHALL expose runtime documentation that explains class purpose, method behavior, and parameter meaning without changing workflow semantics, and that metadata SHALL be sufficient for generated CLI help.

#### Scenario: Namespace objects describe their purpose
- **WHEN** a caller inspects `Dml` or any namespace reachable through `dml.config`, `dml.runtime`, `dml.dag`, or `dml.admin`
- **THEN** the class exposes a docstring that describes the purpose of that boundary or namespace

#### Scenario: Public methods describe behavior
- **WHEN** a caller inspects a public top-level or namespaced `Dml` method
- **THEN** the method exposes a docstring that describes the operation behavior and any notable constraints or side effects

#### Scenario: Generated CLI help can use runtime docs
- **WHEN** the CLI generator inspects `Dml` or one of its public namespace methods
- **THEN** it can derive command descriptions and parameter help from runtime docstrings and annotation metadata without a separate command-specific help registry

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

#### Scenario: Non-generatable CLI parameters are not part of the public method surface
- **WHEN** a public workflow depends on helper state that cannot be generated from CLI input such as an S3 client object
- **THEN** that helper state is provided through `Dml` instance construction or private instance state rather than through a public method parameter

### Requirement: `Dml` stores runtime context, S3 client state, and temporary-directory bookkeeping
The shared `Dml` class SHALL keep only `_context`, `_s3_client`, and `_tempdirs` as private instance attributes. Helper behavior that supports `Dml` public methods SHALL live in module-level functions within `daggerml._internal.dml` rather than in private `Dml` instance methods.

#### Scenario: Namespace and helper access do not require extra Dml instance fields
- **WHEN** a caller uses any public namespace on `Dml`
- **THEN** the namespace behavior is derived from `_context`, `_s3_client`, `_tempdirs`, and delegated helper logic without introducing additional private `Dml` instance attributes

#### Scenario: Dml public workflows do not depend on private helper methods
- **WHEN** a `Dml` repository, runtime, DAG, admin, or config workflow needs helper behavior such as ops dispatch, payload shaping, or revision binding
- **THEN** that helper behavior executes through module-level functions in `daggerml._internal.dml` rather than through `Dml._...` instance methods

#### Scenario: Namespace objects keep only Dml as private state
- **WHEN** a caller inspects the namespace objects exposed by `Dml`
- **THEN** each namespace object keeps only `._dml` as private instance state
- **AND** namespace helper behavior does not rely on additional private attrs or private helper methods on the namespace object

#### Scenario: Remote sync workflows reuse the Dml-owned S3 client
- **WHEN** a caller invokes `dml.fetch`, `dml.pull`, or `dml.push`
- **THEN** the workflow uses the `Dml` instance's private `_s3_client` instead of requiring a public `s3_client` method parameter
