### Requirement: Remote-aware components require explicit remote configuration
The system SHALL require explicit remote configuration at the constructor or helper boundary for any runtime or ops component that performs remote-backed behavior. Remote-aware interfaces MUST receive normalized `remote.root` configuration from the shared internal configuration resolver rather than reading raw environment variables or project config files themselves.

#### Scenario: Remote-aware ops constructor requires remote URI
- **WHEN** a remote-aware ops type is defined
- **THEN** its constructor signature requires a concrete normalized remote URI argument rather than an optional remote parameter

#### Scenario: Remote-aware runtime helper requires remote configuration
- **WHEN** a runtime helper delegates to remote-backed behavior
- **THEN** it passes explicit remote configuration to the remote-aware component it constructs

#### Scenario: Remote-aware component does not resolve env vars directly
- **WHEN** a remote-aware runtime or ops component is used in a remote-backed flow
- **THEN** it receives already-resolved remote configuration from its caller instead of inspecting `DML_REMOTE`, older remote env-var forms, or project config files directly

#### Scenario: Init fails when required remote URI cannot resolve validly
- **WHEN** the shared `Dml` init/bootstrap workflow requires remote-backed bootstrap behavior and shared config resolution does not produce a valid `remote.root`
- **THEN** init fails with a configuration error instead of proceeding with unresolved or implicit remote configuration

### Requirement: Project sync operations require project identity in addition to remote root
The system SHALL require configured `remote.project` for project-addressed sync behavior such as push, pull, fetch, and init-time project checkout. These operations MUST fail closed when `remote.root` exists but `remote.project` is absent.

#### Scenario: Remote-backed mutation without project identity remains allowed
- **WHEN** a runtime or mutation operation requires only remote-backed storage or execution capability
- **THEN** configured `remote.root` is sufficient even when `remote.project` is absent

#### Scenario: Project sync operation without project identity is rejected
- **WHEN** a project-addressed sync operation is requested and resolved config has no `remote.project`
- **THEN** the operation fails with a descriptive error instead of deriving project identity implicitly
