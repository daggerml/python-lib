## MODIFIED Requirements

### Requirement: Remote-aware components require explicit remote configuration
The system SHALL require explicit remote configuration at the constructor or helper boundary for any runtime or ops component that performs remote-backed behavior. Remote-aware interfaces MUST NOT model remote configuration as optional, MUST NOT provide `None` defaults for required remote parameters, and MUST receive normalized `remote.uri` configuration from the shared internal configuration resolver rather than reading raw environment variables or project config files themselves.

#### Scenario: Remote-aware ops constructor requires remote URI
- **WHEN** a remote-aware ops type is defined
- **THEN** its constructor signature requires a concrete normalized remote URI argument rather than an optional remote parameter

#### Scenario: Remote-aware runtime helper requires remote configuration
- **WHEN** a runtime helper delegates to remote-backed behavior
- **THEN** it passes explicit remote configuration to the remote-aware component it constructs

#### Scenario: Remote-aware component does not resolve env vars directly
- **WHEN** a remote-aware runtime or ops component is used in a remote-backed flow
- **THEN** it receives already-resolved remote configuration from its caller instead of inspecting `DML_REMOTE`, older remote env-var forms, or project config files directly
