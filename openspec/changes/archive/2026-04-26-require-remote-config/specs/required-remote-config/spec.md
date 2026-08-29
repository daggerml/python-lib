## ADDED Requirements

### Requirement: Remote-aware components require explicit remote configuration
The system SHALL require explicit remote configuration at the constructor or helper boundary for any runtime or ops component that performs remote-backed behavior. Remote-aware interfaces MUST NOT model remote configuration as optional and MUST NOT provide `None` defaults for required remote parameters.

#### Scenario: Remote-aware ops constructor requires remote root
- **WHEN** a remote-aware ops type is defined
- **THEN** its constructor signature requires a concrete remote root argument rather than an optional remote-root value

#### Scenario: Remote-aware runtime helper requires remote configuration
- **WHEN** a runtime helper delegates to remote-backed behavior
- **THEN** it passes explicit remote configuration to the remote-aware component it constructs

### Requirement: Local-only setup uses local-only primitives
The system SHALL use local-only primitives for code paths that only need local transaction or repository setup behavior and do not perform remote-backed operations.

#### Scenario: Local setup helper avoids remote-aware constructor
- **WHEN** a helper only creates local commits, heads, trees, or transactions
- **THEN** it uses a local-only primitive instead of constructing a remote-aware ops type without remote configuration
