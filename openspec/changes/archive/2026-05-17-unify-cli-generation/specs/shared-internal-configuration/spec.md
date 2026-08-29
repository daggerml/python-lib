## MODIFIED Requirements

### Requirement: CLI limitations caused by serialization are documented, not treated as config divergence
The system SHALL document only those public `Dml` workflows that remain unavailable in the CLI because their public parameter types cannot be generated faithfully from command-line input. These omissions MUST NOT create a separate CLI-specific configuration model.

#### Scenario: Unsupported public parameter types remain API-only
- **WHEN** a public workflow exposes parameter types that the CLI generator cannot represent cleanly
- **THEN** the documentation identifies that workflow as unavailable in the CLI while preserving the shared internal configuration model for supported operations

#### Scenario: CLI-generatable public workflows are not excluded for historical reasons
- **WHEN** a public workflow uses only CLI-generatable parameter types
- **THEN** the CLI exposes that workflow instead of treating it as API-only based on prior manual CLI limitations

#### Scenario: Missing CLI feature does not imply different config rules
- **WHEN** a capability is supported by both API and CLI
- **THEN** both frontends use the same shared internal configuration rules for that capability
