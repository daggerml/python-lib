## MODIFIED Requirements

### Requirement: Multiple config sources normalize into the shared internal model
The system SHALL treat explicit arguments, environment variables, project-local config, and global config as sources that feed the shared internal configuration model. Source-specific loading may differ, but normalization and precedence MUST be centralized in the shared internal resolver.

#### Scenario: Project-local and global config feed shared resolution
- **WHEN** a frontend resolves configuration for an operation in a project directory
- **THEN** project-local `.dml/config.toml` and any applicable global config inputs are loaded as sources for the same shared internal resolution path

#### Scenario: Environment values are normalized centrally
- **WHEN** configuration is resolved from environment variables
- **THEN** the shared internal resolver, not the frontend, maps those values into the canonical internal configuration model

#### Scenario: Init project layout creation delegates to shared internal helper
- **WHEN** the shared `Dml` init/bootstrap workflow must create missing project layout artifacts for a local project
- **THEN** it delegates filesystem bootstrap work to shared internal project-layout helper logic instead of duplicating directory and config-file writes across orchestration helpers

#### Scenario: Init resolves explicit options through shared resolver
- **WHEN** a caller provides init-time options for project/runtime configuration
- **THEN** the shared `Dml` init/bootstrap workflow resolves them through the shared internal resolver before mutating project state
