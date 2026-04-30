## MODIFIED Requirements

### Requirement: Multiple config sources normalize into the shared internal model
The system SHALL treat explicit arguments, environment variables, project-local config, and global config as sources that feed the shared internal configuration model. Source-specific loading may differ, but normalization and precedence MUST be centralized in the shared internal resolver.

#### Scenario: Project-local and global config feed shared resolution
- **WHEN** a frontend resolves configuration for an operation in a project directory
- **THEN** project-local `.dml/config.toml` and any applicable global config inputs are loaded as sources for the same shared internal resolution path

#### Scenario: Environment values are normalized centrally
- **WHEN** configuration is resolved from environment variables
- **THEN** the shared internal resolver, not the frontend, maps those values into the canonical internal configuration model

#### Scenario: Init resolves explicit options through shared resolver
- **WHEN** a caller provides init-time options for project/runtime configuration
- **THEN** `DmlOps.init` resolves them through the shared internal resolver before mutating project state

### Requirement: Project URI is normalized and exposes helper accessors
The system SHALL normalize `project.uri` so that resolved project configuration always includes a branch and never a tag. The resolved config object SHALL expose a `project.branch` helper derived from the normalized URI.

#### Scenario: Missing branch normalizes from default branch
- **WHEN** `project.uri` is provided without a branch in `project/runtime` scope
- **THEN** the resolver appends the effective default branch to the normalized `project.uri`

#### Scenario: Tag URI is rejected for project context
- **WHEN** `project.uri` is provided with a tag selector
- **THEN** project configuration resolution fails because active project context must target a branch, not an immutable tag

#### Scenario: Project branch helper is derived from normalized URI
- **WHEN** resolved configuration includes `project.uri`
- **THEN** `project.branch` returns the branch encoded in the normalized URI rather than reading a standalone branch config parameter

#### Scenario: Init fails when required project URI cannot resolve validly
- **WHEN** init flow requires `project.uri` for bootstrap behavior but resolver output leaves it invalid or unresolved
- **THEN** `DmlOps.init` fails before creating or mutating repository state
