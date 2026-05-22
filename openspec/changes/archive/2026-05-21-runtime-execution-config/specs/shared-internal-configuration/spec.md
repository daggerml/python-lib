## MODIFIED Requirements

### Requirement: Canonical config parameters are reduced to one normalized set
The system SHALL normalize supported configuration inputs into the canonical internal parameters `project.home`, `remote.project`, `db.path`, `remote.root`, `user`, `default_branch`, `config_home`, and ephemeral runtime field `execution.id`.

#### Scenario: Execution identity is part of the canonical runtime config model
- **WHEN** execution-aware runtime code resolves session configuration
- **THEN** the canonical internal model includes `execution.id`
- **AND** that field participates in the same resolved runtime config object as the other canonical parameters

### Requirement: Multiple config sources normalize into the shared internal model
The system SHALL treat explicit arguments, environment variables, project-local config, and global config as sources that feed the shared internal configuration model. Source-specific loading may differ, but normalization and precedence MUST be centralized in the shared internal resolver.

#### Scenario: Execution identity is explicit-or-env only
- **WHEN** `execution.id` is resolved
- **THEN** the shared internal resolver applies `explicit > environment > null`
- **AND** it does not load `execution.id` from project-local or global config files
