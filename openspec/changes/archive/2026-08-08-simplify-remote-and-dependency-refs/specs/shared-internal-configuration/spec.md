## MODIFIED Requirements

### Requirement: Canonical config parameters are reduced to one normalized set
The system SHALL normalize supported configuration inputs into canonical internal parameters including `project.home`, `db.path`, `remote.root`, `user`, `default_branch`, hooks, `config_home`, and ephemeral `execution.id`. The canonical model SHALL NOT include project URI identity or named ordinary remotes.

#### Scenario: Remote root is the sole project endpoint parameter
- **WHEN** remote-backed project configuration is resolved
- **THEN** the canonical endpoint parameter is `remote.root`

#### Scenario: Branch and revision source are not config parameters
- **WHEN** project configuration is resolved
- **THEN** checkout branch and local/remote/dependency revision selection remain operation state rather than canonical config

#### Scenario: Execution identity remains canonical runtime state
- **WHEN** execution-aware runtime configuration is resolved
- **THEN** the canonical model includes ephemeral `execution.id`

### Requirement: Multiple config sources normalize into the shared internal model
The system SHALL normalize explicit arguments, environment variables, project-local `.dml/config.json`, and global `config.json` through one shared resolver. Ephemeral runtime fields such as `execution.id` SHALL remain explicit-or-environment only.

#### Scenario: Project and global JSON config feed shared resolution
- **WHEN** project/runtime configuration is resolved
- **THEN** project-local and global JSON config feed the same precedence path

#### Scenario: Environment values normalize centrally
- **WHEN** environment variables provide supported values
- **THEN** the shared resolver maps them into the canonical model

#### Scenario: Execution identity remains ephemeral
- **WHEN** `execution.id` is resolved
- **THEN** it is not loaded from project or global config files

### Requirement: CLI explicit override names mirror canonical config parameters
The CLI SHALL name explicit configuration override flags after canonical parameters. It SHALL expose `--remote-root` where root override is supported and SHALL NOT expose `--remote-project`.

#### Scenario: Project-home flag maps to canonical parameter
- **WHEN** the CLI resolves an explicit local project path override
- **THEN** it forwards the project-home value as `project.home`

#### Scenario: Remote-root flag maps to canonical parameter
- **WHEN** the CLI resolves an explicit endpoint override
- **THEN** it forwards `--remote-root` as `remote.root`

#### Scenario: Removed project override is absent
- **WHEN** a user views generated global configuration options
- **THEN** `--remote-project` is not exposed

## REMOVED Requirements

### Requirement: Project URI is normalized and exposes helper accessors
**Reason**: The configuration model no longer contains project URI identity.
**Migration**: Use normalized `remote.root` for project synchronization and execution.
