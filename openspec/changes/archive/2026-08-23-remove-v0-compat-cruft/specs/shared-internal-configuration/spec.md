## MODIFIED Requirements

### Requirement: Canonical config parameters are reduced to one normalized set
The system SHALL normalize supported configuration inputs into the exact canonical internal parameters owned by the current resolver. The canonical model SHALL NOT include project URI identity, named ordinary remotes, removed environment inputs, or unknown parameters. Persisted global and project configuration SHALL reject unsupported keys instead of ignoring or preserving them.

#### Scenario: Remote root is the sole project endpoint parameter
- **WHEN** project configuration is resolved
- **THEN** the canonical endpoint parameter is `remote.root`

#### Scenario: Branch and revision source are not config parameters
- **WHEN** remote-backed configuration is resolved
- **THEN** checkout branch and local, remote, or dependency revision selection remain operation state rather than canonical config

#### Scenario: Unknown persisted key is rejected
- **WHEN** global or project JSON contains a key outside the canonical persisted configuration set
- **THEN** resolution fails with an error identifying the unsupported key and source file

#### Scenario: Removed project identity is rejected
- **WHEN** persisted configuration contains `remote.project`, `remote.remotes`, or another removed project-identity field
- **THEN** resolution fails instead of ignoring, preserving, or translating it

### Requirement: Multiple config sources normalize into the shared internal model
The system SHALL treat explicit arguments, currently supported environment variables, project-local `.dml/config.json`, and global `<config_home>/config.json` as sources that feed the shared internal configuration model. Each persisted source SHALL be validated before precedence resolution. Source-specific loading may differ, but normalization and precedence MUST be centralized in the shared internal resolver. Ephemeral runtime fields SHALL NOT be loaded from persisted configuration.

#### Scenario: Project-local and global config feed shared resolution
- **WHEN** a frontend resolves configuration for an operation in a project directory
- **THEN** project-local `.dml/config.json` and applicable global JSON config are validated and loaded through the same shared resolution path

#### Scenario: Environment values are normalized centrally
- **WHEN** configuration is resolved from supported environment variables
- **THEN** the shared internal resolver, not the frontend, maps those values into the canonical internal configuration model

#### Scenario: Removed environment variable has no compatibility mapping
- **WHEN** an environment variable from a removed configuration model is present
- **THEN** it does not populate, alias, or override any canonical parameter

#### Scenario: Init project layout creation delegates to shared internal helper
- **WHEN** the shared `Dml` init workflow must create missing project layout artifacts
- **THEN** it delegates filesystem bootstrap work to shared project-layout logic instead of duplicating JSON config writes

#### Scenario: Init resolves explicit options through shared resolver
- **WHEN** a caller provides init-time configuration options
- **THEN** the shared `Dml` init workflow resolves them through the shared internal resolver before mutating project state

### Requirement: CLI explicit override names mirror canonical config parameters
The CLI SHALL name explicit configuration override flags after the current canonical parameters they populate. It SHALL NOT expose aliases or flags for removed configuration parameters.

#### Scenario: Project-home flag maps to canonical parameter
- **WHEN** the CLI resolves an explicit local project path override
- **THEN** it reads that value from a flag named after `project.home`
- **AND** it forwards the value into shared resolution as `project.home`

#### Scenario: Remote-root flag maps to canonical parameter
- **WHEN** the CLI resolves an explicit remote project override
- **THEN** it reads that value from a flag named after `remote.root`
- **AND** it forwards the value into shared resolution as `remote.root`

#### Scenario: Removed override has no alias
- **WHEN** CLI arguments are generated from current workflows
- **THEN** removed names such as `--remote-project` are not accepted or displayed

## REMOVED Requirements

### Requirement: Project URI is normalized and exposes helper accessors
**Reason**: The current v0 model has no persisted owner/project URI identity; `remote.root` is the sole project endpoint and checkout identity is repository state.
**Migration**: None. Configure `remote.root` and use local, remote-tracking, or dependency revision selectors.
