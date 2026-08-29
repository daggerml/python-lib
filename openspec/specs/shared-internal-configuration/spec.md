## Purpose
Define the canonical configuration model shared by the Python API and CLI.

## Requirements

### Requirement: API and CLI use one shared internal configuration model
The system SHALL resolve configuration through one canonical internal configuration model owned by `_internal`. Both `daggerml.api` and the CLI SHALL use that shared internal resolver rather than maintaining frontend-specific configuration semantics.

#### Scenario: API and CLI share resolution behavior
- **WHEN** API code and CLI code resolve the same explicit values, environment variables, and config-file inputs
- **THEN** they produce the same resolved internal configuration for the underlying operation

#### Scenario: Frontends remain thin bindings
- **WHEN** a frontend prepares to invoke shared internal operations
- **THEN** it delegates configuration precedence, validation, and derivation to shared internal configuration code instead of re-implementing those rules locally

### Requirement: One resolver supports `project/runtime` and `global` scopes
The system SHALL expose one shared internal resolver that supports `project/runtime` and `global` scopes. Both scopes MUST use the same precedence model, but they load different config-file layers according to scope.

#### Scenario: Project scope loads project and global config layers
- **WHEN** configuration is resolved in `project/runtime` scope
- **THEN** the resolver applies `explicit > environment variables > project config > global config > defaults`

#### Scenario: Global scope omits project config
- **WHEN** configuration is resolved in `global` scope
- **THEN** the resolver applies `explicit > environment variables > global config > defaults` without requiring a project config file

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

### Requirement: Dml exposes a canonical config-var construction path
The system SHALL expose a `Dml` construction path that accepts the flattened canonical config-var keys used by the shared internal resolver, while the Python constructor surface uses Python-friendly keyword names.

#### Scenario: Canonical config-var dict feeds shared resolver directly
- **WHEN** a caller provides a flattened dictionary of canonical config vars
- **THEN** the `Dml` config-var factory forwards those keys into shared configuration resolution without requiring caller-side renaming

#### Scenario: Python constructor does not require dot-notation kwargs
- **WHEN** a caller constructs `Dml` through Python keyword arguments
- **THEN** the caller uses Python-friendly parameter names rather than canonical dot-notation keys

### Requirement: DB path can be overridden but defaults from project home
The system SHALL resolve `db.path` with the same precedence as other `project/runtime` parameters, and when no higher-precedence value is provided it SHALL default to `<project.home>/.dml/db/`.

#### Scenario: Explicit DB path overrides dynamic default
- **WHEN** `db.path` is provided explicitly or through `DML_DB_PATH`
- **THEN** the resolved config uses that DB path instead of deriving it from `project.home`

#### Scenario: DB path defaults from project home
- **WHEN** `db.path` is not provided and resolved config includes `project.home`
- **THEN** `db.path` resolves to `<project.home>/.dml/db/`

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
