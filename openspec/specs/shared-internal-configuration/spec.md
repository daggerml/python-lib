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
The system SHALL normalize supported configuration inputs into the canonical internal parameters `project.home`, `remote.project`, `db.path`, `remote.root`, `user`, `default_branch`, `hooks.post-init`, `hooks.post-clone`, and `config_home`.

#### Scenario: Branch context is not a canonical config parameter
- **WHEN** project configuration is resolved
- **THEN** the canonical internal model does not include a separate branch-selection parameter and does not derive the active checkout branch from configuration

#### Scenario: Legacy overlapping remote parameters are not canonical
- **WHEN** remote-backed configuration is resolved
- **THEN** the canonical remote parameter is `remote.root` rather than separate `remote.bucket` or `remote.prefix` parameters

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

### Requirement: Project URI is normalized and exposes helper accessors
The system SHALL normalize and canonicalize local `remote.project` as an optional branchless project identity through shared revision URI utilities. Resolved configuration SHALL treat checkout state as repository state owned by `.dml/HEAD` rather than as a selector embedded in config.

#### Scenario: Local project URI remains branchless when configured
- **WHEN** `remote.project` is resolved for local project configuration
- **THEN** shared configuration preserves canonical branchless form `dml://<owner>/<project>`

#### Scenario: Local project configuration may omit project URI
- **WHEN** local project configuration omits `remote.project`
- **THEN** shared configuration resolves successfully without deriving project identity from other inputs

#### Scenario: Tag or branch selector is not accepted for local project config
- **WHEN** local project configuration provides `remote.project` with a branch or tag selector
- **THEN** configuration resolution fails instead of translating that selector into checkout state

#### Scenario: Project helper accessors do not expose current checkout branch
- **WHEN** resolved configuration includes `remote.project`
- **THEN** helper accessors expose project identity only and do not treat config as the source of the active branch or detached commit

### Requirement: DB path can be overridden but defaults from project home
The system SHALL resolve `db.path` with the same precedence as other `project/runtime` parameters, and when no higher-precedence value is provided it SHALL default to `<project.home>/.dml/db/`.

#### Scenario: Explicit DB path overrides dynamic default
- **WHEN** `db.path` is provided explicitly or through `DML_DB_PATH`
- **THEN** the resolved config uses that DB path instead of deriving it from `project.home`

#### Scenario: DB path defaults from project home
- **WHEN** `db.path` is not provided and resolved config includes `project.home`
- **THEN** `db.path` resolves to `<project.home>/.dml/db/`

### Requirement: CLI limitations caused by serialization are documented, not treated as config divergence
The system SHALL document operations that remain unavailable in the CLI because command-line serialization cannot faithfully represent the required Python-level inputs or outputs. These omissions MUST NOT create a separate CLI-specific configuration model.

#### Scenario: Serialization-limited API behavior stays API-only
- **WHEN** an operation such as `start_fn` depends on Python object or function serialization that the CLI cannot represent cleanly
- **THEN** the documentation identifies that operation as unavailable in the CLI while preserving the shared internal configuration model for supported operations

#### Scenario: Missing CLI feature does not imply different config rules
- **WHEN** a capability is supported by both API and CLI
- **THEN** both frontends use the same shared internal configuration rules for that capability

### Requirement: CLI explicit override names mirror canonical config parameters
The CLI SHALL name explicit configuration override flags after the canonical parameters they populate in the shared internal resolver whenever those parameters are exposed directly to users.

#### Scenario: Project-home flag maps to canonical parameter
- **WHEN** the CLI resolves an explicit local project path override
- **THEN** it reads that value from a flag named after `project.home`
- **AND** it forwards the value into shared resolution as `project.home`

#### Scenario: Remote-root flag maps to canonical parameter
- **WHEN** the CLI resolves an explicit remote project override
- **THEN** it reads that value from a flag named after `remote.root`
- **AND** it forwards the value into shared resolution as `remote.root`

#### Scenario: Existing canonical names remain unchanged
- **WHEN** the CLI exposes other explicit config-shaped overrides such as `--remote-project` or `--config-home`
- **THEN** those flags continue using the established canonical names rather than introducing alternate aliases
