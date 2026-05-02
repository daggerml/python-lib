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
The system SHALL normalize supported configuration inputs into the canonical internal parameters `project.home`, `project.uri`, `db.path`, `remote.uri`, `user`, `default_branch`, `hooks.post-init`, `hooks.post-clone`, and `config_home`.

#### Scenario: Legacy overlapping branch parameter is not canonical
- **WHEN** project configuration is resolved
- **THEN** branch context is carried by normalized `project.uri` rather than by a separate canonical `branch` parameter

#### Scenario: Legacy overlapping remote parameters are not canonical
- **WHEN** remote-backed configuration is resolved
- **THEN** the canonical remote parameter is `remote.uri` rather than separate `remote.root`, `remote.bucket`, or `remote.prefix` parameters

### Requirement: Multiple config sources normalize into the shared internal model
The system SHALL treat explicit arguments, environment variables, project-local config, and global config as sources that feed the shared internal configuration model. Source-specific loading may differ, but normalization and precedence MUST be centralized in the shared internal resolver.

#### Scenario: Project-local and global config feed shared resolution
- **WHEN** a frontend resolves configuration for an operation in a project directory
- **THEN** project-local `.dml/config.toml` and any applicable global config inputs are loaded as sources for the same shared internal resolution path

#### Scenario: Environment values are normalized centrally
- **WHEN** configuration is resolved from environment variables
- **THEN** the shared internal resolver, not the frontend, maps those values into the canonical internal configuration model

#### Scenario: Init project layout creation delegates to shared internal helper
- **WHEN** `DmlOps.init` must create missing project layout artifacts for a local project
- **THEN** it delegates filesystem bootstrap work to shared internal project-layout helper logic instead of duplicating directory and config-file writes in ops code

#### Scenario: Init resolves explicit options through shared resolver
- **WHEN** a caller provides init-time options for project/runtime configuration
- **THEN** `DmlOps.init` resolves them through the shared internal resolver before mutating project state

### Requirement: Project URI is normalized and exposes helper accessors
The system SHALL normalize and canonicalize `project.uri` through shared revision URI utilities. Resolved project configuration MAY target a branch or a tag. The resolved config object SHALL continue to expose helper accessors for the effective project selector.

#### Scenario: Missing selector parses as default branch
- **WHEN** `project.uri` is provided without a branch or tag in `project/runtime` scope
- **THEN** shared revision URI parsing resolves it to a fully realized branch selector using the effective default branch

#### Scenario: Tag URI is accepted for project context
- **WHEN** `project.uri` is provided with a tag selector
- **THEN** project configuration resolution succeeds and preserves canonical tag form

#### Scenario: Project helper accessors derive from canonical URI
- **WHEN** resolved configuration includes `project.uri`
- **THEN** helper accessors derive selector values from canonical parsed URI rather than standalone duplicated parsing logic

#### Scenario: Init fails when required project URI cannot resolve validly
- **WHEN** init flow requires `project.uri` for bootstrap behavior but resolver output leaves it invalid or unresolved
- **THEN** `DmlOps.init` fails before creating or mutating repository state

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
