## MODIFIED Requirements

### Requirement: Remote descriptor rejects incompatible layouts
The system SHALL store the exact current descriptor at each one-project endpoint root with `schema` set to the non-boolean integer `0`. First publication SHALL inspect the entire endpoint root, including sibling project and execution prefixes, and conditionally create the descriptor only when no object exists anywhere under that root. A present descriptor SHALL match the exact version-0 shape. A missing descriptor on a non-empty root or any malformed, extra-field, boolean, nonzero, or otherwise unsupported descriptor SHALL fail before reading or mutating remote state.

#### Scenario: Current descriptor is accepted
- **WHEN** an endpoint descriptor exactly declares schema integer `0` and the current one-project layout fields
- **THEN** remote operations may use its direct refs, CAS, cache, execution, edge, and IO paths

#### Scenario: First push initializes empty root
- **WHEN** first publication observes no descriptor and no object anywhere under the endpoint root
- **THEN** it conditionally creates the exact version-0 descriptor and proceeds only if that initialization wins any race

#### Scenario: Missing descriptor on non-empty root is rejected
- **WHEN** the descriptor is absent but an object exists under the endpoint execution prefix
- **THEN** initialization fails without writing a descriptor or changing that object

#### Scenario: Legacy descriptor is rejected
- **WHEN** a descriptor has another version, a boolean version, a missing or extra field, or another field value
- **THEN** remote operations fail without parsing, migrating, or modifying endpoint state

### Requirement: Branch heads are mutable and project tags are immutable
The system SHALL allow direct endpoint branch head refs to move through safe update operations. The system SHALL reject a non-forced attempt to overwrite an existing tag ref and SHALL allow a forced attempt to replace it.

#### Scenario: Branch head update
- **WHEN** a push safely advances branch `main`
- **THEN** `refs/heads/main.json` may be replaced by the new branch head payload

#### Scenario: Tag overwrite rejected
- **WHEN** `refs/tags/v1.0.json` already exists
- **THEN** publishing tag `v1.0` fails without changing the existing tag ref

#### Scenario: Forced tag overwrite succeeds
- **WHEN** `refs/tags/v1.0.json` already exists and push requests force
- **THEN** publishing tag `v1.0` replaces the existing tag ref with the requested commit

### Requirement: Project refs use typed object ref payloads
The system SHALL encode direct endpoint branch and tag refs as typed remote ref payloads containing exactly `ref.to`, `created`, and `metadata`. Branch and tag refs SHALL point to `commit` objects and SHALL fail before writing if the target object is missing or is not a commit root.

#### Scenario: Project branch ref payload
- **WHEN** branch `main` is written
- **THEN** `refs/heads/main.json` contains `ref.to = "commit:<oid>"`, integer `created`, and object `metadata`

#### Scenario: Project tag ref payload
- **WHEN** tag `v1.0` is written
- **THEN** `refs/tags/v1.0.json` contains `ref.to = "commit:<oid>"`, integer `created`, and object `metadata`

#### Scenario: Project ref root validation fails closed
- **WHEN** a branch or tag ref would point to a missing object or a non-commit root
- **THEN** the write fails without creating or updating the ref

### Requirement: Shared remote CAS
The system SHALL store immutable CAS objects under `cas/sha256/<aa>/<bb>/<oid>` for the sole project endpoint, independent of branch or tag.

#### Scenario: Two projects reference same object
- **WHEN** two endpoint refs point to commit graphs that include the same CAS object
- **THEN** the remote stores that CAS object at one shared CAS path

### Requirement: Global DML config
The system SHALL load global DML configuration as JSON from `<config_home>/config.json`, where `config_home` resolves from explicit input, `DML_CONFIG_HOME`, `XDG_CONFIG_HOME/dml`, then `~/.config/dml`. It SHALL NOT read a TOML compatibility path.

#### Scenario: DML config home wins over fallback
- **WHEN** explicit config home is absent and `DML_CONFIG_HOME` is set
- **THEN** the system reads global config from `$DML_CONFIG_HOME/config.json`

#### Scenario: DML config home wins
- **WHEN** `config_home` is provided explicitly or through `DML_CONFIG_HOME`
- **THEN** the system reads global config from the resolved `<config_home>/config.json`

#### Scenario: XDG config home fallback
- **WHEN** explicit config home and `DML_CONFIG_HOME` are absent and `XDG_CONFIG_HOME` is set
- **THEN** the system reads global config from `$XDG_CONFIG_HOME/dml/config.json`

#### Scenario: Default config fallback
- **WHEN** no explicit, DML, or XDG config home is set
- **THEN** the system reads global config from `~/.config/dml/config.json`

### Requirement: Global user defaults
The system SHALL use supported global JSON configuration for the current user and default branch name without deriving owner/project identity.

#### Scenario: Default project owner
- **WHEN** global JSON config contains a supported `user` value
- **THEN** the resolved user is that value without deriving or persisting project-owner identity

#### Scenario: Default branch
- **WHEN** global JSON config contains `default.branch_name = "main"`
- **THEN** the default branch is `main`

### Requirement: Local remote config
The system SHALL store project-local configuration at `.dml/config.json` using only supported current keys. `remote.root` SHALL be the sole project endpoint setting. Local config SHALL NOT contain project URI, owner, project name, named remotes, checkout branch, or removed compatibility fields, and unknown persisted keys SHALL be rejected.

#### Scenario: Resolve origin main
- **WHEN** local config contains `remote.root` and local HEAD is attached to `main`
- **THEN** push resolves direct endpoint branch `main` without an `origin` alias or project identity

#### Scenario: Project fields are stored
- **WHEN** local project configuration is written
- **THEN** `.dml/config.json` contains only supported current keys and no project URI, owner, or name fields

#### Scenario: Remote fields are stored
- **WHEN** local project configuration records its endpoint
- **THEN** `.dml/config.json` contains supported `remote.root` JSON and no named-remote or checkout fields

#### Scenario: Reject branch-qualified local project URI
- **WHEN** local JSON contains a project URI, whether branchless or selector-qualified
- **THEN** configuration validation fails without preserving or translating it

### Requirement: Config waterfall precedence
The system SHALL resolve current configurable values using explicit API or CLI arguments, supported environment variables, project JSON, global JSON, then defaults. Checkout-state selection SHALL remain outside this waterfall and resolve from `.dml/HEAD`.

#### Scenario: Explicit value wins over environment
- **WHEN** an explicit canonical value and its supported environment variable are both provided
- **THEN** resolution uses the explicit value

#### Scenario: Environment does not override checkout state
- **WHEN** supported configuration environment variables are resolved
- **THEN** checkout still derives from `.dml/HEAD`

#### Scenario: Config used as fallback for non-checkout values
- **WHEN** explicit and environment values are absent
- **THEN** resolution uses supported project JSON, global JSON, then default values

#### Scenario: Remote storage env vars override config
- **WHEN** supported `DML_REMOTE_ROOT` and persisted `remote.root` are both present
- **THEN** resolution uses `DML_REMOTE_ROOT`

### Requirement: Supported DML environment variables
The system SHALL support only `DML_CONFIG_HOME`, `DML_DB_PATH`, `DML_DEFAULT_DB_MAP_SIZE_HEADROOM`, `DML_DEFAULT_DB_MAP_SIZE_MAX`, `DML_DEFAULT_BRANCH_NAME`, `DML_REMOTE_PRUNE_AGE_SECONDS`, `DML_PROJECT_HOME`, `DML_REMOTE_ROOT`, `DML_REMOTE_FETCH_WORKERS`, and `DML_USER` as current configuration inputs. Retired `DML_DEFAULT_BRANCH`, `DML_PROJECT_NAME`, `DML_PROJECT_OWNER`, `DML_REMOTE_PROJECT`, `DML_REMOTE_NAME`, `DML_BRANCH`, `DML_REMOTE`, `DML_REMOTE_BUCKET`, `DML_REMOTE_PREFIX`, `DML_REPO`, `DML_DYNAMODB_TABLE`, `DML_REMOTE_CACHE`, and `DML_HOOK` SHALL NOT influence resolution, act as aliases, or be synthesized as hook context.

#### Scenario: Global config home override
- **WHEN** `DML_CONFIG_HOME` is set
- **THEN** it supplies the canonical global config directory

#### Scenario: Existing user env remains supported
- **WHEN** `DML_USER` is set
- **THEN** it supplies the canonical user value without project-owner semantics

#### Scenario: DML_BRANCH is rejected as unsupported
- **WHEN** `DML_BRANCH` is set
- **THEN** it does not affect checkout or configuration resolution

#### Scenario: Project env overrides config
- **WHEN** removed project-identity variables are set
- **THEN** they do not override or populate any canonical config value

#### Scenario: Remote env overrides config
- **WHEN** `DML_REMOTE_ROOT` is set
- **THEN** it supplies the canonical `remote.root` value

#### Scenario: Hook context env is provided by DML
- **WHEN** a current operation executes without a hook capability
- **THEN** no retired hook, project-name, project-owner, or named-remote context contract is synthesized

### Requirement: Project commands use project-local state and current env names only
The system SHALL resolve project-local state from `<project-directory>/.dml/config.json`, `.dml/HEAD`, local refs, and `.dml/db/`, and SHALL use only current supported environment variables.

#### Scenario: Project config comes from the project directory
- **WHEN** a project command resolves local config
- **THEN** it reads `<project-directory>/.dml/config.json`

#### Scenario: DML_REPO is not used for project database
- **WHEN** a project command opens the object database
- **THEN** it uses `<project-directory>/.dml/db/` and does not consult `DML_REPO`

#### Scenario: DML_REMOTE_ROOT is not used for named remotes
- **WHEN** a project command requires remote storage
- **THEN** it uses `DML_REMOTE_ROOT` as canonical `remote.root` and does not construct a named remote

#### Scenario: Removed execution/cache env vars are ignored
- **WHEN** removed execution or cache environment variables are set
- **THEN** they do not affect the current project command

### Requirement: Project directory initialization
The system SHALL initialize current local project state under `<project-directory>/.dml/` without creating obsolete configuration or synthetic history.

#### Scenario: Init creates DML directory
- **WHEN** initialization succeeds
- **THEN** it creates `.dml/config.json`, `.dml/HEAD`, `.dml/refs/`, and `.dml/db/` using only current formats

#### Scenario: Init refuses existing child directory
- **WHEN** child-directory initialization targets an existing directory
- **THEN** it fails without altering that directory and directs the caller to initialize in place

#### Scenario: Init here creates DML directory in current directory
- **WHEN** in-place initialization succeeds
- **THEN** it creates the current `.dml/` layout in that directory

#### Scenario: Init here uses provided project name
- **WHEN** obsolete project-name input is supplied to in-place initialization
- **THEN** no project identity is persisted or inferred from that name

#### Scenario: Init creates DML gitignore
- **WHEN** initialization succeeds
- **THEN** it creates the current `.dml/.gitignore` policy for local mutable state

#### Scenario: Init creates unborn attached HEAD
- **WHEN** a repository is initialized before its first commit
- **THEN** `.dml/HEAD` is attached to the default branch and the corresponding local branch ref does not exist yet

#### Scenario: Init does not create initial empty commit
- **WHEN** initialization succeeds
- **THEN** local storage does not contain a synthetic empty commit solely to materialize the branch tip

#### Scenario: Obsolete config is not created
- **WHEN** initialization writes project configuration
- **THEN** it does not create `.dml/config.toml` or any project-identity compatibility field

#### Scenario: Detached init without commit is rejected
- **WHEN** initialization is requested in detached mode before any commit exists
- **THEN** initialization fails because detached HEAD requires a concrete commit

## REMOVED Requirements

### Requirement: DML URIs track fetched remote refs
**Reason**: Current tracking uses direct `remote.root` and dependency namespaces rather than owner/project URI or named-remote selectors.
**Migration**: None. Use local `remote` tracking refs or named dependency refs.

### Requirement: Remote operations parse DML URIs
**Reason**: `remote.root` already identifies the sole project endpoint and direct ref paths do not derive from project URIs.
**Migration**: None. Configure `remote.root` and address direct branch or tag names.

### Requirement: Project creation owner default
**Reason**: The v0 project model has no persisted owner/project identity.
**Migration**: None. `user` remains authorship context and does not create endpoint identity.

### Requirement: Fetch updates remote-tracking heads
**Reason**: The named-remote and URI-keyed fetch model is replaced by direct `remote.root` and dependency tracking behavior already specified by this capability.
**Migration**: None. Fetch from `remote.root` or a configured dependency.

### Requirement: Project sync requires a configured named remote
**Reason**: Project synchronization requires `remote.root`, not an ordinary named remote or default `origin` alias.
**Migration**: None. Configure `remote.root`.

### Requirement: Init shell hooks
**Reason**: Hook and project-identity configuration from the retired TOML project model is not part of the current v0 configuration surface.
**Migration**: None. No compatibility hook variables or TOML hook configuration are retained.
