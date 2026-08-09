## ADDED Requirements

### Requirement: One-project remote refs namespace
The system SHALL store branch and tag refs directly under `refs/{heads,tags}/` within resolved `remote.root`. A remote root SHALL represent exactly one DML project and execution domain and SHALL NOT require owner or project selectors.

#### Scenario: Branch head path
- **WHEN** branch `main` is addressed at `remote.root`
- **THEN** its ref path is `refs/heads/main.json`

#### Scenario: Tag path
- **WHEN** tag `v1.0` is addressed at `remote.root`
- **THEN** its ref path is `refs/tags/v1.0.json`

### Requirement: Remote descriptor rejects incompatible layouts
The system SHALL store a versioned descriptor at each one-project endpoint root. First publication to a truly empty root SHALL conditionally create the current descriptor before other remote state. Missing descriptors on non-empty roots and unsupported or legacy descriptors SHALL be rejected before reading or mutating refs.

#### Scenario: Current descriptor is accepted
- **WHEN** an endpoint descriptor declares the supported one-project layout version
- **THEN** remote operations may use its direct refs, CAS, cache, and execution paths

#### Scenario: First push initializes empty root
- **WHEN** first publication observes a root with no descriptor or DML transport state
- **THEN** it conditionally creates the current descriptor and proceeds only if that initialization wins any race

#### Scenario: Missing descriptor on non-empty root is rejected
- **WHEN** a root has DML transport state but no descriptor
- **THEN** remote operations fail without modifying that root

#### Scenario: Legacy descriptor is rejected
- **WHEN** an endpoint identifies the prior owner/project layout
- **THEN** the operation fails with migration guidance before reading or writing project refs

### Requirement: Remote root is the sole project endpoint
The system SHALL use resolved `remote.root` for project synchronization, CAS, cache, execution coordination, and remote maintenance. Named dependencies SHALL be import-only endpoints and SHALL NOT replace `remote.root` for these operations.

#### Scenario: Project synchronization requires remote root
- **WHEN** fetch without `dep`, pull, push, or remote administration is requested without valid `remote.root`
- **THEN** the operation fails with a descriptive configuration error

#### Scenario: Runtime uses remote root
- **WHEN** remote-backed execution or cache behavior is requested
- **THEN** the operation uses the same resolved `remote.root`

### Requirement: Local tracking namespaces follow endpoint capability
The system SHALL store fetched `remote.root` refs under `.dml/refs/remote/{heads,tags}` and dependency refs under `.dml/refs/dep/<name>/{heads,tags}`. Tracking files SHALL contain commit pointers and SHALL be local GC roots.

#### Scenario: Track fetched project branch
- **WHEN** branch `main` is fetched from `remote.root`
- **THEN** `.dml/refs/remote/heads/main` points to the fetched commit

#### Scenario: Track fetched dependency tag
- **WHEN** tag `v1` is fetched from dependency `models`
- **THEN** `.dml/refs/dep/models/tags/v1` points to the fetched commit

## MODIFIED Requirements

### Requirement: Global DML config
The system SHALL load global DML config from `$DML_CONFIG_HOME/config.json`, `$XDG_CONFIG_HOME/dml/config.json`, or `~/.config/dml/config.json` in that precedence order.

#### Scenario: DML config home wins
- **WHEN** `DML_CONFIG_HOME` is set
- **THEN** the system reads `$DML_CONFIG_HOME/config.json`

#### Scenario: XDG config home fallback
- **WHEN** only `XDG_CONFIG_HOME` is set
- **THEN** the system reads `$XDG_CONFIG_HOME/dml/config.json`

#### Scenario: Default config fallback
- **WHEN** neither config-home environment variable is set
- **THEN** the system reads `~/.config/dml/config.json`

### Requirement: Global user defaults
The system SHALL use global config for user attribution, the default branch, and bootstrap hook configuration without deriving project owner identity.

#### Scenario: Default user attribution
- **WHEN** global config defines user `alice`
- **THEN** commits and runtime actions use `alice` unless overridden

#### Scenario: Default branch
- **WHEN** global config defines default branch `main`
- **THEN** init and selector-less fetch use `main`

### Requirement: Branch heads are mutable and project tags are immutable
The system SHALL allow branch head refs to move through safe update operations. It SHALL reject a non-forced attempt to overwrite an existing tag and allow forced replacement.

#### Scenario: Branch head update
- **WHEN** a push safely advances branch `main`
- **THEN** `refs/heads/main.json` may be replaced by the new branch head payload

#### Scenario: Tag overwrite rejected
- **WHEN** `refs/tags/v1.0.json` exists and force is false
- **THEN** publishing tag `v1.0` fails without changing the existing ref

#### Scenario: Forced tag overwrite succeeds
- **WHEN** `refs/tags/v1.0.json` exists and force is true
- **THEN** publishing tag `v1.0` replaces the existing ref

### Requirement: Project refs use typed object ref payloads
The system SHALL encode direct branch and tag refs as typed remote ref payloads containing `ref.to`, `created`, and `metadata`. They SHALL point to `commit` objects and fail before writing if the target object is missing or is not a commit root.

#### Scenario: Direct branch ref payload
- **WHEN** branch `main` is written
- **THEN** `refs/heads/main.json` contains `ref.to = "commit:<oid>"`, integer `created`, and object `metadata`

#### Scenario: Direct tag ref payload
- **WHEN** tag `v1.0` is written
- **THEN** `refs/tags/v1.0.json` contains the same typed payload shape

#### Scenario: Ref root validation fails closed
- **WHEN** a branch or tag would point to a missing object or non-commit root
- **THEN** the write fails without creating or updating the ref

### Requirement: Shared remote CAS
The system SHALL store immutable objects reachable from direct project heads, tags, caches, and execution state under `cas/sha256/<aa>/<bb>/<oid>` within `remote.root`, deduplicated by object ID.

#### Scenario: Multiple refs share an object
- **WHEN** multiple published refs contain the same reachable object
- **THEN** `remote.root` stores that object at one CAS path

### Requirement: Local remote config
The system SHALL store project-local configuration under `.dml/config.json`, with `remote.root` as the only project remote endpoint. It SHALL NOT store project URI identity, named ordinary remotes, or checkout branch selection in configuration.

#### Scenario: Remote root is stored
- **WHEN** project-local config records `s3://bucket/demo`
- **THEN** `.dml/config.json` contains `remote.root = "s3://bucket/demo"` and no project URI or named remote mapping

#### Scenario: Checkout remains filesystem state
- **WHEN** local configuration is written
- **THEN** active branch or detached commit selection remains in `.dml/HEAD`

### Requirement: Config waterfall precedence
The system SHALL resolve configurable values using explicit CLI/API arguments first, environment variables second, and config-file values last. Checkout and revision-source selection SHALL not be configuration waterfall values.

#### Scenario: Explicit remote root wins
- **WHEN** a supported operation receives an explicit `remote.root` override and environment or config also provides one
- **THEN** the operation uses the explicit root

#### Scenario: Revision source does not change checkout state
- **WHEN** a command selects remote or dependency tracking refs
- **THEN** it does not derive or update `.dml/HEAD` unless that workflow explicitly performs checkout

### Requirement: Supported DML environment variables
The system SHALL support current configuration environment variables including `DML_REMOTE_ROOT`, `DML_USER`, `DML_PROJECT_HOME`, and `DML_CONFIG_HOME`. It SHALL NOT support `DML_REMOTE_PROJECT`, `DML_PROJECT_NAME`, `DML_PROJECT_OWNER`, project URI selectors, `DML_REMOTE_NAME`, or `DML_BRANCH` as configuration or checkout state.

#### Scenario: Remote root env overrides config
- **WHEN** `DML_REMOTE_ROOT` is set for a supported remote-backed operation
- **THEN** it overrides configured `remote.root`

#### Scenario: Removed project env is ignored
- **WHEN** `DML_REMOTE_PROJECT` is set
- **THEN** it does not create project identity or select a remote ref

#### Scenario: Branch env is unsupported
- **WHEN** `DML_BRANCH` is set
- **THEN** it is not used for checkout or fetch selection

#### Scenario: Hook context omits project and remote identity
- **WHEN** a bootstrap hook runs
- **THEN** DML provides operation and local-path context without `DML_PROJECT_NAME`, `DML_PROJECT_OWNER`, or `DML_REMOTE_NAME`

### Requirement: Project commands use project-local state and current env names only
The system SHALL resolve local state from the project directory and remote-backed project behavior from resolved `remote.root`. Named dependency endpoint config SHALL be read only when a workflow explicitly selects `dep=<name>`.

#### Scenario: Project config comes from project directory
- **WHEN** a project command resolves local configuration
- **THEN** it reads `<project-directory>/.dml/config.json`

#### Scenario: Project remote operation uses remote root
- **WHEN** project synchronization or execution resolves its endpoint
- **THEN** it uses normalized `remote.root`, not named remote or project identity fields

### Requirement: Project directory initialization
The system SHALL initialize local project state under `<project-directory>/.dml/` with JSON configuration, HEAD state, database storage, and the existing ignore-file behavior.

#### Scenario: Init creates project directory state
- **WHEN** `dml init demo` succeeds
- **THEN** it creates `demo/.dml/config.json`, `demo/.dml/HEAD`, and `demo/.dml/db/`

#### Scenario: Init here creates local state
- **WHEN** `dml init --here demo` succeeds
- **THEN** it creates `.dml/config.json`, `.dml/HEAD`, and `.dml/db/` in the current directory

#### Scenario: Init creates unborn attached HEAD
- **WHEN** init succeeds without a commit
- **THEN** HEAD attaches to the default branch without materializing its branch ref or a synthetic commit

#### Scenario: Init refuses invalid destinations
- **WHEN** child-directory init targets an existing directory or detached init has no commit
- **THEN** initialization fails without creating incoherent state

### Requirement: Init shell hooks
The system SHALL support ordered `post-init` hooks after `.dml/` exists and allow explicit hook suppression. Hook context SHALL identify the operation and local/config paths without project URI or named-remote identity.

#### Scenario: Init hooks run in order
- **WHEN** enabled post-init hooks are configured
- **THEN** they run in configured order within the initialized project directory

#### Scenario: Init no-hooks skips hooks
- **WHEN** init disables hooks
- **THEN** no post-init hook runs

#### Scenario: Hook environment omits removed identity
- **WHEN** a post-init hook runs
- **THEN** its environment omits project owner/name, remote name, and branch-selector variables

### Requirement: Fetch updates remote-tracking heads
The system SHALL implement `fetch [--dep DEP] [BRANCH|@TAG]`. It SHALL fetch exactly the selected branch or tag closure from `remote.root` or the named dependency, default to branch `default.branch_name`, and update only the selected tracking namespace after the complete closure has been validated and materialized. Every fetch failure SHALL preserve the existing tracking ref.

#### Scenario: Fetch default remote branch
- **WHEN** `dml fetch` succeeds
- **THEN** `.dml/refs/remote/heads/<default.branch_name>` is updated

#### Scenario: Fetch selected remote tag
- **WHEN** `dml fetch @v1` succeeds
- **THEN** `.dml/refs/remote/tags/v1` is updated

#### Scenario: Fetch selected dependency branch
- **WHEN** `dml fetch --dep models feature` succeeds
- **THEN** `.dml/refs/dep/models/heads/feature` is updated without updating remote tracking refs or other dependencies

#### Scenario: Unknown dependency fails
- **WHEN** `dml fetch --dep unknown main` is requested
- **THEN** the command fails without changing local tracking refs

#### Scenario: Failed closure materialization preserves tracking
- **WHEN** fetch finds the selected ref but any required object is missing, invalid, or cannot be downloaded
- **THEN** fetch fails and preserves the prior tracking ref for that branch or tag

#### Scenario: Tracking update occurs last
- **WHEN** fetch successfully validates and materializes the complete selected closure
- **THEN** it atomically replaces the selected tracking ref after all fallible closure work completes

### Requirement: Pull fetches and merges the configured upstream
The system SHALL implement pull as fetching the current attached branch's configured upstream branch from `remote.root`, then merging that remote tracking ref. Pull SHALL accept no endpoint, dependency, or revision argument.

#### Scenario: Pull configured upstream
- **WHEN** local branch `feature` tracks remote branch `main` and pull succeeds
- **THEN** remote tracking branch `main` is refreshed and `feature` advances to the merge result

#### Scenario: Pull untracked branch fails
- **WHEN** the current attached branch has no configured upstream
- **THEN** pull fails without fetching or advancing the branch

### Requirement: Push uses conditional publication and fast-forward safety
The system SHALL publish to resolved `remote.root`. Non-forced branch pushes SHALL materialize and validate the current remote tip, require fast-forward ancestry, and conditionally update the observed ref; forced pushes SHALL overwrite directly.

#### Scenario: Missing branch is created safely
- **WHEN** a non-forced push targets an absent branch
- **THEN** it creates `refs/heads/<branch>.json` only if it remains absent

#### Scenario: Fast-forward push
- **WHEN** the remote branch tip is an ancestor and its observed ETag still matches
- **THEN** push updates the branch head

#### Scenario: Non-fast-forward or raced push fails
- **WHEN** ancestry validation fails or the observed remote ref changes
- **THEN** push fails without overwriting the remote branch

#### Scenario: Forced push overwrites a ref
- **WHEN** force is requested for a branch or tag
- **THEN** push overwrites the direct ref without ancestry or conditional-write checks

## REMOVED Requirements

### Requirement: Remote project refs namespace
**Reason**: One endpoint root identifies one project, so owner/project paths are unnecessary.
**Migration**: Republish each prior project to its own root using direct `refs/heads/*` and `refs/tags/*`.

### Requirement: DML URIs track fetched remote refs
**Reason**: Project URIs and URI-keyed tracking refs are removed.
**Migration**: Fetch from `remote.root` or a named dependency into its fixed local tracking namespace.

### Requirement: Remote operations parse DML URIs
**Reason**: Remote operations receive normalized endpoint configuration and plain branch or `@tag` selectors.
**Migration**: Replace project URI selectors with `remote.root`, `--dep`, and namespace-independent revisions.

### Requirement: Project creation owner default
**Reason**: Remote transport no longer has project owner identity.
**Migration**: Configure endpoint access through the root and backend credentials.

### Requirement: Project sync requires a configured named remote
**Reason**: Project synchronization always uses resolved `remote.root`.
**Migration**: Remove named ordinary remotes and configure `remote.root` directly.
