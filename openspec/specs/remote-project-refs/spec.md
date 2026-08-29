## Purpose

Define remote project reference storage, configuration, synchronization, and enumeration behavior.

## Requirements

### Requirement: One-project remote refs namespace
The system SHALL store branch and tag refs directly under `refs/{heads,tags}/` within resolved `remote.root`. A remote root SHALL represent exactly one DML project and execution domain and SHALL NOT require owner or project selectors.

#### Scenario: Branch head path
- **WHEN** branch `main` is addressed at `remote.root`
- **THEN** its ref path is `refs/heads/main.json`

#### Scenario: Tag path
- **WHEN** tag `v1.0` is addressed at `remote.root`
- **THEN** its ref path is `refs/tags/v1.0.json`

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

### Requirement: Tracking refs SHALL become visible after shallow state is valid
Depth-limited fetch SHALL update a local tracking ref only after every included commit snapshot is materialized and every omitted unavailable parent is recorded in valid shallow-history metadata. A failed fetch MAY leave unreferenced immutable objects but SHALL preserve the prior tracking ref and valid shallow metadata.

#### Scenario: Depth fetch fails before completion
- **WHEN** an object required by an included snapshot cannot be fetched or validated
- **THEN** the selected tracking ref retains its prior value
- **AND** no invalid shallow boundary is exposed through that ref

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
- **THEN** the write fails without creating or updating the project ref

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

### Requirement: Pull fetches and merges the configured upstream
The system SHALL implement branch pull as fetching the current attached branch's configured upstream branch from `remote.root` followed by merge of that upstream tracking ref into the current branch. Pull SHALL accept an optional positive history depth and no positional remote or branch argument. Pull without depth SHALL fetch new remote commits until it reaches locally available history while preserving any older shallow boundary. Pull SHALL fail without advancing the branch when the fetched history is insufficient to prove the required merge relationship.

#### Scenario: Pull configured upstream
- **WHEN** current local branch `feature` tracks remote-root branch `main` and `dml pull` succeeds
- **THEN** the remote tracking ref for `main` is refreshed and `feature` advances to the merge result or fetched commit when fast-forwardable

#### Scenario: Pull shallow branch incrementally
- **WHEN** a shallow local branch tip is an ancestor of a newer remote upstream tip through remotely available commits
- **THEN** pull materializes the connecting commits, preserves the older shallow boundary, and fast-forwards the local branch

#### Scenario: Pull depth cannot prove ancestry
- **WHEN** pull with a requested depth stops before reaching history needed to prove a merge relationship
- **THEN** pull fails with deepening guidance without advancing the local branch

#### Scenario: Pull untracked branch fails
- **WHEN** the current attached branch has no configured upstream
- **THEN** `dml pull` fails without fetching or advancing the branch

#### Scenario: Pull remote argument is rejected
- **WHEN** a user supplies a positional remote or branch argument to `dml pull`
- **THEN** command parsing rejects the invocation

### Requirement: Push uses conditional publication and fast-forward safety
The system SHALL expose a keyword-only `force` option on `Dml.push()` that defaults to `False`. For a non-forced branch push, the system SHALL read and materialize an existing remote branch tip without modifying local heads or working state, require that tip to be an ancestor of the candidate commit, and conditionally replace the branch ref using the observed ETag. If the remote branch is absent, the system SHALL create it only if it remains absent. A forced branch or tag push SHALL overwrite the ref without reading, ancestry validation, or conditional-write checks.

#### Scenario: Missing branch is created safely
- **WHEN** a non-forced push targets a remote branch ref that does not exist
- **THEN** push creates the branch ref only if it still does not exist

#### Scenario: Missing branch creation loses race
- **WHEN** a non-forced push observes that a remote branch ref is absent and another client creates it before publication
- **THEN** push fails without overwriting the remote branch ref

#### Scenario: Remote branch tip is materialized for validation
- **WHEN** a non-forced push targets an existing remote branch ref
- **THEN** the system materializes the remote commit closure locally for ancestry validation without updating local tracking refs, branch refs, or HEAD

#### Scenario: Fast-forward push
- **WHEN** the remote branch head is an ancestor of the local branch head and the observed ETag still matches
- **THEN** push updates the remote branch head to the local commit

#### Scenario: Non-fast-forward push rejected
- **WHEN** the remote branch head is not an ancestor of the local branch head and force is not requested
- **THEN** push fails without updating the remote branch head

#### Scenario: Conditional update loses race
- **WHEN** a non-forced push validates a remote branch head and another client updates that branch before publication
- **THEN** push fails without overwriting the newer remote branch head

#### Scenario: Force push overwrites a ref
- **WHEN** force is requested for a branch or tag push
- **THEN** push overwrites the remote ref with the local commit without remote-tip validation or conditional-write checks

### Requirement: Branch and tag enumeration SHALL select local, fetched, or endpoint refs
The system SHALL enumerate branches and tags from exactly one source selected independently by `remote` and `dep`. With neither selector it SHALL use local refs; with only `remote` it SHALL use refs at configured `remote.root`; with only `dep` it SHALL use locally fetched refs for that dependency; and with both selectors it SHALL use refs at that dependency's configured endpoint. An unknown dependency or a required but unconfigured endpoint SHALL fail with a descriptive configuration error.

#### Scenario: Local source is selected by default
- **WHEN** branch or tag enumeration omits `remote` and `dep`
- **THEN** only refs in the local branch or tag namespace are returned

#### Scenario: Main endpoint is selected by remote
- **WHEN** branch or tag enumeration sets `remote = True` and omits `dep`
- **THEN** only refs in configured `remote.root` are returned

#### Scenario: Fetched dependency source is selected by dependency
- **WHEN** branch or tag enumeration sets `dep = "models"` and leaves `remote = False`
- **THEN** only locally fetched refs for dependency `models` are returned

#### Scenario: Dependency endpoint is selected by both selectors
- **WHEN** branch or tag enumeration sets `remote = True` and `dep = "models"`
- **THEN** only refs at dependency `models`' configured endpoint are returned

### Requirement: Branch and tag enumeration SHALL preserve exact commit tips
Each enumerated branch or tag record SHALL preserve its ref name and the exact commit ref stored at the selected source. The result sequence SHALL be ordered lexicographically by ref name. Endpoint enumeration SHALL NOT require the selected commit or any reachable object to exist in the local object database. If any selected local, fetched, or endpoint ref is malformed or does not identify a commit, enumeration SHALL fail rather than omit or coerce that ref. The caller-facing item shape is owned by `unified-dml-surface`.

#### Scenario: Fetched ref returns tracked tip
- **WHEN** a locally fetched dependency branch `main` tracks `commit:a1`
- **THEN** the enumerated record for `main` carries exact tip `commit:a1`

#### Scenario: Unmaterialized remote tip remains visible
- **WHEN** an endpoint branch `main` points to `commit:b2` and that commit is absent locally
- **THEN** the endpoint record for `main` carries exact tip `commit:b2`

#### Scenario: Results are ordered by name
- **WHEN** the selected source contains refs `zeta`, `main`, and `alpha`
- **THEN** enumeration returns list items in `alpha`, `main`, `zeta` name order

#### Scenario: Malformed local or fetched pointer fails the listing
- **WHEN** a selected local or fetched tracking file does not contain a valid commit pointer
- **THEN** enumeration fails without returning a partial result

#### Scenario: Malformed endpoint ref fails the listing
- **WHEN** a selected endpoint ref contains an invalid typed ref payload
- **THEN** enumeration fails without returning a partial result

#### Scenario: Non-commit endpoint ref fails the listing
- **WHEN** a selected endpoint branch or tag points to a namespace other than `commit`
- **THEN** enumeration fails without materializing that object or returning a partial result

### Requirement: Endpoint ref enumeration SHALL be bounded and read-only
Endpoint branch and tag enumeration SHALL inspect the selected endpoint's descriptor and requested ref namespace. When the descriptor is absent, it SHALL perform one endpoint-state existence listing limited to at most one key anywhere under the resolved endpoint root solely to distinguish a truly empty endpoint from incompatible non-empty state; the probe SHALL NOT enumerate or decode object payloads. Enumeration SHALL NOT fetch or materialize CAS objects, mutate local tracking refs, create an endpoint descriptor, or otherwise write local or remote state.

#### Scenario: Listing does not materialize remote commits
- **WHEN** endpoint enumeration observes a ref whose commit is absent locally
- **THEN** the commit remains absent from the local object database after enumeration

#### Scenario: Listing does not update tracking refs
- **WHEN** an endpoint ref differs from its local tracking ref
- **THEN** endpoint enumeration returns the endpoint tip without changing the local tracking ref

#### Scenario: Listing an empty uninitialized endpoint is non-mutating
- **WHEN** endpoint enumeration targets an empty endpoint with no descriptor
- **THEN** it returns no refs and does not create a descriptor

#### Scenario: Descriptorless emptiness check is bounded
- **WHEN** endpoint enumeration finds no descriptor
- **THEN** it performs one existence listing limited to at most one key anywhere under the resolved endpoint root
- **AND** it does not enumerate object payloads or traverse CAS

#### Scenario: Listing rejects incompatible endpoint state without mutation
- **WHEN** endpoint enumeration targets a non-empty endpoint with a missing, legacy, or unsupported descriptor
- **THEN** it fails without reading project refs or changing the endpoint
