## ADDED Requirements

### Requirement: Remote project refs namespace
The system SHALL store project branch and tag refs under `refs/projects/<owner>/<project>/{heads,tags}/` within the remote protocol root.

#### Scenario: Branch head path
- **WHEN** project `alice/demo` branch `main` is addressed on the remote
- **THEN** the branch head ref path is `refs/projects/alice/demo/heads/main.json`

#### Scenario: Tag path
- **WHEN** project `alice/demo` tag `v1.0` is addressed on the remote
- **THEN** the tag ref path is `refs/projects/alice/demo/tags/v1.0.json`

### Requirement: Branch heads are mutable and project tags are immutable
The system SHALL allow project branch head refs to move through safe update operations. The system SHALL reject a non-forced attempt to overwrite an existing project tag ref and SHALL allow a forced attempt to replace it.

#### Scenario: Branch head update
- **WHEN** a push safely advances project `alice/demo` branch `main`
- **THEN** the existing `refs/projects/alice/demo/heads/main.json` ref may be replaced by the new branch head payload

#### Scenario: Tag overwrite rejected
- **WHEN** `refs/projects/alice/demo/tags/v1.0.json` already exists
- **THEN** publishing tag `v1.0` fails without changing the existing tag ref

#### Scenario: Forced tag overwrite succeeds
- **WHEN** `refs/projects/alice/demo/tags/v1.0.json` already exists and push requests force
- **THEN** publishing tag `v1.0` replaces the existing tag ref with the requested commit

### Requirement: Project refs use typed object ref payloads
The system SHALL encode project branch and tag refs as typed remote ref payloads containing `ref.to`, `created`, and `metadata`.

Project branch and tag refs SHALL point to `commit` objects and SHALL fail before writing the ref if the target object is missing or is not a `commit` root.

Project ref `metadata` remains unconstrained in this change.

#### Scenario: Project branch ref payload
- **WHEN** project `alice/demo` branch `main` is written
- **THEN** `refs/projects/alice/demo/heads/main.json` contains `ref.to = "commit:<oid>"`, integer `created`, and object `metadata`

#### Scenario: Project tag ref payload
- **WHEN** project `alice/demo` tag `v1.0` is written
- **THEN** `refs/projects/alice/demo/tags/v1.0.json` contains `ref.to = "commit:<oid>"`, integer `created`, and object `metadata`

#### Scenario: Project ref root validation fails closed
- **WHEN** a project branch or tag ref would point to a missing object or a non-`commit` root
- **THEN** the write fails without creating or updating the project ref

### Requirement: Shared remote CAS
The system SHALL store immutable CAS objects in a shared remote CAS under `cas/sha256/<aa>/<bb>/<oid>` independent of owner, project, or branch.

#### Scenario: Two projects reference same object
- **WHEN** two project refs point to commit graphs that include the same CAS object
- **THEN** the remote stores that CAS object at one shared CAS path

### Requirement: Global DML config
The system SHALL load global DML config from `$DML_CONFIG_HOME/config.toml`, `$XDG_CONFIG_HOME/dml/config.toml`, or `~/.config/dml/config.toml` in that precedence order.

#### Scenario: DML config home wins
- **WHEN** `DML_CONFIG_HOME` is set
- **THEN** the system reads global config from `$DML_CONFIG_HOME/config.toml`

#### Scenario: XDG config home fallback
- **WHEN** `DML_CONFIG_HOME` is unset and `XDG_CONFIG_HOME` is set
- **THEN** the system reads global config from `$XDG_CONFIG_HOME/dml/config.toml`

#### Scenario: Default config fallback
- **WHEN** neither `DML_CONFIG_HOME` nor `XDG_CONFIG_HOME` is set
- **THEN** the system reads global config from `~/.config/dml/config.toml`

### Requirement: Global user defaults
The system SHALL use global config for user defaults and bootstrap hook configuration.

#### Scenario: Default project owner
- **WHEN** global config contains `[user].name = "alice"` and `dml init demo` omits an owner
- **THEN** the project owner is `alice`

#### Scenario: Default branch
- **WHEN** global config contains `[defaults].branch = "main"` and `dml init demo` omits a branch
- **THEN** the initial branch is `main`

### Requirement: Local remote config
The system SHALL store project-local config under `.dml/config.toml` containing branchless project identity and remote storage settings. The current checkout branch MUST NOT be stored in local config.

#### Scenario: Resolve origin main
- **WHEN** local config defines project identity `dml://alice/demo` and the attached local branch is `main`
- **THEN** `dml push` resolves the default remote target as project owner `alice`, project `demo`, and branch `main`

#### Scenario: Project fields are stored
- **WHEN** local project config is written for project `alice/demo`
- **THEN** `.dml/config.toml` contains `[project].uri = "dml://alice/demo"` and does not contain branch-selection fields

#### Scenario: Remote fields are stored
- **WHEN** local project config records the remote storage URI for project `alice/demo`
- **THEN** `.dml/config.toml` contains the configured `[remote]` fields and no local checkout branch field

#### Scenario: Reject branch-qualified local project URI
- **WHEN** local config would store `dml://alice/demo#main` or `dml://alice/demo@v1`
- **THEN** config validation fails without writing the selector-bearing URI

### Requirement: Config waterfall precedence
The system SHALL resolve configurable values using explicit CLI/API arguments first, environment variables second, and config file values last. Checkout-state selection is not part of this waterfall and SHALL be resolved from `.dml/HEAD`.

#### Scenario: Explicit value wins over environment
- **WHEN** a command receives an explicit mutable branch argument and environment variables also provide configuration inputs
- **THEN** the command uses the explicit branch argument for that mutable branch target

#### Scenario: Environment does not override checkout state
- **WHEN** a command omits an explicit branch argument and environment variables are resolved
- **THEN** the command still derives the current checkout from `.dml/HEAD` rather than from configuration environment variables

#### Scenario: Config used as fallback for non-checkout values
- **WHEN** a command omits explicit overrides and no matching environment value is set
- **THEN** the command uses configured values such as `remote.project`, `remote.root`, or `default_branch` but not a config-derived current branch

#### Scenario: Remote storage env vars override config
- **WHEN** `DML_REMOTE_BUCKET` or `DML_REMOTE_PREFIX` is set for a remote operation
- **THEN** the command uses the environment value instead of the configured remote storage field

### Requirement: Supported DML environment variables
The system SHALL support only the DML environment variables defined for the project model and SHALL treat hook context variables as output-only process context. `DML_BRANCH` is not a supported environment variable.

#### Scenario: Global config home override
- **WHEN** `DML_CONFIG_HOME` is set
- **THEN** the global DML config directory resolves from `DML_CONFIG_HOME`

#### Scenario: Existing user env remains supported
- **WHEN** `DML_USER` is set and an owner is omitted
- **THEN** the system uses `DML_USER` as the default project owner

#### Scenario: DML_BRANCH is rejected as unsupported
- **WHEN** `DML_BRANCH` is set during project or runtime command resolution
- **THEN** the system does not use it as checkout state or branch selection input

#### Scenario: Project env overrides config
- **WHEN** `DML_PROJECT_NAME`, `DML_PROJECT_OWNER`, or `DML_REMOTE_PROJECT` is set
- **THEN** the corresponding project config value is overridden for that command

#### Scenario: Remote env overrides config
- **WHEN** `DML_REMOTE_ROOT`, `DML_REMOTE_BUCKET`, or `DML_REMOTE_PREFIX` is set
- **THEN** the corresponding remote selection or storage value is overridden for that command

#### Scenario: Hook context env is provided by DML
- **WHEN** a hook command runs
- **THEN** DML sets `DML_HOOK`, `DML_PROJECT_HOME`, and, for clone hooks, `DML_REMOTE_NAME`

### Requirement: Project commands use project-local state and current env names only
The system SHALL resolve project-local state from the project directory and SHALL use only the current supported environment variable surface for git-like project operations.

#### Scenario: Project config comes from the project directory
- **WHEN** a project command resolves project-local config
- **THEN** it reads from `<project-directory>/.dml/config.toml`

#### Scenario: DML_REPO is not used for project database
- **WHEN** a project command opens the local object database
- **THEN** it uses `<project-directory>/.dml/db/` and does not use `DML_REPO`

#### Scenario: DML_REMOTE_ROOT is not used for named remotes
- **WHEN** a remote project command resolves remote storage
- **THEN** it uses named remote bucket/prefix config or `DML_REMOTE_BUCKET` and `DML_REMOTE_PREFIX`, not `DML_REMOTE_ROOT`

#### Scenario: Removed execution/cache env vars are ignored
- **WHEN** `DML_DYNAMODB_TABLE` or `DML_REMOTE_CACHE` is set during a git-like project operation
- **THEN** the operation does not use those values

### Requirement: Project directory initialization
The system SHALL initialize local project state under `<project-directory>/.dml/` for `init`.

#### Scenario: Init creates DML directory
- **WHEN** `dml init demo` succeeds
- **THEN** the system creates `demo/.dml/`, `demo/.dml/config.toml`, `.dml/HEAD`, and local database storage under `demo/.dml/db/`

#### Scenario: Init refuses existing child directory
- **WHEN** `dml init demo` runs and `demo/` already exists
- **THEN** init fails and instructs the user to initialize that directory with `dml init --here demo`

#### Scenario: Init here creates DML directory in current directory
- **WHEN** `dml init --here demo` succeeds from the current directory
- **THEN** the system creates `.dml/`, `.dml/config.toml`, `.dml/HEAD`, and local database storage under `.dml/db/`

#### Scenario: Init here uses provided project name
- **WHEN** `dml init --here demo` succeeds from directory `workdir`
- **THEN** the local project name is `demo`

#### Scenario: Init creates DML gitignore
- **WHEN** `dml init demo` succeeds
- **THEN** the system writes `demo/.dml/.gitignore` containing `*`

#### Scenario: Init creates unborn attached HEAD
- **WHEN** `dml init demo` succeeds
- **THEN** the system creates `demo/.dml/`, `demo/.dml/config.toml`, `.dml/HEAD`, and local database storage under `demo/.dml/db/`
- **AND** `.dml/HEAD` is attached to the default branch
- **AND** the corresponding local branch ref file does not exist yet

#### Scenario: Init does not create initial empty commit
- **WHEN** `dml init demo` succeeds
- **THEN** local storage does not contain a synthetic initial empty commit solely to materialize the branch tip

#### Scenario: Detached init without commit is rejected
- **WHEN** init is requested in detached mode before any commit exists
- **THEN** init fails because detached HEAD requires a concrete commit

### Requirement: Init shell hooks
The system SHALL support `post-init` shell hooks from global DML config that run in the project directory after `.dml/` exists.

#### Scenario: Init hook succeeds
- **WHEN** a `post-init` hook command is configured and `dml init demo` runs
- **THEN** the hook command runs in the `demo` project directory after `demo/.dml/` exists

#### Scenario: Init here hook succeeds
- **WHEN** a `post-init` hook command is configured and `dml init --here demo` runs
- **THEN** the hook command runs in the current directory after `.dml/` exists

#### Scenario: Hooks run in configured order
- **WHEN** multiple `post-init` hook commands are configured and `dml init demo` runs
- **THEN** the hook commands run in their configured list order

#### Scenario: Init no-hooks skips hooks
- **WHEN** `dml init --no-hooks demo` runs
- **THEN** no `post-init` hook commands run

#### Scenario: Hook environment omits removed branch env
- **WHEN** a `post-init` hook command runs
- **THEN** the process environment includes `DML_HOOK`, `DML_PROJECT_HOME`, `DML_PROJECT_NAME`, `DML_PROJECT_OWNER`, and `DML_CONFIG_HOME`, and does not include `DML_BRANCH`

### Requirement: DML URIs track fetched remote refs
The system SHALL track fetched remote branches and tags locally by configured remote name and branch or tag name. A remote-tracking selector SHALL use `<remote-name>/<branch-name>` for branches and `<remote-name>@<tag-name>` for tags.

#### Scenario: Store fetched branch tracking ref
- **WHEN** `dml fetch origin` fetches remote branch `main`
- **THEN** local storage tracks it as `origin/main` pointing to the resolved commit

#### Scenario: Store fetched tag tracking ref
- **WHEN** `dml fetch origin` fetches remote tag `v1.0`
- **THEN** local storage tracks it as `origin@v1.0` pointing to the resolved commit

#### Scenario: Tracking ref stores commit pointer
- **WHEN** a fetched remote ref is persisted locally
- **THEN** the persisted tracking ref contains the resolved commit pointer

#### Scenario: Remote tracking selector resolves locally
- **WHEN** a user-facing command receives `origin/main`
- **THEN** the command resolves it locally through the tracking ref for `origin/main`

### Requirement: Remote operations parse DML URIs
The system SHALL parse and canonicalize DML revision URIs through one centralized shared revision URI parser/stringifier boundary before deriving remote project ref paths.

#### Scenario: Push parses branch URI through shared parser
- **WHEN** push targets canonical URI `dml://alice/demo#main`
- **THEN** remote operations derive `refs/projects/alice/demo/heads/main.json` from the shared parsed revision object

#### Scenario: Fetch parses tag URI through shared parser
- **WHEN** fetch targets canonical URI `dml://alice/demo@v1.0`
- **THEN** remote operations derive `refs/projects/alice/demo/tags/v1.0.json` from the shared parsed revision object

#### Scenario: Branch/tag capability checks remain operation-specific
- **WHEN** a mutation operation targets the wrong selector type (branch op with tag URI, or tag op with branch URI)
- **THEN** the operation fails at method boundary capability checks even though URI parsing/canonicalization succeeds

### Requirement: Project creation owner default
The system SHALL default project owner to the configured current user when project creation omits an owner.

#### Scenario: Create project without owner
- **WHEN** the configured user is `alice` and project `demo` is created without an explicit owner
- **THEN** the project URI is `dml://alice/demo`

### Requirement: Fetch updates remote-tracking heads
The system SHALL fetch all branch and tag refs for a configured named remote, materialize each referenced commit closure locally, and update the corresponding local remote-tracking refs. `fetch` SHALL accept at most one optional remote name and SHALL default to `origin`. A branch- or tag-qualified DML project URI SHALL instead fetch only that addressed ref and update its URI-keyed tracking ref.

#### Scenario: Fetch default origin
- **WHEN** `dml fetch` succeeds and `origin` has branches `main` and `feature` plus tag `v1`
- **THEN** local tracking refs for `origin/main`, `origin/feature`, and `origin@v1` are updated

#### Scenario: Fetch selected remote
- **WHEN** `dml fetch research` succeeds
- **THEN** it updates tracking refs for every branch and tag in remote `research` without updating other remotes

#### Scenario: Unknown remote fails
- **WHEN** `dml fetch unknown` is requested
- **THEN** the command fails without changing local tracking refs

#### Scenario: Fetch explicit project ref
- **WHEN** `dml fetch dml://alice/research#main` succeeds
- **THEN** local storage updates the URI-keyed tracking ref for `dml://alice/research#main` without requiring a configured named remote

### Requirement: Pull fetches and merges the configured upstream
The system SHALL implement branch pull as fetching the current attached branch's configured upstream remote followed by merge of that upstream tracking ref into the current branch. Pull SHALL accept no positional remote or branch argument.

#### Scenario: Pull configured upstream
- **WHEN** current local branch `feature` tracks `origin/main` and `dml pull` succeeds
- **THEN** `origin/main` is refreshed and `feature` advances to the merge result or fetched commit when fast-forwardable

#### Scenario: Pull untracked branch fails
- **WHEN** the current attached branch has no configured upstream
- **THEN** `dml pull` fails without fetching or advancing the branch

#### Scenario: Pull remote argument is rejected
- **WHEN** a user supplies a positional argument to `dml pull`
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

### Requirement: Project sync requires a configured named remote
The system SHALL require a configured named remote before default project-addressed synchronization. `origin` SHALL be the default named remote for `fetch` and for first publication of an untracked branch.

#### Scenario: Default sync without origin
- **WHEN** a repository has remote storage but no remote named `origin` and default fetch or first branch publication is requested
- **THEN** the operation fails with a descriptive error stating that `origin` is required

#### Scenario: Named upstream does not require origin
- **WHEN** the current branch tracks `research/main` and remote `research` is configured
- **THEN** pull and push use `research` without requiring `origin`
