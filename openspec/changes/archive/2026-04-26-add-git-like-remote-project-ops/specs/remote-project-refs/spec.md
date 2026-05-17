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
The system SHALL allow project branch head refs to move through safe update operations and SHALL reject attempts to overwrite existing project tag refs.

#### Scenario: Branch head update
- **WHEN** a push safely advances project `alice/demo` branch `main`
- **THEN** the existing `refs/projects/alice/demo/heads/main.json` ref may be replaced by the new branch head payload

#### Scenario: Tag overwrite rejected
- **WHEN** `refs/projects/alice/demo/tags/v1.0.json` already exists
- **THEN** publishing tag `v1.0` fails without changing the existing tag ref

### Requirement: Project refs use manifest ref payloads
The system SHALL encode project branch and tag refs using the existing remote ref payload schema for manifest refs.

Project branch and tag refs SHALL point to commit manifests, SHALL include direct DAG `targets`, and SHALL fail before writing the ref if the target manifest is missing, invalid, or has `closure["dag"]` inconsistent with the ref `targets["dag"]`.

#### Scenario: Project branch ref payload
- **WHEN** project `alice/demo` branch `main` is written
- **THEN** `refs/projects/alice/demo/heads/main.json` contains `kind`, `schema`, `target`, `created_at`, `targets`, and `meta` fields following the remote ref schema

#### Scenario: Project tag ref payload
- **WHEN** project `alice/demo` tag `v1.0` is written
- **THEN** `refs/projects/alice/demo/tags/v1.0.json` contains `kind`, `schema`, `target`, `created_at`, `targets`, and `meta` fields following the remote ref schema

#### Scenario: Project ref target validation fails closed
- **WHEN** a project branch or tag ref would point to a missing manifest, invalid manifest, non-commit manifest, or inconsistent direct DAG targets
- **THEN** the write fails without creating or updating the project ref

### Requirement: Shared remote CAS
The system SHALL store immutable CAS objects in a shared remote CAS under `cas/sha256/<aa>/<bb>/<oid>` independent of owner, project, or branch.

#### Scenario: Two projects reference same object
- **WHEN** two project refs target manifests that include the same CAS object
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
The system SHALL store project-local config under `.dml/config.toml` containing project identity, current branch, and named remotes that map remote names to project URIs and storage roots.

#### Scenario: Resolve origin main
- **WHEN** local config defines `origin` as `dml://alice/demo`
- **THEN** `dml push origin main` resolves to project owner `alice`, project `demo`, and branch `main`

#### Scenario: Project fields are stored
- **WHEN** local project config is written for project `alice/demo`
- **THEN** `.dml/config.toml` contains `[project].name`, `[project].owner`, `[project].uri`, and `[branch].current`

#### Scenario: Remote fields are stored
- **WHEN** local project config records remote `origin`
- **THEN** `.dml/config.toml` contains `[remotes.origin].uri`, `[remotes.origin].bucket`, and `[remotes.origin].prefix`

#### Scenario: Reject overlong remote URI
- **WHEN** local config would store a canonical remote project URI longer than 64 bytes
- **THEN** config validation fails

### Requirement: Config waterfall precedence
The system SHALL resolve configurable values using explicit CLI/API arguments first, environment variables second, and config file values last.

#### Scenario: Explicit value wins over environment
- **WHEN** a command receives an explicit branch argument and `DML_BRANCH` is also set
- **THEN** the command uses the explicit branch argument

#### Scenario: Environment wins over config
- **WHEN** a command omits a branch argument, `DML_BRANCH` is set, and config has `[branch].current`
- **THEN** the command uses `DML_BRANCH`

#### Scenario: Config used as fallback
- **WHEN** a command omits a branch argument and `DML_BRANCH` is unset
- **THEN** the command uses the configured branch value

#### Scenario: Remote storage env vars override config
- **WHEN** `DML_REMOTE_BUCKET` or `DML_REMOTE_PREFIX` is set for a remote operation
- **THEN** the command uses the environment value instead of the configured remote storage field

### Requirement: Supported DML environment variables
The system SHALL support only the DML environment variables defined for the project model and SHALL treat hook context variables as output-only process context.

#### Scenario: Global config home override
- **WHEN** `DML_CONFIG_HOME` is set
- **THEN** the global DML config directory resolves from `DML_CONFIG_HOME`

#### Scenario: Existing user env remains supported
- **WHEN** `DML_USER` is set and an owner is omitted
- **THEN** the system uses `DML_USER` as the default project owner

#### Scenario: Existing branch env remains supported
- **WHEN** `DML_BRANCH` is set and a command omits an explicit branch
- **THEN** the system uses `DML_BRANCH` as the selected branch

#### Scenario: Project env overrides config
- **WHEN** `DML_PROJECT_NAME`, `DML_PROJECT_OWNER`, or `DML_REMOTE_PROJECT` is set
- **THEN** the corresponding project config value is overridden for that command

#### Scenario: Remote env overrides config
- **WHEN** `DML_REMOTE`, `DML_REMOTE_URI`, `DML_REMOTE_BUCKET`, or `DML_REMOTE_PREFIX` is set
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
The system SHALL initialize local project state under `<project-directory>/.dml/` for both `init` and `clone`.

#### Scenario: Init creates DML directory
- **WHEN** `dml init demo` succeeds
- **THEN** the system creates `demo/.dml/`, `demo/.dml/config.toml`, and local database storage under `demo/.dml/db/`

#### Scenario: Init refuses existing child directory
- **WHEN** `dml init demo` runs and `demo/` already exists
- **THEN** init fails and instructs the user to initialize that directory with `dml init --here demo`

#### Scenario: Init here creates DML directory in current directory
- **WHEN** `dml init --here demo` succeeds from the current directory
- **THEN** the system creates `.dml/`, `.dml/config.toml`, and local database storage under `.dml/db/`

#### Scenario: Init here uses provided project name
- **WHEN** `dml init --here demo` succeeds from directory `workdir`
- **THEN** the local project name is `demo`

#### Scenario: Init creates DML gitignore
- **WHEN** `dml init demo` succeeds
- **THEN** the system writes `demo/.dml/.gitignore` containing `*`

#### Scenario: Init creates initial branch
- **WHEN** `dml init demo` succeeds
- **THEN** local storage contains an initial empty commit/tree and the current branch is `main`

#### Scenario: Clone creates DML directory
- **WHEN** `dml clone dml://alice/demo` succeeds
- **THEN** the system creates a project directory with `.dml/`, `.dml/.gitignore`, `.dml/config.toml`, and local database storage under `.dml/db/`

### Requirement: Init and clone shell hooks
The system SHALL support `post-init` and `post-clone` shell hooks from global DML config that run in the project directory after `.dml/` exists.

#### Scenario: Init hook succeeds
- **WHEN** a `post-init` hook command is configured and `dml init demo` runs
- **THEN** the hook command runs in the `demo` project directory after `demo/.dml/` exists

#### Scenario: Init here hook succeeds
- **WHEN** a `post-init` hook command is configured and `dml init --here demo` runs
- **THEN** the hook command runs in the current directory after `.dml/` exists

#### Scenario: Hooks run in configured order
- **WHEN** multiple `post-init` hook commands are configured and `dml init demo` runs
- **THEN** the hook commands run in their configured list order

#### Scenario: Clone does not run init hooks
- **WHEN** both `post-init` and `post-clone` hooks are configured and `dml clone dml://alice/demo` runs
- **THEN** only `post-clone` hook commands run

#### Scenario: Init no-hooks skips hooks
- **WHEN** `dml init --no-hooks demo` runs
- **THEN** no `post-init` hook commands run

#### Scenario: Clone no-hooks skips hooks
- **WHEN** `dml clone --no-hooks dml://alice/demo` runs
- **THEN** no `post-clone` hook commands run

#### Scenario: Hook environment is provided
- **WHEN** a `post-init` or `post-clone` hook command runs
- **THEN** the process environment includes `DML_HOOK`, `DML_PROJECT_HOME`, `DML_PROJECT_NAME`, `DML_PROJECT_OWNER`, `DML_CONFIG_HOME`, and `DML_BRANCH`

#### Scenario: Clone hook fails
- **WHEN** a `post-clone` hook command exits non-zero during `dml clone dml://alice/demo`
- **THEN** clone fails and reports the failing hook command

### Requirement: DML URIs track fetched remote refs
The system SHALL track fetched remote branches and tags locally by canonical normalized DML URI.

#### Scenario: Store fetched branch tracking ref
- **WHEN** `dml fetch dml://alice/tools#main` succeeds
- **THEN** local storage tracks `dml://alice/tools#main` as pointing to the resolved commit

#### Scenario: Store fetched tag tracking ref
- **WHEN** `dml fetch dml://alice/tools@v1.0` succeeds
- **THEN** local storage tracks `dml://alice/tools@v1.0` as pointing to the resolved commit

#### Scenario: Tracking ref stores commit pointer
- **WHEN** a fetched remote ref is persisted locally
- **THEN** the persisted tracking ref contains the resolved commit pointer

#### Scenario: Canonical URI head is stored
- **WHEN** a remote fetch resolves project `alice/tools` branch `main`
- **THEN** the local tracking ref is stored under canonical URI `dml://alice/tools#main`

#### Scenario: Derived expression is not stored as URI head
- **WHEN** a remote operation resolves a derived expression such as `HEAD~2`
- **THEN** the system stores only the canonical project branch or tag URI for any tracking head it writes

#### Scenario: URI tracking ref length is validated
- **WHEN** a command would create a tracking ref whose canonical DML URI exceeds 64 bytes
- **THEN** the command fails without writing the tracking ref

#### Scenario: Overlong URI is rejected directly
- **WHEN** a canonical DML URI exceeds 64 bytes
- **THEN** the system rejects it and does not hash or rewrite it into an alternate tracking key

#### Scenario: URI tracking ref characters are validated explicitly
- **WHEN** a command would create a DML URI tracking ref
- **THEN** the system validates the canonical URI as a DML project URI before writing the tracking ref

#### Scenario: User-facing DML URI resolves to local tracking ref
- **WHEN** a user-facing command receives `dml://alice/tools#main`
- **THEN** the command resolves it locally through the tracking ref for `dml://alice/tools#main`

### Requirement: Remote operations parse DML URIs
The system SHALL parse canonical DML URIs into structured remote project ref paths for remote operations.

#### Scenario: Push parses branch URI
- **WHEN** push targets canonical URI `dml://alice/demo#main`
- **THEN** the remote branch path is `refs/projects/alice/demo/heads/main.json`

#### Scenario: Fetch parses tag URI
- **WHEN** fetch targets canonical URI `dml://alice/demo@v1.0`
- **THEN** the remote tag path is `refs/projects/alice/demo/tags/v1.0.json`

#### Scenario: Raw URI is not used as remote path
- **WHEN** a remote operation handles `dml://alice/demo#main`
- **THEN** it does not use `dml://alice/demo#main` as a raw remote object path segment

### Requirement: Project creation owner default
The system SHALL default project owner to the configured current user when project creation omits an owner.

#### Scenario: Create project without owner
- **WHEN** the configured user is `alice` and project `demo` is created without an explicit owner
- **THEN** the project URI is `dml://alice/demo`

### Requirement: Fetch updates remote-tracking head
The system SHALL fetch a remote project branch by reading its branch head ref, materializing the referenced commit closure locally, and updating a local remote-tracking head.

#### Scenario: Fetch origin main
- **WHEN** `dml fetch origin main` succeeds
- **THEN** local storage contains the fetched commit closure and tracks `dml://alice/demo#main` as pointing to the fetched commit

#### Scenario: Fetch explicit project URI
- **WHEN** `dml fetch dml://alice/tools#main` succeeds
- **THEN** local storage contains the fetched commit closure and tracks `dml://alice/tools#main` as pointing to the fetched commit

#### Scenario: Fetch explicit project tag URI
- **WHEN** `dml fetch dml://alice/tools@v1.0` succeeds
- **THEN** local storage contains the fetched commit closure and tracks `dml://alice/tools@v1.0` as pointing to the fetched commit

### Requirement: Pull fetches and merges
The system SHALL implement branch pull as fetch followed by merge of the fetched remote-tracking head into the current branch.

#### Scenario: Pull origin main
- **WHEN** `dml pull origin main` succeeds while the current branch is `main`
- **THEN** local tracking ref `dml://alice/demo#main` is updated and local branch `main` advances to the merge result or fetched commit when already fast-forwardable

#### Scenario: Pull different branch fails
- **WHEN** the current branch is `feature` and the user runs `dml pull origin main`
- **THEN** pull fails without merging or advancing the current branch

### Requirement: Push uses ETag and fast-forward safety
The system SHALL update remote branch heads only with an ETag conditional write and SHALL reject non-fast-forward pushes unless force is requested.

#### Scenario: Fast-forward push
- **WHEN** the remote branch head is an ancestor of the local branch head and the observed ETag still matches
- **THEN** push updates the remote branch head to the local commit

#### Scenario: Non-fast-forward push rejected
- **WHEN** the remote branch head is not an ancestor of the local branch head and force is not requested
- **THEN** push fails without updating the remote branch head

#### Scenario: Force push keeps ETag safety
- **WHEN** force is requested and the observed ETag no longer matches
- **THEN** push fails without updating the remote branch head

#### Scenario: Push missing branch without create fails
- **WHEN** push targets a remote branch ref that does not exist and `--create` is not provided
- **THEN** push fails without creating the remote branch ref

#### Scenario: Push missing branch with create succeeds
- **WHEN** push targets a remote branch ref that does not exist and `--create` is provided
- **THEN** push writes the remote branch ref only if it still does not exist

#### Scenario: Create push loses race
- **WHEN** push uses `--create` and another client creates the remote branch ref first
- **THEN** push fails without overwriting the remote branch ref

### Requirement: Clone records origin
The system SHALL clone a remote project branch by initializing local state from the remote branch and recording the remote project as `origin`.

#### Scenario: Clone project
- **WHEN** `dml clone dml://alice/demo` succeeds
- **THEN** local config contains an `origin` remote for `dml://alice/demo` and local branch state is initialized from the selected remote branch

#### Scenario: Clone default branch
- **WHEN** `dml clone dml://alice/demo` omits an explicit branch commit-ish
- **THEN** clone fetches and initializes the configured default branch, falling back to `main`

#### Scenario: Clone explicit branch
- **WHEN** `dml clone dml://alice/demo#experiment` succeeds
- **THEN** clone fetches and initializes branch `experiment`
