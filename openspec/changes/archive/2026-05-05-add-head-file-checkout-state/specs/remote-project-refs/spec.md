## MODIFIED Requirements

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
- **THEN** the command uses configured values such as `remote.project`, `remote.uri`, or `default_branch` but not a config-derived current branch

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
- **THEN** the corresponding supported project config value is overridden for that command

#### Scenario: Remote env overrides config
- **WHEN** `DML_REMOTE`, `DML_REMOTE_URI`, `DML_REMOTE_BUCKET`, or `DML_REMOTE_PREFIX` is set
- **THEN** the corresponding remote selection or storage value is overridden for that command

#### Scenario: Hook context env is provided by DML
- **WHEN** a hook command runs
- **THEN** DML sets `DML_HOOK`, `DML_PROJECT_HOME`, and, for clone hooks, `DML_REMOTE_NAME`

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

#### Scenario: Init creates initial branch and attaches HEAD
- **WHEN** `dml init demo` succeeds
- **THEN** local storage contains an initial empty commit/tree, local branch `main`, and `.dml/HEAD` attached to `main`

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
