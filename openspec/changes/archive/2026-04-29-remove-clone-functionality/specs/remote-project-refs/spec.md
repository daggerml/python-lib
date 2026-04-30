## MODIFIED Requirements

### Requirement: Project directory initialization
The system SHALL initialize local project state under `<project-directory>/.dml/` for `init`.

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

#### Scenario: Hook environment is provided
- **WHEN** a `post-init` hook command runs
- **THEN** the process environment includes `DML_HOOK`, `DML_PROJECT_HOME`, `DML_PROJECT_NAME`, `DML_PROJECT_OWNER`, `DML_CONFIG_HOME`, and `DML_BRANCH`

## REMOVED Requirements

### Requirement: Init and clone shell hooks
**Reason**: Clone hooks are removed because clone is no longer a supported workflow.
**Migration**: Keep bootstrap automation in `post-init` hooks and run explicit remote commands after initialization when needed.

### Requirement: Clone records origin
**Reason**: Clone is removed; automatic origin recording during clone no longer applies.
**Migration**: Configure remotes through init-time/project config workflows and use explicit fetch/push/pull commands.
