# thin-cli-routing Specification

## Purpose
TBD - created by archiving change thin-cli-git-ops-routing. Update Purpose after archive.
## Requirements
### Requirement: CLI project commands delegate to a single Dml workflow method
The `dml` CLI project command handlers SHALL remain thin adapters that parse ref and source-selection arguments and invoke exactly one workflow entrypoint per command path.

#### Scenario: Fetch delegates through Dml
- **WHEN** a user runs `dml fetch [--dep DEP] [BRANCH|@TAG]`
- **THEN** the CLI calls one shared `Dml` fetch method with the selected dependency and ref

#### Scenario: Checkout delegates through Dml
- **WHEN** a user runs checkout with a revision and optional `--remote`
- **THEN** the CLI calls one shared `Dml` checkout method that returns attached or detached result details

#### Scenario: Merge delegates through Dml
- **WHEN** a user runs merge with a revision and optional `--remote`
- **THEN** the CLI calls one shared `Dml` merge method and does not instantiate commit or remote ops directly

### Requirement: CLI does not own git-like project business logic
The `_cli` layer SHALL NOT contain git-like project orchestration logic that coordinates repository state, commit resolution, or remote protocol execution.

#### Scenario: Project logic relocation
- **WHEN** git-like project command behavior requires cross-subsystem coordination
- **THEN** the implementation resides in the shared `Dml` workflow layer and the internal ops it invokes, while CLI code remains argument parsing and result forwarding only
