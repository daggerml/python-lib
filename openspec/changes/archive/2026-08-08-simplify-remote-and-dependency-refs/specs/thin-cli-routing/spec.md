## MODIFIED Requirements

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
