## ADDED Requirements

### Requirement: CLI project commands delegate to a single DmlOps method
The `dml` CLI project command handlers SHALL remain thin adapters that parse command arguments and invoke exactly one `DmlOps` method per command path.

#### Scenario: Fetch delegates through DmlOps
- **WHEN** a user runs `dml fetch <remote-or-uri> [branch]`
- **THEN** the CLI handler parses inputs and calls one `DmlOps` fetch workflow method that performs remote synchronization behavior

#### Scenario: Checkout delegates through DmlOps
- **WHEN** a user runs `dml checkout <revision>`
- **THEN** the CLI handler parses the revision and calls one `DmlOps` checkout workflow method that returns attached/detached result details

#### Scenario: Merge delegates through DmlOps
- **WHEN** a user runs `dml merge <revision> --head <head-ref> --user <user>`
- **THEN** the CLI handler calls one `DmlOps` merge workflow method and does not instantiate commit/remote ops directly

### Requirement: CLI does not own git-like project business logic
The `_cli` layer SHALL NOT contain git-like project orchestration logic that coordinates repository state, commit resolution, or remote protocol execution.

#### Scenario: Project logic relocation
- **WHEN** git-like project command behavior requires cross-subsystem coordination
- **THEN** the implementation resides in `DmlOps` (and internal ops it invokes), while CLI code remains argument parsing and result forwarding only

### Requirement: Clone command composes via DmlOps workflow
The clone CLI entrypoint SHALL delegate clone workflow composition to a single `DmlOps` method after input parsing and command-level validation.

#### Scenario: Clone branch flow delegation
- **WHEN** a user runs `dml clone dml://alice/demo#main --bucket my-bucket`
- **THEN** the CLI entrypoint delegates to one `DmlOps` clone workflow method that performs fetch and checkout composition and returns clone result metadata

#### Scenario: Clone tag flow delegation
- **WHEN** a user runs `dml clone dml://alice/demo@v1.0 --bucket my-bucket`
- **THEN** the CLI entrypoint delegates to one `DmlOps` clone workflow method that performs fetch and detached checkout semantics through internal ops
