## MODIFIED Requirements

### Requirement: CLI project commands delegate to a single DmlOps method
The `dml` CLI project command handlers SHALL remain thin adapters that parse command arguments and invoke exactly one workflow entrypoint per command path.

#### Scenario: Fetch delegates through DmlOps
- **WHEN** a user runs `dml fetch <remote-or-uri> [branch]`
- **THEN** the CLI handler parses inputs and calls one `DmlOps` fetch workflow method that performs remote synchronization behavior

#### Scenario: Checkout delegates through DmlOps
- **WHEN** a user runs `dml checkout <revision>`
- **THEN** the CLI handler parses the revision and calls one `DmlOps` checkout workflow method that returns attached/detached result details

#### Scenario: Merge delegates through DmlOps
- **WHEN** a user runs `dml merge <revision> --head <head-ref> --user <user>`
- **THEN** the CLI handler calls one `DmlOps` merge workflow method and does not instantiate commit/remote ops directly

#### Scenario: Clone delegates through internal operations entrypoint
- **WHEN** a user runs `dml clone <remote-uri> [options]`
- **THEN** the CLI handler parses inputs and calls one supported internal operations entrypoint for clone orchestration without invoking `DmlOps.clone`

### Requirement: Clone command composes via DmlOps workflow
The clone CLI entrypoint SHALL delegate clone workflow composition through supported internal operations after input parsing and command-level validation.

#### Scenario: Clone branch flow delegation
- **WHEN** a user runs `dml clone dml://alice/demo#main --bucket my-bucket`
- **THEN** the CLI entrypoint delegates to one internal clone orchestration path that performs fetch and checkout composition and returns clone result metadata without `DmlOps.clone`

#### Scenario: Clone tag flow delegation
- **WHEN** a user runs `dml clone dml://alice/demo@v1.0 --bucket my-bucket`
- **THEN** the CLI entrypoint delegates to one internal clone orchestration path that performs fetch and detached checkout semantics through internal ops without `DmlOps.clone`
