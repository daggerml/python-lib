## MODIFIED Requirements

### Requirement: CLI project commands delegate to a single internal API method
The `dml` CLI project command handlers SHALL remain thin adapters that parse command arguments and invoke exactly one supported `daggerml._internal` API entrypoint per command path.

#### Scenario: Fetch delegates through internal API
- **WHEN** a user runs `dml fetch <remote-or-uri> [branch]`
- **THEN** the CLI handler parses inputs and calls one internal fetch workflow entrypoint that performs remote synchronization behavior

#### Scenario: Checkout delegates through internal API
- **WHEN** a user runs `dml checkout <revision>`
- **THEN** the CLI handler parses the revision and calls one internal checkout workflow entrypoint that returns attached/detached result details

#### Scenario: Merge delegates through internal API
- **WHEN** a user runs `dml merge <revision> --head <head-ref> --user <user>`
- **THEN** the CLI handler calls one internal merge workflow entrypoint and does not instantiate commit/remote ops directly

#### Scenario: Init delegates through internal API
- **WHEN** a user runs `dml init <name-or-flags>`
- **THEN** the CLI handler parses inputs and calls one internal init workflow entrypoint without composing additional bootstrap workflows in CLI code

## REMOVED Requirements

### Requirement: Clone command composes via DmlOps workflow
**Reason**: Clone is removed from the product surface to enforce an init-first lifecycle and eliminate duplicate bootstrap orchestration.
**Migration**: Initialize projects with `dml init` and then run explicit remote synchronization commands (`dml fetch`, `dml checkout`, `dml pull`) as needed.
