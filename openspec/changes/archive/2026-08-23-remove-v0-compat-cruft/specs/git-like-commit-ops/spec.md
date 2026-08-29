## MODIFIED Requirements

### Requirement: Git-like project workflows are owned by `Dml` orchestration
Git-like project command workflows SHALL be available through the shared internal `Dml` orchestration boundary, which coordinates commit, head, and remote operations while delegating concrete repository actions to lower-level ops classes.

#### Scenario: Pull executes through Dml workflow
- **WHEN** a caller invokes project pull with its current workflow inputs
- **THEN** `Dml` obtains project and remote context through shared configuration, performs remote synchronization, and applies merge behavior through internal ops

#### Scenario: Push executes through Dml workflow
- **WHEN** a caller invokes project push with current push options
- **THEN** `Dml` obtains project and remote context through shared configuration, performs project-aware remote push behavior through the relevant ops classes, and returns the push result through the shared boundary

#### Scenario: Revert executes through Dml workflow
- **WHEN** a caller invokes project revert with revision, branch target, and user context
- **THEN** `Dml` resolves the revision and performs revert behavior through repository ops

#### Scenario: Checkout executes through Dml workflow
- **WHEN** a caller invokes repository checkout with a revision value
- **THEN** `Dml` resolves the revision and performs attached-or-detached checkout behavior through repository ops

#### Scenario: Init runs through Dml-owned project setup
- **WHEN** a caller invokes repository init or bootstrap behavior
- **THEN** `Dml` initializes project state under `.dml/` through the shared internal boundary instead of requiring a separate bootstrap entrypoint

#### Scenario: Init recovers config-first partial state
- **WHEN** `.dml/config.json` contains only valid current keys but `.dml/db/` is missing at init time
- **THEN** the Dml-owned init workflow creates the missing DB state and continues bootstrap behavior through the relevant ops classes

#### Scenario: Init rejects obsolete partial config
- **WHEN** partial project state contains `.dml/config.toml` or removed JSON keys
- **THEN** init fails instead of interpreting or migrating the obsolete configuration
