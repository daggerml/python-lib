## MODIFIED Requirements

### Requirement: Git-like project workflows are owned by DmlOps orchestration
Git-like project command workflows SHALL execute through `DmlOps` orchestration methods that coordinate commit and remote operations without requiring CLI-owned business logic.

#### Scenario: Pull executes through DmlOps workflow
- **WHEN** a caller invokes project pull with remote target, head ref, and user context
- **THEN** `DmlOps` resolves project context, performs remote synchronization, and applies merge behavior through internal ops

#### Scenario: Push executes through DmlOps workflow
- **WHEN** a caller invokes project push with remote target and push options
- **THEN** `DmlOps` performs project-aware remote push behavior and returns the push result without CLI-managed remote orchestration

#### Scenario: Revert executes through DmlOps workflow
- **WHEN** a caller invokes project revert with revision, head ref, and user context
- **THEN** `DmlOps` resolves the revision and performs revert behavior through internal commit operations

#### Scenario: Init runs as in-place project setup
- **WHEN** a caller invokes `DmlOps.init`
- **THEN** it initializes project state under `.dml/` in the current location instead of creating a separate project directory

#### Scenario: Init recovers config-first partial state
- **WHEN** `.dml/config.toml` exists but `.dml/db/` is missing at init time
- **THEN** `DmlOps.init` creates the missing DB state and continues bootstrap behavior based on resolved configuration
