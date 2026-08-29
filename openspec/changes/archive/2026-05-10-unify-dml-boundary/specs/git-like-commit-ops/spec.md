## MODIFIED Requirements

### Requirement: Git-like project workflows are owned by `Dml` orchestration
Git-like project command workflows SHALL be available through the shared internal `Dml` orchestration boundary, which coordinates commit, head, and remote operations while delegating concrete repository actions to lower-level ops classes.

#### Scenario: Pull executes through Dml workflow
- **WHEN** a caller invokes project pull with remote target, branch target, and user context
- **THEN** `Dml` obtains project and remote context through `dml_context`, resolves any fuzzy selectors through its fuzzy-resolution submodule, performs remote synchronization, and applies merge behavior through internal ops

#### Scenario: Push executes through Dml workflow
- **WHEN** a caller invokes project push with remote target and push options
- **THEN** `Dml` obtains project and remote context through `dml_context`, performs project-aware remote push behavior through the relevant ops classes, and returns the push result through the shared boundary

#### Scenario: Revert executes through Dml workflow
- **WHEN** a caller invokes project revert with revision, branch target, and user context
- **THEN** `Dml` resolves the revision through its fuzzy-resolution submodule and performs revert behavior through `CommitOps`

#### Scenario: Checkout executes through Dml workflow
- **WHEN** a caller invokes repository checkout with a revision value
- **THEN** `Dml` resolves the revision through its fuzzy-resolution submodule and performs attached-vs-detached checkout behavior through the relevant ops classes

#### Scenario: Init runs through Dml-owned project setup
- **WHEN** a caller invokes repository init/bootstrap behavior
- **THEN** `Dml` initializes project state under `.dml/` in the current location through the shared internal boundary instead of requiring a separate bootstrap entrypoint

#### Scenario: Init recovers config-first partial state
- **WHEN** `.dml/config.toml` exists but `.dml/db/` is missing at init time
- **THEN** the Dml-owned init workflow uses `dml_context` to resolve bootstrap context, creates the missing DB state, and continues bootstrap behavior through the relevant ops classes
