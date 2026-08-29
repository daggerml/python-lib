## MODIFIED Requirements

### Requirement: Git-like project workflows are owned by `Dml` orchestration
Git-like project command workflows SHALL continue to execute through the shared internal `Dml` orchestration boundary, but commit and related repository ops SHALL no longer own hidden constructor-injected DB state. Any DB-using commit workflow SHALL execute against an explicit `db` context supplied by the caller.

#### Scenario: Revert executes through Dml workflow
- **WHEN** a caller invokes project revert with revision, branch target, and user context
- **THEN** `Dml` resolves the revision through its fuzzy-resolution submodule and performs revert behavior through `CommitOps` using an explicit DB context passed into the workflow

#### Scenario: Checkout executes through Dml workflow
- **WHEN** a caller invokes repository checkout with a revision value
- **THEN** `Dml` resolves the revision through its fuzzy-resolution submodule and performs attached-vs-detached checkout behavior through the relevant ops classes using explicit DB contexts

#### Scenario: Merge updates branch through explicit DB context
- **WHEN** a branch-targeted merge needs to inspect commit ancestry and create or fast-forward the result
- **THEN** the merge workflow performs its DB-backed work through explicit DB arguments rather than through constructor-owned DB state on `CommitOps` or collaborating ops classes
