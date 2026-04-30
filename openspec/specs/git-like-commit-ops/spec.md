## ADDED Requirements

### Requirement: Merge advances current head
The system SHALL merge another commit or branch into the current branch by creating a merge commit when needed and advancing the current head.

#### Scenario: Merge non-conflicting branch
- **WHEN** a user merges a branch whose tree changes do not conflict with the current branch
- **THEN** the system creates a merge commit with both commits as parents and advances the current head to that merge commit

#### Scenario: Merge fast-forward
- **WHEN** the current branch head is an ancestor of the merged commit
- **THEN** the system advances the current head to the merged commit without creating an unnecessary merge commit

### Requirement: Merge detects DAG-name conflicts
The system SHALL reject merges where both sides changed the same DAG name to different DAG refs since the merge base.

#### Scenario: Conflicting DAG name
- **WHEN** the merge base has `train -> dag:a`, the current branch has `train -> dag:b`, and the merged branch has `train -> dag:c`
- **THEN** merge fails with a conflict naming `train` and does not advance the current head

### Requirement: Revert commit creates inverse commit
The system SHALL revert a commit by applying the inverse of that commit's tree diff to the current branch as a new commit.

A revert SHALL only modify a DAG name when the current tree still matches the post-commit value introduced by the reverted commit. If the current tree no longer matches that post-commit value, revert SHALL fail with a conflict and SHALL NOT advance the current branch.

#### Scenario: Revert added DAG
- **WHEN** the reverted commit added DAG name `train`
- **THEN** the revert commit removes `train` from the current branch tree if safe to apply

#### Scenario: Revert changed DAG
- **WHEN** the reverted commit changed `train` from `dag:a` to `dag:b`
- **THEN** the revert commit changes `train` back to `dag:a` if the current tree still permits safe application

#### Scenario: Revert changed DAG conflict
- **WHEN** the reverted commit changed `train` from `dag:a` to `dag:b` and the current tree has `train -> dag:c`
- **THEN** revert fails with a conflict naming `train` and does not advance the current branch

#### Scenario: Revert added DAG conflict
- **WHEN** the reverted commit added `train -> dag:a` and the current tree has `train -> dag:b`
- **THEN** revert fails with a conflict naming `train` and does not advance the current branch

#### Scenario: Revert removed DAG conflict
- **WHEN** the reverted commit removed `train -> dag:a` and the current tree already has `train -> dag:b`
- **THEN** revert fails with a conflict naming `train` and does not advance the current branch

### Requirement: DAG checkout from revision
The system SHALL support checking out one DAG from a resolved revision into the current branch tree and committing that change.

#### Scenario: Checkout DAG with same name
- **WHEN** `dml dag checkout HEAD~1 train` resolves `HEAD~1` to a commit containing `train -> dag:a`
- **THEN** the system creates a new commit whose tree contains `train -> dag:a` and advances the current head

#### Scenario: Checkout DAG with alias
- **WHEN** `dml dag checkout origin/main train --as baseline_train` resolves `origin/main` to a commit containing `train -> dag:a`
- **THEN** the system creates a new commit whose tree contains `baseline_train -> dag:a` and advances the current head

#### Scenario: Checkout refuses overwrite by default
- **WHEN** the target name already exists with a different DAG ref and `--replace` is not provided
- **THEN** DAG checkout fails without creating a commit or advancing the current head

#### Scenario: Checkout replaces when requested
- **WHEN** the target name already exists with a different DAG ref and `--replace` is provided
- **THEN** DAG checkout creates a new commit with the target name pointing to the checked-out DAG ref

### Requirement: Revision resolution
The system SHALL resolve revision values used by git-like commands to concrete local commit refs without performing network fetches.

#### Scenario: Resolve branch shorthand
- **WHEN** a command receives `main` as a revision
- **THEN** the system resolves it as local branch `main`

#### Scenario: Resolve remote-tracking branch shorthand
- **WHEN** a command receives `origin/main` as a revision
- **THEN** the system resolves it through the configured remote URI to local tracking ref `dml://<owner>/<project>#main`

#### Scenario: Resolve fetched DML branch URI
- **WHEN** a command receives `dml://alice/tools#main` as a revision and that tracking ref exists locally
- **THEN** the system resolves it to the commit stored for that tracking ref

#### Scenario: Resolve fetched DML tag URI
- **WHEN** a command receives `dml://alice/tools@v1.0` as a revision and that tracking ref exists locally
- **THEN** the system resolves it to the commit stored for that tracking ref

#### Scenario: Unfetched DML URI is not fetched implicitly
- **WHEN** a command receives `dml://alice/tools#main` as a revision and no matching local tracking ref exists
- **THEN** the command fails without contacting the remote

#### Scenario: Resolve first-parent ancestry
- **WHEN** a command receives `HEAD~2` as a revision
- **THEN** the system resolves it by walking two first-parent steps from the current head commit

#### Scenario: Resolve local tag shorthand
- **WHEN** a command receives `v1.0` as a revision and `v1.0` resolves as a local tag
- **THEN** the system resolves it to the commit referenced by that tag

### Requirement: Checkout repository state from revision
The system SHALL support checking out repository state from a resolved revision and SHALL distinguish branch-attached from detached checkouts.

#### Scenario: Checkout branch attaches runtime
- **WHEN** `dml checkout main` resolves `main` to a local branch
- **THEN** the system sets active HEAD to branch `main` and reports branch-attached checkout

#### Scenario: Checkout tag detaches runtime
- **WHEN** `dml checkout v1.0` resolves `v1.0` to a tag target commit
- **THEN** the system clears active HEAD and reports detached checkout at that commit

#### Scenario: Checkout commit expression detaches runtime
- **WHEN** `dml checkout HEAD~1` resolves to a concrete commit
- **THEN** the system clears active HEAD and reports detached checkout at that commit

#### Scenario: Commit while detached does not advance branch
- **WHEN** a user checks out a non-branch revision and then runs commit flow through `IndexOps.commit`
- **THEN** the system commits the index without advancing any branch head

#### Scenario: Checkout unresolved remote URI fails locally
- **WHEN** `dml checkout dml://alice/tools#main` is requested and no local tracking ref exists for that URI
- **THEN** checkout fails without implicit fetch and reports that the revision cannot be resolved locally

### Requirement: DAG removal remains explicit
The system SHALL remove DAG names from the current branch tree only through an explicit DAG removal command, not through DAG checkout of an absent source.

#### Scenario: Checkout absent DAG
- **WHEN** DAG checkout targets a commit that does not contain the requested DAG name
- **THEN** the command fails without deleting the target name from the current branch
## Requirements
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
