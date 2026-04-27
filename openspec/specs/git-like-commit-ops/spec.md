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

### Requirement: Clone composes fetch then checkout
The system SHALL implement clone as `fetch` followed by `checkout`, using the fetched target revision for checkout semantics.

#### Scenario: Clone branch uses fetch then attached checkout
- **WHEN** `dml clone dml://alice/tools#main` is requested
- **THEN** the system fetches `dml://alice/tools#main` and checks out `main` as a branch-attached HEAD

#### Scenario: Clone tag uses fetch then detached checkout
- **WHEN** `dml clone dml://alice/tools@v1.0` is requested
- **THEN** the system fetches `dml://alice/tools@v1.0` and checks out the resolved commit in detached mode

#### Scenario: Clone direct commit is not supported yet
- **WHEN** `dml clone <uri>@<commit-ref>` is requested for a direct commit target that is not fetchable as a branch/tag ref
- **THEN** clone fails with an error indicating direct commit clone is unsupported until fetch supports commit-target retrieval

#### Scenario: Clone does not run init hooks
- **WHEN** `dml clone` initializes a local repository for the first time
- **THEN** the system does not invoke `dml init` and does not run init hooks as part of clone

### Requirement: DAG removal remains explicit
The system SHALL remove DAG names from the current branch tree only through an explicit DAG removal command, not through DAG checkout of an absent source.

#### Scenario: Checkout absent DAG
- **WHEN** DAG checkout targets a commit that does not contain the requested DAG name
- **THEN** the command fails without deleting the target name from the current branch
