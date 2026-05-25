## Purpose
Define the git-like repository workflow contracts for revision resolution, checkout, merge, revert, DAG checkout, and shared `Dml` orchestration over commit/head/remote subsystems.

## Requirements

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
The system SHALL resolve revision values used by git-like commands to concrete local commit refs without performing network fetches. `HEAD` and ancestry expressions based on `HEAD` SHALL resolve through the repository's `.dml/HEAD` file.

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

#### Scenario: Resolve first-parent ancestry from HEAD file
- **WHEN** a command receives `HEAD~2` as a revision
- **THEN** the system resolves `HEAD` through `.dml/HEAD` and walks two first-parent steps from that resolved commit

#### Scenario: Resolve local tag shorthand
- **WHEN** a command receives `v1.0` as a revision and `v1.0` resolves as a local tag
- **THEN** the system resolves it to the commit referenced by that tag

### Requirement: Checkout repository state from revision
The system SHALL support checking out repository state from a resolved revision and SHALL distinguish branch-attached from detached checkouts by rewriting `.dml/HEAD`.

#### Scenario: Checkout branch attaches runtime
- **WHEN** `dml checkout main` resolves `main` to a local branch
- **THEN** the system writes `.dml/HEAD` as `ref: refs/local/heads/main` and reports branch-attached checkout

#### Scenario: Checkout tag detaches runtime
- **WHEN** `dml checkout v1.0` resolves `v1.0` to a tag target commit
- **THEN** the system writes `.dml/HEAD` as that detached commit and reports detached checkout at that commit

#### Scenario: Checkout commit expression detaches runtime
- **WHEN** `dml checkout HEAD~1` resolves to a concrete commit
- **THEN** the system writes `.dml/HEAD` as that detached commit and reports detached checkout at that commit

#### Scenario: Commit while detached does not advance branch or HEAD
- **WHEN** a user checks out a non-branch revision and then runs commit flow through `IndexOps.commit`
- **THEN** the system may create the new detached commit but does not advance any branch head and does not rewrite `.dml/HEAD`

### Requirement: Mutable project workflows require an attached branch
The system SHALL require `.dml/HEAD` to be attached to a local branch before default project workflows mutate branch history or publish a branch tip.

#### Scenario: Push uses attached HEAD branch by default
- **WHEN** `.dml/HEAD` is attached to local branch `foo` and the user runs project push without an explicit branch override
- **THEN** the system pushes local branch `foo` to remote branch URI `dml://<owner>/<project>#foo`

#### Scenario: Pull requires attached HEAD
- **WHEN** `.dml/HEAD` is detached and the user runs project pull without an explicit mutable branch target
- **THEN** the command fails instead of selecting a branch from config or environment

#### Scenario: Merge requires attached HEAD when defaulting destination
- **WHEN** `.dml/HEAD` is detached and the user runs a merge workflow that would otherwise target the current branch
- **THEN** the command fails because the current checkout is not a mutable branch target

#### Scenario: Checkout unresolved remote URI fails locally
- **WHEN** `dml checkout dml://alice/tools#main` is requested and no local tracking ref exists for that URI
- **THEN** checkout fails without implicit fetch and reports that the revision cannot be resolved locally

### Requirement: DAG removal remains explicit
The system SHALL remove DAG names from the current branch tree only through an explicit DAG removal command, not through DAG checkout of an absent source.

#### Scenario: Checkout absent DAG
- **WHEN** DAG checkout targets a commit that does not contain the requested DAG name
- **THEN** the command fails without deleting the target name from the current branch

### Requirement: Branch-targeted commit workflows update branches through HeadOps
The system SHALL perform branch advancement in git-like commit workflows through `HeadOps` public methods rather than direct head storage access.

#### Scenario: Merge updates branch through HeadOps
- **WHEN** a branch-targeted merge needs to fast-forward or store a merge commit
- **THEN** the workflow advances the branch through `HeadOps` using the expected current commit and the new commit

#### Scenario: Revert updates branch through HeadOps
- **WHEN** a branch-targeted revert creates a new commit
- **THEN** the workflow advances the branch through `HeadOps` rather than writing the head object directly

#### Scenario: DAG checkout updates branch through HeadOps
- **WHEN** DAG checkout creates a new commit on a branch
- **THEN** the workflow advances the branch through `HeadOps` rather than writing the head object directly

### Requirement: Repository inspection workflows resolve revisions locally
The system SHALL provide repository inspection workflows for `show`, `log`, and `diff` that resolve revisions locally without performing implicit network fetches.

#### Scenario: Show resolves revision locally
- **WHEN** a user runs `dml show origin/main`
- **THEN** the system resolves `origin/main` through existing local tracking state
- **AND** it does not contact the remote automatically

#### Scenario: Diff resolves both revisions locally
- **WHEN** a user runs `dml diff dml://alice/demo#main HEAD`
- **THEN** the system resolves both revisions from local state only

### Requirement: Branch creation and listing expose git-like branch inspection workflows
The system SHALL support creating a local branch from the current HEAD commit and listing locally tracked remote branches for git-like branch inspection.

#### Scenario: Branch remote lists tracked refs
- **WHEN** a user runs `dml branch --remote`
- **THEN** the system returns the set of locally tracked remote branch selectors

#### Scenario: Branch create copies the current head commit without moving HEAD
- **WHEN** a caller invokes `dml.branch("feature")` while HEAD is attached to `main`
- **THEN** the system creates local branch `feature` at the current HEAD commit
- **AND** HEAD remains attached to `main`

### Requirement: Repository status reports current DAG map and live indexes
The system SHALL provide a repository status workflow that reports the current HEAD state, local branches, the DAG map for the current revision, and live indexes.

#### Scenario: Status reports attached head
- **WHEN** HEAD is attached to branch `main` and a user runs `dml status`
- **THEN** the response reports attached head state for `main`
- **AND** includes the DAG map for the commit selected by that head

#### Scenario: Status reports detached head
- **WHEN** HEAD is detached and a user runs `dml status`
- **THEN** the response reports detached head state and the current commit

### Requirement: Show returns commit delta over DAG namespace
The system SHALL compute commit-introduced change for `dml show` as DAG-map additions, removals, and updates between the selected commit tree and its base tree.

#### Scenario: Show detects DAG addition
- **WHEN** a commit introduces `train -> dag:a` where the base tree had no `train`
- **THEN** `dml show` reports `train` under `change.added`

#### Scenario: Show detects DAG update
- **WHEN** a commit changes `train` from `dag:a` to `dag:b`
- **THEN** `dml show` reports `train` under `change.updated` with `before` and `after`

#### Scenario: Show detects DAG removal
- **WHEN** a commit removes `train -> dag:a`
- **THEN** `dml show` reports `train` under `change.removed`

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
