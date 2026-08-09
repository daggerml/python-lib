## Purpose
Define the git-like repository workflow contracts for revision resolution, checkout, merge, revert, DAG checkout, and shared `Dml` orchestration over commit/head/remote subsystems.

## Requirements

### Requirement: History mutation may select fetched remote revisions
Merge, rebase, and revert SHALL expose `remote=False` to select the revision from local remote tracking state. These workflows SHALL NOT expose dependency selection and SHALL NOT fetch implicitly.

#### Scenario: Merge fetched remote revision
- **WHEN** merge receives revision `main` with `remote=True`
- **THEN** it merges the commit from `.dml/refs/remote/heads/main`

#### Scenario: Rebase onto fetched remote revision
- **WHEN** rebase receives revision `main` with `remote=True`
- **THEN** it rebases onto the locally tracked remote commit

#### Scenario: Revert fetched remote revision
- **WHEN** revert receives revision `@v1` with `remote=True`
- **THEN** it reverts the commit from `.dml/refs/remote/tags/v1`

#### Scenario: Missing remote revision does not fetch
- **WHEN** a history mutation selects an absent remote tracking revision
- **THEN** it fails with fetch-required guidance without network access or history changes

### Requirement: Tag creation may select a fetched remote revision
Tag creation SHALL accept a namespace-independent revision and `remote=False`. With `remote=True`, it SHALL resolve the revision from existing remote tracking state. It SHALL NOT expose dependency selection or fetch implicitly.

#### Scenario: Create local tag from remote tracking branch
- **WHEN** local tag `baseline` is created from revision `main` with `remote=True`
- **THEN** `baseline` points to the already-fetched remote tracking commit for `main`

#### Scenario: Missing remote tag source does not fetch
- **WHEN** tag creation selects an absent remote tracking revision
- **THEN** it fails with fetch-required guidance without creating the local tag

### Requirement: Merge advances current head
The system SHALL merge another commit or branch into the current branch by creating a merge commit when needed and advancing the current head. When the current attached head has no resolved commit because the branch is unborn, merge SHALL treat that destination as empty history and advance the current head to the merged commit without requiring a synthetic base commit.

#### Scenario: Merge non-conflicting branch
- **WHEN** a user merges a branch whose tree changes do not conflict with the current branch
- **THEN** the system creates a merge commit with both commits as parents and advances the current head to that merge commit

#### Scenario: Merge fast-forward
- **WHEN** the current branch head is an ancestor of the merged commit
- **THEN** the system advances the current head to the merged commit without creating an unnecessary merge commit

#### Scenario: Merge into unborn attached head
- **WHEN** `.dml/HEAD` is attached to local branch `main`
- **AND** branch `main` has no materialized commit ref yet
- **AND** the merged revision resolves to commit `commit:abc123`
- **THEN** the system advances branch `main` directly to `commit:abc123`
- **AND** it does not require or create a synthetic initial commit

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
The system SHALL accept every supported revision grammar form with local, remote, or dependency source selection and resolve it to a concrete commit without network access when possible. A normalized source argument SHALL select local refs by default, remote tracking refs when `remote=True`, or dependency tracking refs when `dep=<name>`. Exact commit IDs and refs SHALL resolve from the local object database regardless of selected source. The only invalid source-argument combination is simultaneous `remote=True` and non-null `dep`; any otherwise unresolvable combination SHALL raise a descriptive resolution error.

#### Scenario: Resolve branch shorthand
- **WHEN** a command receives `main` as a revision
- **THEN** the system resolves it as local branch `main`

#### Scenario: Resolve remote-tracking branch shorthand
- **WHEN** a command receives `origin/main` as a revision
- **THEN** the system resolves it through the local tracking ref for remote `origin` branch `main`

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
The system SHALL require `.dml/HEAD` to be attached to a local branch before default project workflows mutate branch history or publish a branch tip. Default push SHALL use the current branch's configured upstream. If the current branch is untracked, default push SHALL publish to `origin/<local-branch>` and record that upstream only after publication succeeds.

#### Scenario: Push uses configured upstream
- **WHEN** `.dml/HEAD` is attached to local branch `foo` tracking `research/main` and the user runs push
- **THEN** the system pushes local branch `foo` to remote branch `research/main`

#### Scenario: First push establishes origin upstream
- **WHEN** attached local branch `foo` has no upstream and the user runs push successfully
- **THEN** the system publishes to `origin/foo`
- **AND** configures `foo` to track `origin/foo`

#### Scenario: Failed first push leaves branch untracked
- **WHEN** attached local branch `foo` has no upstream and publication to `origin/foo` fails
- **THEN** `foo` remains untracked

#### Scenario: Push requires attached HEAD
- **WHEN** `.dml/HEAD` is detached and the user runs push
- **THEN** the command fails

### Requirement: DAG removal remains explicit
The system SHALL remove DAG names from the current branch tree only through an explicit DAG removal command, not through DAG checkout of an absent source.

#### Scenario: Checkout absent DAG
- **WHEN** DAG checkout targets a commit that does not contain the requested DAG name
- **THEN** the command fails without deleting the target name from the current branch

### Requirement: Commits do not carry a current DAG pointer
The system SHALL model commits as immutable history records containing parent refs, a tree ref, and commit metadata, and SHALL NOT expose a dedicated commit-level current-DAG field.

#### Scenario: Commit description omits current DAG field
- **WHEN** the system describes a commit for history inspection
- **THEN** the description includes commit metadata and the commit tree's DAG map
- **AND** it does not include `commit.dag` or any equivalent commit-level current-DAG pointer

#### Scenario: Unnamed finalized execution DAG stays out of history
- **WHEN** runtime work finalizes an execution DAG without adding a named DAG entry to the commit tree
- **THEN** the finalized DAG is returned as a durable DAG ref
- **AND** no history commit is created for that finalization
- **AND** the finalized DAG is not reintroduced as a dedicated field on any commit object

### Requirement: Runtime commit only advances history for named DAG publication
The system SHALL finalize runtime DAGs independently from history updates. A runtime commit operation SHALL always return the finalized DAG ref and SHALL only create or advance commit history when the finalized DAG is published into the commit tree under a name.

#### Scenario: Named runtime commit creates history and returns finalized DAG
- **WHEN** runtime work finalizes a DAG with `name` set
- **THEN** the operation returns the finalized DAG ref
- **AND** it also creates a commit whose tree records that DAG under the given name

#### Scenario: Unnamed runtime commit does not advance HEAD
- **WHEN** runtime work finalizes a DAG with `name` unset
- **THEN** the operation returns the finalized DAG ref
- **AND** it does not create a commit
- **AND** it does not change `HEAD` or the current branch ref

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

### Requirement: Branch creation and listing expose tracked remote workflows
The system SHALL support `branch create [--remote REMOTE] [--revision REV] NAME`. `REMOTE` SHALL default to `origin`; the created local branch SHALL track `REMOTE/NAME`. When revision is omitted and `REMOTE/NAME` exists remotely, creation SHALL fetch that branch and initialize the local branch at its tip. Otherwise, omitted revision SHALL retain current-HEAD and unborn-branch behavior. An explicit revision SHALL always take precedence over a matching remote branch.

#### Scenario: Existing remote branch initializes new local branch
- **WHEN** `dml branch create feature` is invoked and remote `origin` has branch `feature`
- **THEN** the system fetches `origin/feature`, creates local `feature` at that commit, and configures it to track `origin/feature`

#### Scenario: Selected remote initializes new local branch
- **WHEN** `dml branch create --remote research feature` is invoked and remote `research` has branch `feature`
- **THEN** the system creates local `feature` at the fetched `research/feature` commit and configures that upstream

#### Scenario: Explicit revision overrides remote tip
- **WHEN** `dml branch create --revision HEAD~1 feature` is invoked and `origin/feature` exists
- **THEN** local `feature` points to `HEAD~1` and tracks `origin/feature`

#### Scenario: Missing remote branch uses current head
- **WHEN** `dml branch create feature` is invoked, `origin/feature` does not exist, and HEAD resolves to a concrete commit
- **THEN** local `feature` points to the current HEAD commit and tracks `origin/feature`

#### Scenario: Branch list omits unborn current branch
- **WHEN** HEAD is attached to unborn branch `main`
- **THEN** `dml branch list` does not include `main` until that branch ref is materialized

### Requirement: First branch commit materializes an unborn branch ref
The system SHALL materialize the current branch ref when the first history-producing commit is finalized on an attached unborn branch.

#### Scenario: First named commit on unborn branch writes ref
- **WHEN** HEAD is attached to unborn branch `main`
- **AND** runtime finalization produces the first history commit `commit:abc123`
- **THEN** the system writes `.dml/refs/local/heads/main` pointing to `commit:abc123`
- **AND** HEAD remains attached to `main`

### Requirement: Repository status reports upstream synchronization
The system SHALL report the current branch's configured upstream and ahead/behind counts relative to its local remote-tracking ref when available.

#### Scenario: Status reports configured upstream
- **WHEN** attached branch `feature` tracks `origin/main`
- **THEN** status identifies `origin/main` as its upstream

#### Scenario: Status reports synchronization counts
- **WHEN** the upstream tracking ref exists and current branch differs from it
- **THEN** status reports the computed ahead and behind counts

#### Scenario: Status reports unavailable upstream counts
- **WHEN** the current branch has no upstream or its upstream has not been fetched
- **THEN** status reports unavailable ahead and behind counts without inferring an upstream by local branch name

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
