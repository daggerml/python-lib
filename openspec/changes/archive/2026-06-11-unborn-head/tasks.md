## 1. HEAD Semantics

- [x] 1.1 Update `Head` to represent attached unborn branches as `commit=None` when the current branch ref file is absent.
- [x] 1.2 Keep detached HEAD commit-backed and reject detached init with no commit.

## 2. Revision And Merge Flows

- [x] 2.1 Update revision resolution to return `None` for valid selectors that do not currently resolve to a commit.
- [x] 2.2 Teach `CommitOps.merge(...)` and branch-targeted commit flows to accept an unborn destination.

## 3. Bootstrap And First Commit

- [x] 3.1 Remove synthetic initial commit creation from init.
- [x] 3.2 Materialize the branch ref only when the first real commit lands.

## 4. Tests

- [x] 4.1 Add contract coverage for unborn init, first commit, merge on unborn HEAD, and branch creation on unborn HEAD.
- [x] 4.2 Update status and branch-list tests to match the git-like unborn UX.
