## 1. Persisted Remote And Upstream State

- [x] 1.1 Extend repository configuration to persist slash-free named remotes and add compatibility migration from legacy `remote.project` to `origin`, with tests for normalized reads, writes, and invalid remote names.
- [x] 1.2 Add persisted optional per-branch upstream metadata plus head operations for reading, setting, renaming, and deleting it; cover branch rename/delete and legacy untracked branches.
- [x] 1.3 Move or compatibly resolve remote-tracking refs under named remote namespaces, including GC-root enumeration and migration of refs belonging to legacy `origin`.

## 2. Named Remote Synchronization

- [x] 2.1 Add public named remote `add`, `list`, and `delete` operations, rejecting deletion while a local branch tracks the remote; expose the generated CLI surface and CLI contract tests.
- [x] 2.2 Redesign fetch to accept an optional named remote, default to `origin`, enumerate that remote's branch and tag refs, materialize their closures, update only that remote's tracking refs; preserve branch- and tag-qualified URI fetch as an explicit one-off path.
- [x] 2.3 Update offline revision resolution to resolve `remote/branch` and remote-tag selectors through named tracking refs without implicit network access.

## 3. Branch And Upstream Workflows

- [x] 3.1 Replace branch creation's positional revision API with `branch create [--remote REMOTE] [--revision REV] NAME`, recording `REMOTE/NAME` as upstream and preserving existing explicit-revision and unborn-branch behavior.
- [x] 3.2 Implement omitted-revision branch creation from an existing `REMOTE/NAME` remote tip, with targeted fetch and fallback to current HEAD when that remote branch is absent.
- [x] 3.3 Add `branch set-upstream REMOTE/BRANCH` for the attached branch and test malformed, unknown-remote, detached-HEAD, and successful reassignment cases.

## 4. Upstream-Based Pull, Push, And Status

- [x] 4.1 Change pull to require an attached branch with an upstream, refresh that upstream remote, and merge its tracking ref; reject positional pull arguments and untracked branches.
- [x] 4.2 Change default push to publish the attached branch to its configured upstream and retain conditional non-fast-forward protection; for an untracked branch, publish to `origin/<local-name>` and set that upstream only after success.
- [x] 4.3 Extend status with nullable upstream identity and calculate ahead/behind only against its matching fetched tracking ref.

## 5. Bootstrap, Documentation, And Verification

- [x] 5.1 Update init and clone workflows so configured project remotes become `origin`, cloned branches track `origin/<branch>`, and tag clones remain detached without a branch upstream.
- [x] 5.2 Update the full Bash CLI workflow to demonstrate named remote fetch, an alias branch tracking `origin/main`, upstream-based pull, and remote deletion naming.
- [x] 5.3 Update CLI, configuration, history/remotes, and remotes-and-sync documentation to describe named remotes, branch upstreams, remote-tracking selectors, and the new command grammar.
- [x] 5.4 Run focused unit and remote-sync integration tests, generated CLI contract tests, `uv run bash examples/run-all-examples.sh --make-temp --run bash_full_cli_workflow`, and the repository's required non-slow validation suite.
