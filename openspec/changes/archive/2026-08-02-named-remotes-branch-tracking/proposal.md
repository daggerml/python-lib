## Why

Project synchronization currently infers a remote branch from the local branch name. This breaks local aliases and makes `pull`, `push`, and ahead/behind status unreliable when a branch should follow a differently named remote branch or a different project.

## What Changes

- Add named project remotes, with `origin` as the default remote migrated from existing `remote.project` configuration.
- Add per-local-branch upstream configuration containing a remote name and remote branch name.
- **BREAKING** Replace positional `pull`/`push` target behavior with upstream-based pull/push behavior; preserve explicit URI fetch for one-off ref retrieval.
- Make `fetch [REMOTE]` discover and update all branch and tag tracking refs for the selected named remote.
- Add `branch create [--remote REMOTE] [--revision REV] NAME`, including automatic tracking of `REMOTE/NAME` and remote-tip initialization when no revision is supplied.
- Add explicit upstream reassignment through `branch set-upstream REMOTE/BRANCH`.
- Make an untracked branch's bare `push` create `origin/<local-branch>` and establish that upstream after successful publication.
- Rename the remote-removal operation to `remote delete`.
- Report the configured upstream and its ahead/behind relationship in status.

## Capabilities

### New Capabilities
- `named-remote-branch-tracking`: Named remote configuration and per-branch upstream lifecycle.

### Modified Capabilities
- `remote-project-refs`: Remote fetching, tracking-ref identity, configured project sync, and pull behavior change to named remotes and upstreams.
- `clone-bootstrap-workflow`: Clone records and configures the `origin` remote and cloned branch upstream.
- `git-like-commit-ops`: Revision resolution, branch creation, default push, pull, and status use named remote-tracking refs and branch upstreams.
- `generated-dml-cli`: Generated command signatures expose the revised remote and branch command surface.

## Impact

- Affects `Dml`, `_BranchNamespace`, configuration, head/ref persistence, revision resolution, remote transport operations, and generated CLI commands.
- Changes persisted repository configuration and local remote-tracking ref layout; existing `remote.project` repositories require migration to `origin` and retain untracked branches until an upstream is established.
- Updates remote-sync integration tests, CLI contract tests, the complete Bash CLI workflow, and remote/history, configuration, CLI, and architecture documentation.
