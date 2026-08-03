## Context

See proposal.md for motivation. Current configuration stores one `remote.project`; remote-tracking refs are keyed by owner/project; and default pull, push, and status infer the upstream branch from the local branch name. The generated CLI mirrors public `Dml` signatures, so API signature changes define the command grammar.

## Goals / Non-Goals

**Goals:**

- Persist named project remotes and per-branch upstream intent independently from cached tracking refs.
- Make a default `origin` workflow convenient while allowing a branch to track another project or differently named branch.
- Preserve non-fast-forward publication safety and local-only revision resolution.
- Migrate repositories with persisted `remote.project` without requiring user intervention.

**Non-Goals:**

- Supporting Git's complete refspec, remote URL, fetch refspec, pruning, or multi-upstream models.
- Changing `remote.root`, CAS transport, remote execution, or remote project ref wire format.
- Providing positional remote overrides to pull or push.

## Decisions

### Named remotes replace the single default project

Repository configuration will store a mapping from a slash-free remote name to a branchless DML project URI. Existing `remote.project` is interpreted and persisted as `origin` when configuration is loaded or written. The public remote namespace provides `add`, `list`, and `delete`; `delete` is chosen over `remove` to match branch and tag commands.

The alternative, retaining `remote.project` alongside optional named remotes, would leave two competing defaults and ambiguous migration rules.

### Upstreams are branch metadata

Each local branch has optional metadata `{remote: str, merge: str}`. The upstream is not inferred from a tracking ref or local branch name. Branch rename migrates its metadata; branch delete removes it; `branch set-upstream REMOTE/BRANCH` updates the attached branch.

The alternative of deriving upstream by same branch name recreates the reproduced alias failure. Supporting multiple upstreams is excluded because pull and status need one unambiguous target.

### Tracking refs are keyed by remote name

Local tracking refs move to a remote-name namespace, with heads represented as `origin/main` and tags as `origin@v1`. Fetch enumerates all project branch and tag refs for one selected remote and materializes each closure before updating its tracking pointer. Revision resolution remains offline and resolves named remote selectors only from these cached refs.

An explicit branch- or tag-qualified DML URI remains a one-off fetch path. It updates the existing URI-keyed tracking ref and does not create or modify a named remote or branch upstream.

The alternative URI-keyed layout exposes project identity but cannot express a stable user-selected remote name or naturally support `fetch origin`.

### Branch creation establishes intent before synchronization

`branch create [--remote REMOTE] [--revision REV] NAME` records `REMOTE/NAME` as upstream. With an omitted revision, it checks the remote branch: an existing branch is fetched and becomes the local tip; an absent branch falls back to the current branch-creation behavior. An explicit revision never triggers remote-tip substitution.

This gives `branch create foo` the familiar checkout-like behavior when `origin/foo` exists without making explicit local-history selection surprising.

### Pull and push are upstream-only operations

`pull` fetches the current upstream's remote and merges its tracking ref. `push` publishes to the current upstream. Both accept no positional remote argument. For an untracked attached branch, successful bare push creates `origin/<local-name>` and then records the upstream; failure leaves metadata unchanged.

The alternative `pull origin` or `push origin` would override persisted branch intent at the call site and still leave the target branch ambiguous.

### Status reflects configured intent

Status adds a nullable upstream selector. Ahead/behind is calculated only when the current branch has a configured upstream with a locally fetched tracking ref; otherwise both counts remain unavailable. This intentionally replaces same-name inference.

## Risks / Trade-offs

- [Persisted configuration migration can leave partially upgraded state] -> Read legacy `remote.project` as `origin` and atomically write normalized configuration before mutating refs.
- [A remote branch can change between branch-create discovery and fetch] -> Use the fetched tip as the created branch tip; subsequent pull/push retain normal merge and conditional-publication checks.
- [Deleting a remote leaves branches with invalid upstreams] -> Reject deletion while it is referenced by an upstream, requiring reassignment first.
- [Fetching all refs transfers more data than a single-ref fetch] -> Preserve concurrent fetch worker configuration and let users choose a specific named remote.
- [The ref-layout change can affect local GC roots and legacy tracking refs] -> Enumerate both layouts during migration or rewrite legacy tracking refs before retiring the old layout.

## Migration Plan

1. Normalize existing `remote.project` configuration into named remote `origin` while keeping unassociated existing local branches untracked.
2. Migrate or recognize existing URI-keyed tracking refs as `origin` tracking refs when their project URI matches the migrated origin project.
3. Update clone and init paths to create `origin` directly; clone records the selected branch upstream.
4. Roll back code only before configuration/refs are migrated. After migration, compatibility readers must remain until a separately planned format-removal change.
