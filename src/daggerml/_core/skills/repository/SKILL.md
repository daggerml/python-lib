---
name: daggerml-repository
description: Use when managing DaggerML project state, history, remotes, dependencies, cache, or garbage collection. Applies DML-specific repository and shared-state conventions.
---

# DaggerML Repository Management

Treat the DaggerML docs and current source as the authority for command behavior.
Use this skill to decide **which state to inspect and when to change it**, not
to replace CLI help. Use `dml` for repository operations; never edit managed
`.dml/` state directly.

## Establish The Project And Endpoint

Run commands from an initialized project (`dml init`) or select its project home
explicitly. DML does not search parent directories for a project. Use `dml clone`
when starting from existing remote history rather than initializing over it.

Before syncing or executing remote-backed work, verify the effective
`remote.root` with `dml config show`. It is the shared synchronization,
execution, cache, and artifact endpoint, not a dependency registry.
Configuration can be overridden by environment or explicit options, so inspect
the effective setting rather than assuming the project file is authoritative.

## Inspect Before Changing History

Use `dml status` to establish the current branch, checkout, and revision; use
`dml log`, `dml show`, and `dml diff` to understand the history or DAG snapshot
you intend to change. Resolve ambiguous revision names before checkout,
branch/tag movement, merge, rebase, or revert. Branches and tags name commits;
a detached checkout is not an attached branch for new history.

Check status again after a revision-changing operation. Do not treat a DAG
name at the current tip as an exact historical result; keep the commit or DAG
ref when the distinction matters.

## Coordinate Remote And Dependency State

Decide whether you need to fetch remote history, integrate an upstream branch,
or publish local history. `fetch` updates local tracking state; `pull`
integrates the upstream (fast-forward-only by default); `push` publishes an
attached branch. A remote-tracking revision is a fetched local view, not a live
read of the endpoint. Inspect branch attachment and upstream before publishing.

A shallow clone or fetch has a complete selected snapshot but may lack older
ancestry. Deepen or unshallow before operations that need to compare or prove
history across that boundary.

Use dependencies for importing another project's DAGs, not for execution or
cache coordination. Configure the dependency endpoint and fetch its desired
branch or tag before using its results. Dependencies are import-only; do not
push to them or target them with garbage collection.

## Investigate Cache Identity Before Invalidation

A cache key identifies a computation; invalidation targets an **exact execution
ref**. First inspect the cache description and its associated execution record.
Confirm which execution is selected and whether its result remains reusable;
cleanup state can differ from result state. If invalidation is intentional,
retain and use the returned `index:` or `frozenindex:` execution ref, not the
cache key, DAG ref, bare ID, or a guessed execution.

Invalidation affects other users of the same `remote.root`. Treat unexpected
reuse as a question about the runnable and normalized inputs before refreshing
shared work. Use CLI cache and runtime inspection for the recorded identity and
lifecycle; CLI help supplies the current commands.

## Collect Only Unreachable State

Local and remote GC collect unreachable objects; remote GC uses the configured
`remote.root`, not a dependency. Confirm which refs must remain reachable before
collecting, and do not run GC while synchronization or endpoint execution is
active. Never run it concurrently with fetch, pull, or push. There is no GC dry
run or dependency target.

## Review Shared-State Decisions

Before finishing, check:

- Is this the intended project, revision, branch, and endpoint?
- Was remote-tracking state fetched, and is shallow ancestry sufficient?
- Is a dependency being treated as import-only?
- Was cache reuse explained before considering invalidation?
- Does an invalidation use the exact execution ref?
- Are needed refs retained and synchronization idle before GC?
