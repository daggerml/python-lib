## Context

The current repository bootstrap path always creates an empty commit and a materialized branch ref. The desired model is closer to git: attached HEAD may point at a branch name before that branch has any commit. This depends on the prior `Index <: Commit` refactor so runtime staging can begin from explicit empty commit-shaped state.

## Goals / Non-Goals

**Goals:**
- Support attached unborn branches with no on-disk branch ref yet.
- Let the first meaningful commit become the first branch commit.
- Keep detached HEAD semantics commit-backed only.
- Allow merge-like workflows to treat `None` as an unresolved-but-valid commit selector where appropriate.

**Non-Goals:**
- Adding clone in this change.
- Changing how missing named branches behave outside the current unborn HEAD.
- Redesigning remote tracking formats.

## Decisions

- Attached HEAD stays encoded as `ref: refs/local/heads/<branch>` even when that branch ref file is absent.
  Rationale: the unborn state is represented by branch-ref absence, not by a third HEAD payload format.
- Detached init is rejected.
  Rationale: detached state implies an actual commit.
- `resolve_rev(...)` may return `None` for a syntactically valid revision that does not currently resolve to a commit.
  Rationale: unborn `HEAD` and ancestry beyond the root share that shape cleanly.
- `CommitOps.merge(None, commit)` will act as a fast-forward from nothing.
  Rationale: that lets first-commit and unborn-merge flows reuse the normal merge seam.
- `branch.create("foo")` on an unborn attached repo repoints HEAD and does not materialize a branch ref file.
  Rationale: there is still no commit to persist.

## Risks / Trade-offs

- [Many callers assume HEAD always resolves to a commit] -> Change `Head` semantics first and audit all `head_info["commit"]` call sites.
- [Status and branch UX can drift from git expectations] -> Preserve attached-branch reporting in `status()` while keeping unborn branches out of `branch.list()`.
- [Overusing `None` can blur error handling] -> Reserve `None` for valid selectors with no resolved commit; missing named refs still fail.
