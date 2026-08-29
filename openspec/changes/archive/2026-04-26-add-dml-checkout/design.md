## Context

The current git-like surface supports revision resolution for merge/revert and DAG-level checkout, but it does not provide a top-level checkout that switches active repository checkout state to another revision. Users need a direct way to move between branches and point-in-time commits/tags while preserving clear semantics for whether subsequent commits advance a branch.

`IndexOps.commit` advances branch history when a branch head is active. That existing behavior is useful and should remain unchanged: checking out a non-branch should act like detached HEAD in git, with no active HEAD to advance. No runtime behavior changes are introduced in this change.

## Goals / Non-Goals

**Goals:**
- Add `dml checkout <revision>` for commit/tag/branch and other supported revision inputs.
- Resolve revision values locally and classify them as branch-attached vs detached targets.
- Keep branch checkout behavior head-attached so new commits advance the selected branch.
- Keep non-branch checkout behavior detached by clearing active HEAD and relying on existing commit semantics.
- Define clone behavior as `fetch` followed by `checkout` so clone target semantics match checkout semantics.
- Support clone by branch or tag refs, but not by direct commit ref until fetch supports commit-target retrieval.
- Preserve local-only resolution behavior (no implicit network fetches).
- Return clear CLI feedback for attached vs detached mode.

**Non-Goals:**
- Adding implicit fetch during checkout.
- Changing DAG-level checkout command semantics.
- Introducing new merge/rebase behavior as part of this change.

## Decisions

### Checkout target classification
`dml checkout` resolves the revision first, then classifies the resolved reference:
- Branch target -> attached mode (active branch set).
- Any non-branch target (tag, commit ref, ancestry expression result, remote-tracking ref) -> detached mode.

Alternative considered: always create/attach a synthetic branch for non-branch targets. Rejected because it hides detached behavior and can cause accidental branch movement.

### Detached checkout semantics use existing commit behavior
Detached checkout clears active HEAD. Commits created while detached use existing `IndexOps.commit` behavior: commit the index without moving any branch head.

Alternative considered: introduce a new runtime checkout mode flag. Rejected because detached semantics already follow directly from the absence of an active HEAD.

### Local-only revision resolution
Checkout uses existing local revision resolution without implicit remote operations. Unfetched remote URIs fail with a local-resolution error.

Alternative considered: auto-fetch unresolved remote targets during checkout. Rejected to keep checkout deterministic, offline-safe, and consistent with existing revision rules.

### Clone is fetch then checkout
`dml clone` reuses existing primitives by first fetching the requested remote ref, then running checkout against the fetched revision target. This keeps one source of truth for attach/detach semantics and avoids bespoke clone-only state transitions.

For now, clone accepts targets that fetch can materialize as refs (branches and tags). Clone by direct commit is rejected because fetch does not yet support fetching arbitrary commit objects by commit id.

Clone does not invoke `init`; it performs first-time repository initialization directly, then fetches and checks out the requested target. `init` hooks remain scoped to explicit `dml init` invocation and do not run during clone.

Alternative considered: add direct commit fetch as part of this change. Rejected to keep scope focused on checkout semantics and avoid expanding remote transport behavior.

### CLI mode visibility
`dml checkout` output explicitly states whether checkout is attached to a branch or detached, and identifies the resolved target.

Alternative considered: minimal success output only. Rejected because explicit mode reporting avoids user confusion about later commit behavior.

## Risks / Trade-offs

- Detached behavior confusion for users expecting branch movement -> Mitigation: explicit checkout mode messaging and docs examples.
- Ambiguous revision parsing can attach/detach unexpectedly -> Mitigation: deterministic resolution precedence and tests across branch/tag/commit forms.
- Detached commits may be hard to recover if users forget refs -> Mitigation: ensure command output reports resolved commit and recommend branch checkout for durable progression.
- Clone target confusion for revision inputs -> Mitigation: document and enforce that clone currently supports fetched branch/tag refs, and return a specific error for direct commit clone attempts.

## Migration Plan

- No data migration required.
- Add checkout command and HEAD attach/detach handling without changing runtime internals.
- Update CLI/help docs and add tests before release.

## Open Questions

- None for initial implementation.
