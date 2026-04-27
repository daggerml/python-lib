## Why

Users can already check out a single DAG from another revision, but there is no top-level `dml checkout` for moving repository state to a specific revision target. Adding `dml checkout` now closes a core git-like workflow gap and makes branch-vs-detached HEAD behavior explicit.

## What Changes

- Add a new `dml checkout <revision>` command that accepts commits, tags, branches, and other supported revision expressions.
- Extend revision resolution to infer the intended target kind (branch, tag, commit, remote-tracking ref, ancestry expression) and resolve it locally.
- Define `dml clone` as composition of `fetch` then `checkout`, using the fetched target as checkout input.
- Allow clone-by-tag when the tag can be fetched as a ref target, and reject clone-by-commit for now because fetch does not yet support direct commit fetch.
- Define checkout behavior differences:
  - Branch checkout sets active HEAD to that branch so later commits advance branch history.
  - Non-branch checkout (commit, tag, detached revision) clears active HEAD, so `IndexOps.commit` keeps its current behavior (commit index only, no branch advancement).
- Keep checkout local-only: no implicit network fetches for unresolved remote URIs.
- Surface clear CLI feedback showing whether checkout is branch-attached or detached.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `git-like-commit-ops`: Extend revision resolution and checkout semantics from DAG-level checkout to top-level repository checkout.

## Impact

- CLI surface for new `dml checkout` command and status messaging.
- Head/index operations that decide whether commits advance branch pointers (branch checkout) or remain detached (no HEAD).
- Commit-ish parser/resolver behavior for branch/tag/commit inference.
- Clone flow behavior for branch/tag targets via `fetch -> checkout` composition and explicit non-support for commit-target clone.
- Tests for checkout target resolution, detached-mode behavior, and branch re-attachment.
