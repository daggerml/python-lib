## 1. Core Ref Model And Selector Cleanup

- [x] 1.1 Remove named-remote selector expectations from revision parsing and git-op workflows, keeping canonical `dml://...` selectors and explicit `@tag` tag selectors.
- [x] 1.2 Align detached commit, `status`, and `show`/`diff` behavior with the actual DML model and update tests to match the new authoritative contracts.
- [x] 1.3 Audit existing git-op tests for old `origin/...`, bare-tag, or stale detached-head assumptions and replace them rather than preserving compatibility paths.

## 2. Branch And Tag Lifecycle Commands

- [x] 2.1 Add `dml.branch` namespace methods for list, create, move, rename, and delete using resolved commit selectors and existing lower-level ref storage.
- [x] 2.2 Add `dml.tag` namespace methods for list, create, and delete, with tag mutation expressed as delete-then-create rather than in-place movement.
- [x] 2.3 Ensure attached `HEAD` follows branch rename, current branch deletion is handled coherently, and `branch create <name> <dml://...>` works from fetched remote commits.

## 3. Remote Ref Workflows

- [x] 3.1 Harmonize `push`, `fetch`, and `pull` around same-name branch tracking under configured `remote.project`.
- [x] 3.2 Add `dml push --delete <revision>` so remote branch and tag deletion reuse normal revision parsing.
- [x] 3.3 Decide whether explicit `push dml://owner/project#branch` and `push dml://owner/project@tag` are kept only if they simplify implementation; implement that chosen rule consistently.

## 4. Surface And CLI Alignment

- [x] 4.1 Update the shared `Dml` surface and generated CLI structure to expose `branch` and `tag` namespaces instead of the older top-level branch expectation.
- [x] 4.2 Keep validation only where operations actually require it, relying on existing DB and ref-path validation rather than layering duplicate checks through every boundary.

## 5. Verification

- [x] 5.1 Add or update `_core` contract and integration coverage for branch lifecycle, tag lifecycle, revision parsing, detached commits, remote delete, and same-name tracking behavior.
- [x] 5.2 Run targeted git-op tests, then the required project validation sequence, and fix any behavioral gaps exposed by the new model.
