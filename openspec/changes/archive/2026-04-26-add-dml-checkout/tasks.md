## 1. CLI Checkout Surface

- [x] 1.1 Add `dml checkout <revision>` command wiring in the CLI command tree.
- [x] 1.2 Add CLI output/messages that explicitly report branch-attached vs detached scratch mode after checkout.
- [x] 1.3 Add CLI help text and usage examples for branch, tag, and commit-expression checkout.

## 2. Clone Composition and Target Rules

- [x] 2.1 Refactor/implement `dml clone` as `fetch` followed by `checkout`.
- [x] 2.2 Ensure clone target parsing accepts fetchable branch/tag refs and routes to checkout with consistent attach/detach semantics.
- [x] 2.3 Add explicit clone error for direct commit targets that fetch cannot retrieve yet.
- [x] 2.4 Ensure clone performs first-time repo initialization directly and does not invoke `init` hooks.

## 3. Revision Resolution and Classification

- [x] 3.1 Extend revision resolution to infer branch/tag/commit-like targets for checkout routing.
- [x] 3.2 Add local tag shorthand resolution coverage for checkout and keep branch/URI/`HEAD~N` behavior consistent.
- [x] 3.3 Ensure checkout resolution remains local-only and returns a clear error for unfetched remote URIs.

## 4. Runtime Checkout Mode

- [x] 4.1 Implement branch checkout flow that sets active HEAD to the selected branch.
- [x] 4.2 Implement non-branch checkout flow that clears active HEAD (detached).
- [x] 4.3 Keep runtime internals unchanged and rely on existing detached commit behavior.

## 5. Commit Progression Semantics

- [x] 5.1 Update commit/index operations so commits in detached mode do not advance shared branch heads.
- [x] 5.2 Preserve existing `IndexOps.commit` behavior without semantic changes.
- [x] 5.3 Validate transitions between detached and attached checkout states across consecutive checkout/commit operations.

## 6. Tests and Documentation

- [x] 6.1 Add tests for checkout resolution across branch, tag, commit ref, and `HEAD~N` expressions.
- [x] 6.2 Add tests verifying detached checkout commits index state without branch-head movement.
- [x] 6.3 Add tests verifying branch re-attachment resumes branch-head progression.
- [x] 6.4 Add tests for clone branch and clone tag using `fetch -> checkout` composition.
- [x] 6.5 Add tests verifying clone rejects direct commit targets until fetch supports commit retrieval.
- [x] 6.6 Add tests verifying clone does not invoke `init` or run init hooks.
- [x] 6.7 Update relevant docs for `dml checkout`, clone target semantics, and local-only checkout resolution behavior.
