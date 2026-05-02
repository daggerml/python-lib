## 1. HeadOps pointer boundary

- [x] 1.1 Add the new `HeadOps` public API for branch and index listing, creation, deletion, commit lookup, and commit updates with optional `txn` support.
- [x] 1.2 Add private txn-required helpers behind the public `HeadOps` methods.
- [x] 1.3 Add a dedicated stale-update `DmlRepoError` subclass with a `current_commit` attribute.
- [x] 1.4 Move internal head/index ref generation fully inside `HeadOps`.

## 2. Internal caller migration

- [x] 2.1 Refactor `CommitOps` branch-targeted workflows to use `HeadOps` branch methods instead of direct head storage access.
- [x] 2.2 Refactor `IndexOps` to use opaque index ids plus `HeadOps` commit lookup and `update_index_commit` flows.
- [x] 2.3 Refactor `RemoteOps` tracking-head writes to use `HeadOps` public methods.
- [x] 2.4 Remove or narrow shared helpers such as `BaseOps.get_ctx` that expose `Head` or `Index` objects outside `HeadOps`.

## 3. Surface and test updates

- [x] 3.1 Update API and CLI code to stop carrying head/index refs and use branch names, opaque index ids, and commit refs instead.
- [x] 3.2 Update contract and integration tests for the new `HeadOps` pointer boundary and stale-update conflict behavior.
- [x] 3.3 Run the relevant head, index, commit, remote, API, and CLI test coverage and fix any regressions.
