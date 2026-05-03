## 1. File-backed pointer infrastructure in HeadOps

- [x] 1.1 Add `.dml/refs` path resolution helpers in `HeadOps` for local heads/tags/indexes and remote-tracking heads/tags.
- [x] 1.1a Add/centralize identifier validation for `owner`, `project`, `branch`, and `tag` as `[A-Za-z0-9\-\*\|_]+`.
- [x] 1.2 Implement pointer file read/write helpers that map `<commit_id>` <-> `Ref("commit:<id>")`.
- [x] 1.3 Add mutation-site file locking and atomic file replacement for create/update/delete pointer operations.
- [x] 1.4 Keep expected-current commit checks in update paths and raise `DmlPointerConflictError` with `current_commit` on stale writes.

## 2. Switch runtime pointer flows to filesystem refs

- [x] 2.1 Update branch/index list/get/create/delete/update methods in `HeadOps` to use file paths only.
- [x] 2.2 Update remote-tracking branch/tag pointer handling in `HeadOps` to use `.dml/refs/remote/<owner>/<project>/{heads,tags}`.
- [x] 2.3 Change `list_pointer_roots` to return commit refs directly.
- [x] 2.4 Ensure `fetch_uri` updates local file-backed remote-tracking refs and `pull_uri_into_branch` remains `fetch + merge`.
- [x] 2.5 Remove `head:<name>` / `index:<id>` string usage from `_cli/*`, `ops/commit.py`, `ops/index.py`, and related call sites; use branch names and opaque index ids instead.

## 3. Remove DB pointer model

- [x] 3.1 Remove `Head` and `Index` classes from `src/daggerml/_internal/types.py`.
- [x] 3.2 Remove any remaining `head`/`index` DB namespace assumptions from `HeadOps` implementation.
- [x] 3.3 Update docs/tests/specs that assert DB pointer namespaces to assert filesystem refs and commit-id payload semantics.
- [x] 3.4 Keep S3 remote protocol behavior unchanged; verify no remote CAS/refs schema changes are introduced.
- [x] 3.5 Update docs/help/error messages to remove `head:<name>` / `index:<id>` examples and wording.
