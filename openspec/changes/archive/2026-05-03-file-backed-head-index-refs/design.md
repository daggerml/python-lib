## Context

`HeadOps` currently persists branch and index pointers as DB objects (`Head`, `Index`) under LMDB namespaces and relies on DB transactions for storage-level atomicity. The target model replaces these pointers with filesystem refs under `.dml/refs` in the project directory.

The design keeps stale-update correctness via expected-current commit checks and introduces file locks only around mutation sites. The release is intentionally breaking and excludes backward compatibility.

Remote S3 protocol behavior remains unchanged. This change affects only local pointer/tracking storage representation.

## Goals / Non-Goals

**Goals:**
- Persist local heads/tags/indexes as files under `.dml/refs/local/{heads,tags,indexes}`.
- Persist remote-tracking refs as files under `.dml/refs/remote/<owner>/<project>/{heads,tags}`.
- Manage local and remote-tracking branch/tag/index pointers through `HeadOps`.
- Store only commit IDs in pointer files.
- Keep stale-write detection through compare-and-swap style update methods in `HeadOps`.
- Use lock-scoped atomic file replacement for pointer mutations.
- Return commit refs directly from `list_pointer_roots`.
- Keep `pull_uri_into_branch` as `fetch_uri` + merge behavior.
- Keep GC traversal rooted in current refs (no root-argument redesign in this change).
- Remove `Head`/`Index` types and DB pointer namespaces.

**Non-Goals:**
- Backward compatibility with prior DB pointer storage.
- Hybrid read/write behavior across both DB and filesystem pointer backends.
- Broader redesign of commit, DAG, or remote CAS semantics.
- Changes to S3 remote CAS/refs path schema, payload schema, push/fetch protocol, or remote GC behavior.

## Decisions

### Pointer storage layout is file-backed
`HeadOps` reads/writes pointer files in the project `.dml/refs` tree instead of storing pointer objects in LMDB.

### URI strings are I/O shape, not local storage identity
`dml://<owner>/<project>[#branch|@tag]` remains the user-facing parse/render format. Local remote-tracking pointers are persisted as filesystem paths under `.dml/refs/remote/<owner>/<project>/{heads,tags}`.

### Identifier character set is constrained for unambiguous path mapping
`owner`, `project`, `branch`, and `tag` identifiers are constrained to alphanumeric characters plus `-`, `*`, `|`, and `_` (`[A-Za-z0-9\-\*\|_]+`). This guarantees `<name>` remains a single path segment with no escaping or slash handling.

### Pointer payload format is commit-id only
Each pointer file stores the raw commit ID string (e.g. 64-char lowercase hex). `HeadOps` converts this into `Ref("commit:<id>")` at API boundaries.

### No `head:` / `index:` pointer-string surface
Callers and user-facing layers do not pass or expose `head:<name>` or `index:<id>` strings. Branch targeting uses plain branch names, index targeting uses opaque index ids, and commit targeting uses `commit:<id>` refs where needed.

### Lock only mutation sites
File locking is used around create/update/delete mutation paths. Read-only operations remain lock-free unless they are part of a mutation critical section.

### Atomic updates use expected-current checks
`update_branch_commit` and `update_index_commit` continue to require an `old_commit` and reject stale writes with `DmlPointerConflictError(current_commit=...)`.

### `list_pointer_roots` returns commit refs
GC/root traversal consumes commit refs directly, not pointer refs.

### No migration/back-compat
Implementation proceeds as: implement file paths -> switch all pointer operations to file paths -> remove DB pointer paths/types.

### Remote protocol remains unchanged
All S3 remote object/ref formats and remote operations remain as-is. Only local tracking ref persistence changes.

## Proposed File Tree

```text
<project_home>/
  .dml/
    refs/
      local/
        heads/
          <name>
        tags/
          <name>
        indexes/
          <id>
      remote/
        <owner>/
          <project>/
            heads/
              <name>
            tags/
              <name>
```

## File Semantics

- File content: `<commit_id>` (single commit id string, no `commit:` prefix).
- Missing pointer file: treated as missing pointer (`DmlRepoError` at `HeadOps` boundary).
- Commit existence: validated against LMDB commit namespace before create/update acceptance.
- Mutation writes: write temp file + atomic replace in same directory while holding lock.

## Pull/Fetch/GC Semantics

- `fetch_uri` materializes commit state and creates/updates matching local remote-tracking refs under `.dml/refs/remote/...`.
- `pull_uri_into_branch` remains `fetch_uri(uri)` followed by merge into the selected local branch.
- GC roots are derived from current refs; root-argument surface changes are outside this proposal.

## Risks / Trade-offs

- Pointer updates are no longer inside LMDB transaction boundaries.
- Lock discipline must be correct to avoid torn concurrent mutations.
- Tests that assert `head`/`index` namespace iteration will need replacement with filesystem ref assertions.

## Open Questions

- None at proposal time; path layout and payload format are fixed for this change.
