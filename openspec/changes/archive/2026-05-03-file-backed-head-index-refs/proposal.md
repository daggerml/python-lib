## Why

Branch and index pointers are currently persisted in LMDB namespaces (`head`, `index`) as typed objects (`Head`, `Index`). This keeps pointer identity coupled to DB object modeling when the desired model is filesystem refs under project-local `.dml/refs`. Moving pointers to files simplifies pointer inspection and keeps branch/index mutation rules in one file-backed ref layer.

This change is intentionally breaking and does not provide backward compatibility with prior pointer storage.

## What Changes

- Replace local branch/tag/index pointer persistence with filesystem refs under `.dml/refs/local`, with `HeadOps` as the owner for branch/tag/index pointer management.
- Add local remote-tracking pointer persistence under `.dml/refs/remote/<owner>/<project>`.
- Store pointer file contents as raw commit IDs (no `commit:` prefix).
- Update `HeadOps` pointer read/write/list/update logic to be file-backed with mutation-site file locking and atomic write/replace.
- Change `list_pointer_roots` to return commit refs directly (`Ref("commit:<id>")`) rather than pointer refs.
- Remove all `head:<name>` / `index:<id>` pointer-string usage across the codebase (including `_cli/*`, `ops/commit.py`, `ops/index.py`, and related tests/docs); branch and index are addressed by branch names and opaque index ids only.
- Keep S3 remote CAS/refs protocol and layout unchanged.
- Treat `dml://<owner>/<project>[#branch|@tag]` as I/O boundary format only (user input / CLI output), while local tracking storage uses `.dml/refs/remote/...` paths.
- Constrain `owner`, `project`, `branch`, and `tag` identifiers to alphanumeric characters plus `-`, `*`, `|`, and `_` (`[A-Za-z0-9\-\*\|_]+`) so `<name>` maps unambiguously to a single path segment.
- Keep internal `pull_uri_into_branch` semantics as `fetch_uri` followed by merge into the target local branch.
- Keep GC root behavior as traversal from current refs; user-provided root selection is not part of this change.
- Remove `Head` and `Index` types from internal type contracts and namespace registration.
- Remove `head` and `index` DB namespace usage entirely after file-backed flow is in place.

## Proposed File Tree

```text
<project_home>/
  .dml/
    refs/
      local/
        heads/
          <name>            # file contents: <64-hex-commit-id>
        tags/
          <name>            # file contents: <64-hex-commit-id>
        indexes/
          <id>              # file contents: <64-hex-commit-id>
      remote/
        <owner>/
          <project>/
            heads/
              <name>        # file contents: <64-hex-commit-id>
            tags/
              <name>        # file contents: <64-hex-commit-id>
```

## Capabilities

### Modified Capabilities
- `headops-pointer-management`: pointer storage backend changes from DB objects to filesystem refs; roots become commit refs.

### New Capabilities
- `file-backed-pointer-refs`: project-local ref directory structure, file content format, and locking/update semantics for local and remote-tracking refs.

## Impact

- Affected code: `src/daggerml/_internal/ops/head.py`, `src/daggerml/_internal/types.py` (primary), plus all call sites/tests that assert DB `head`/`index` namespace behavior.
- Affected behavior: pointer persistence moves outside LMDB transaction atomicity; stale-update safety is preserved via expected-current commit checks plus lock-scoped atomic file replacement at mutation sites.
- Compatibility: no migration or compatibility layer; old DB pointer storage is removed in this release.
