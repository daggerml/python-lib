## Why

DaggerML's git-like surface has drifted between code, tests, and specs, especially around revision syntax, detached commit behavior, status payloads, and missing branch/tag lifecycle commands. We need one coherent ref model that matches current DML concepts, intentionally breaks incompatible legacy expectations, and exposes the full lifecycle of local branches, local tags, and fetched remote refs.

## What Changes

- **BREAKING**: Simplify and harmonize revision and remote syntax around local branch names, explicit `@tag` tag selectors, and `dml://owner/project#branch` or `@tag` remote selectors. Remove named-remote shorthand such as `origin/main` from the maintained model.
- **BREAKING**: Treat detached commit behavior, status payload shape, and show/diff payload shape according to current DML semantics rather than preserving older spec language.
- Add explicit branch lifecycle commands for local mutable refs: list, create, move, rename, and delete.
- Add explicit tag lifecycle commands for local immutable refs: list, create, and delete. Tag mutation remains delete-then-create rather than in-place movement.
- Add remote ref deletion through `dml push --delete <revision>`, where revision parsing determines which remote branch or tag is deleted.
- Standardize the tracking model: fetched remote refs are addressed by `dml://...`, local branches track same-named remote branches under the configured project, and `checkout dml://...` remains detached.
- Make the proposal explicitly non-backward-compatible: no migration shims, no named-remote affordances, and no redundant pre-validation beyond what is required at actual use sites and already enforced by the DB.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `git-like-commit-ops`: Replace the drifted git-op contract set with a coherent DML ref lifecycle, command matrix, detached-head semantics, and remote deletion model.
- `remote-project-refs`: Remove named-remote assumptions, standardize fetched remote refs around `dml://...`, and define push/delete/fetch/pull behavior for same-name tracking.
- `revision-parsing-contract-matrix`: Align accepted revision forms with the simplified selector model, including explicit `@tag` selectors and the removal of `origin/...` grammar.
- `unified-dml-surface`: Update the shared `Dml` caller-facing command surface to expose `branch` and `tag` namespaces rather than the older top-level branch expectation.

## Impact

- Affected code: `src/daggerml/_core/dml.py`, `head.py`, `commit.py`, remote-sync code, and generated CLI surfaces.
- Affected tests: revision parsing, `_core` git-op contracts, remote sync integration, and any tests still assuming named-remotes, old detached-head behavior, or old status/show payloads.
- Affected specifications: the four capabilities listed above receive breaking contract updates.
- Dependencies: no new dependency is expected.
- Compatibility: backward compatibility is intentionally not preserved; older selector forms, command shapes, and behavior mismatches are removed rather than supported in parallel.
