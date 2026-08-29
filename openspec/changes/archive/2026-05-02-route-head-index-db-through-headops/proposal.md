## Why

Head and index persistence is currently spread across `HeadOps`, `CommitOps`, `IndexOps`, `RemoteOps`, and shared helpers. That leaks `Head` and `Index` storage details across the internal ops layer, makes pointer updates inconsistent, and leaves retryable stale-write handling undefined.

## What Changes

- Route all branch and index database reads, writes, creation, and deletion through `HeadOps` public methods.
- Stop exposing `Head` and `Index` objects, or refs to them, outside `HeadOps`.
- Add atomic branch/index commit update methods that require the caller to provide the expected current commit.
- Add a dedicated conflict error for stale branch/index updates with a `current_commit` attribute so callers can retry.
- Update internal callers to use branch names, opaque index ids, and commit refs instead of head/index refs.

## Capabilities

### New Capabilities
- `headops-pointer-management`: Internal branch/index pointer lifecycle and atomic commit update behavior owned by `HeadOps`.

### Modified Capabilities
- `git-like-commit-ops`: Branch-targeted commit workflows must advance branches through `HeadOps` instead of direct head storage access.

## Impact

- Affected code: `src/daggerml/_internal/ops/head.py`, `commit.py`, `index.py`, `remote.py`, `base_ops.py`, `__init__.py`, and user-facing API/CLI code that currently carries head/index refs.
- Affected tests: internal ops contract tests, integration tests for head/index flows, and any API/CLI tests that assume head/index refs are exposed.
- Affected internal API: branch and index callers will move to branch names, opaque index ids, commit refs, and `HeadOps` conflict-aware update methods.
