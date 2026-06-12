## Why

`Index` currently models a mutable working state that behaves like a commit under construction, but the type hierarchy treats it as unrelated to `Commit`. That mismatch complicates runtime staging today and blocks the unborn-HEAD work that wants a cleaner `commit or Commit(...)` creation path.

## What Changes

- Introduce a dedicated internal capability for representing `Index` as a mutable subclass of `Commit`.
- Preserve current external runtime and history behavior in this stage; this is a model refactor, not a workflow change.
- Move `IndexOps.create(...)` to build an index from commit-shaped state instead of a separate head-only payload.

## Capabilities

### New Capabilities
- `mutable-index-commit-model`: Internal contract for treating runtime indexes as mutable commits with an added in-progress DAG pointer.

### Modified Capabilities

## Impact

- Affected code: `src/daggerml/_core/types.py`, `src/daggerml/_core/index.py`, runtime/index tests.
- Affected systems: runtime staging, index serialization, commit/index boundary.
- No intended public API or CLI behavior change in this stage.
