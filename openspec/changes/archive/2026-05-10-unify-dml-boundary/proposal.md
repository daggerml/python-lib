## Why

The branch already contains part of the unification work under `src/daggerml/_internal/`: `_internal.__init__` now expects a shared `Dml` export, `_internal.dml_context` centralizes config-derived runtime/project helpers, and some internal modules have started importing through the `_internal` export surface.

The change drifted because the core `_internal.dml.Dml` facade and `_internal.dml_resolution` helpers were never added, while the change artifacts were still marked complete. We need to realign the change with what is actually present and finish the missing boundary work with a small `_internal`-only patch instead of pretending the full API/CLI/contrib migration already landed.

## What Changes

- Complete the missing shared `_internal.Dml` boundary and `_internal.dml_resolution` module that the current `_internal` export surface already references.
- Keep `_internal.dml_context` as the config/context owner and make the shared `Dml` delegate through it plus the existing ops classes.
- Finish the remaining work in a few files under `src/daggerml/_internal/` instead of expanding the blast radius across `api`, `cli`, and `contrib` in this change.
- Preserve existing compatibility surfaces such as `daggerml.api.Dml` and `DmlOps` for now; broader removal/cleanup can happen in a follow-up once the shared internal boundary is real.

## Capabilities

### New Capabilities
- `unified-dml-surface`: One caller-facing `Dml` contract shared by API wrappers and CLI handlers, including the fixed top-level methods plus `dag`, `admin`, `runtime`, `config`, and `ops` namespaces.

### Modified Capabilities
- `git-like-commit-ops`: Finish the shared `Dml` entrypoint that owns project-workflow orchestration while reusing the already-landed ops/context groundwork.
- `dmlops-init-recovery`: Preserve init/recovery behavior by putting the bootstrap entrypoint on the new shared internal `Dml` class.

## Impact

- Affects only a small `_internal` slice: `src/daggerml/_internal/__init__.py`, `src/daggerml/_internal/dml_context.py`, the new `src/daggerml/_internal/dml.py`, the new `src/daggerml/_internal/dml_resolution.py`, and any minimal adjacent wiring needed inside `src/daggerml/_internal/`.
- Changes the internal layering contract by making the shared `_internal.Dml` boundary real instead of just referenced by exports and callers.
- Leaves API, CLI, contrib, and broad compatibility cleanup to follow-up work once the internal boundary exists and is importable.
- Keeps on-disk repository formats, revision grammar, and existing caller contracts unchanged in this phase.
