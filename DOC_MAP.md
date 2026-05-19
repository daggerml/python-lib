# Edit Doc Map

Audience: coding agents and maintainers working on this repository.

Use this file to identify which project docs to read before editing a code path, then list the docs you consulted in your summary or PR notes.

## Global Docs (Always Read)

- `docs/README.md`: docs index and layout conventions.
- `docs/architecture/system-overview.md`: system-level layering and subsystem boundaries.

## Path Rules

### Public Python API

- Match: `src/daggerml/api.py`, `src/daggerml/__init__.py`
- Read:
  - `docs/reference/python-api.md`
  - `docs/concepts/dags-and-nodes.md`
  - `docs/concepts/refs-and-namespaces.md`
  - `docs/reference/errors.md`
  - `docs/concepts/codecs-and-values.md`
  - `docs/architecture/type-system.md`

### Codec module

- Match: `src/daggerml/codecs.py`
- Read:
  - `docs/concepts/codecs-and-values.md`
  - `docs/reference/python-api.md`
  - `docs/contrib/reference/s3-and-codecs.md`
  - `docs/reference/errors.md`

### CLI surface

- Match: `src/daggerml/_cli/**`
- Read:
  - `docs/reference/cli.md`
  - `docs/architecture/internal-modules.md`
- Also read:
  - `docs/reference/errors.md`

### Internal operations

- Match: `src/daggerml/_internal/ops/**`
- Read:
  - `docs/architecture/ops-layer.md`
  - `docs/architecture/internal-modules.md`
  - `docs/concepts/codecs-and-values.md` (when touching codec serialization/import behavior)
- Also read:
  - the matching source module under `src/daggerml/_internal/ops/` for the file being changed
  - example: editing `src/daggerml/_internal/ops/commit.py` -> read `docs/concepts/commits-and-history.md` and `docs/architecture/ops-layer.md`

### Internal codec registry

- Match: `src/daggerml/_internal/codec.py`
- Read:
  - `docs/concepts/codecs-and-values.md`
  - `docs/architecture/ops-layer.md`

### Internal types and contracts

- Match: `src/daggerml/_internal/types.py`, `src/daggerml/_internal/builtins.py`
- Read:
  - `docs/concepts/refs-and-namespaces.md`
  - `docs/architecture/type-system.md`
  - `docs/reference/errors.md`

### Internal storage / DB integration

- Match: `src/daggerml/_internal/_db.pyx`, `src/daggerml/_internal/util.py`
- Read:
  - `docs/concepts/storage.md`
  - `docs/architecture/storage-internals.md`
  - `docs/guides/store-and-load-external-data.md`

### Runtime / execution flow

- Match: `src/daggerml/_config.py`, `src/daggerml/util.py`, execution-related internals
- Read:
  - `docs/reference/configuration.md`
  - `docs/concepts/execution.md`
  - `docs/architecture/remote-protocol.md`
  - `docs/reference/errors.md`

### C implementation and headers

- Match: `c/src/**`, `c/include/**`
- Read:
  - `c/README.md`
  - `docs/concepts/storage.md`
  - `docs/concepts/dags-and-nodes.md`
- Also read:
  - `docs/architecture/storage-internals.md` when touching DB/reference behavior

### Remote and sync behavior

- Match: files related to remote/sync (for example `*_remote*`, remote ops/CLI)
- Read:
  - `docs/architecture/remote-protocol.md`
  - `docs/concepts/remotes.md`
  - `docs/architecture/ops-layer.md`

### Commit / DAG / head / index behavior

- Match: files related to commit, dag, head, index (ops or CLI)
- Read:
  - `docs/concepts/commits-and-history.md`
  - `docs/concepts/dags-and-nodes.md`
  - `docs/architecture/ops-layer.md`

### Contrib modules

- Match: `src/daggerml/contrib/**`
- Read:
  - `docs/contrib/README.md`
  - `docs/contrib/concepts/runtime.md`
  - matching contrib doc(s):
    - `docs/contrib/reference/python-api.md`
    - `docs/contrib/reference/s3-and-codecs.md`
    - `docs/contrib/reference/runtime-surfaces.md`
    - `docs/contrib/architecture/execution-flow.md`
    - `docs/contrib/architecture/supervisor-and-state.md`

### Tests

- Match: `tests/**`
- Read:
  - docs corresponding to the code under test (same topic rules below)
- Also read:
  - `docs/architecture/internal-modules.md` for internal test areas
  - `CONTRIBUTING.md` for test layout and marker policy

### Packaging / build / project config

- Match: `pyproject.toml`, `uv.lock`, `CMakeLists.txt`
- Read:
  - `README.md`
  - `CONTRIBUTING.md`
  - `c/README.md` (for C build changes)

### Documentation edits

- Match: `docs/**`
- Read:
  - `docs/README.md`

## Topic Rules (Apply In Addition To Path Rules)

- If changing adapter behavior:
  - `docs/concepts/execution.md`
  - `docs/architecture/remote-protocol.md`
  - `docs/contrib/concepts/runtime.md`
  - `docs/contrib/reference/runtime-surfaces.md`
  - `docs/contrib/architecture/supervisor-and-state.md`
- If changing data/object representation:
  - `docs/concepts/dags-and-nodes.md`
  - `docs/concepts/refs-and-namespaces.md`
  - `docs/architecture/type-system.md`
- If changing codec behavior or literal write normalization:
  - `docs/concepts/codecs-and-values.md`
- If changing storage, references, GC, or artifacts:
  - `docs/concepts/storage.md`
  - `docs/architecture/storage-internals.md`
  - `docs/guides/store-and-load-external-data.md`
- If changing user-facing errors:
  - `docs/reference/errors.md`

## Ambiguity Rule

If no rule clearly matches:

- Read `docs/architecture/system-overview.md` and `docs/README.md`.
- Add or refine a mapping in this file in the same change.

## Maintenance

When adding a new top-level code area or major module, add or update a mapping here in the same PR.
