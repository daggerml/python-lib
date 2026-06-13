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

- Match: `src/daggerml/_cli.py`
- Read:
  - `docs/reference/cli.md`
  - `docs/architecture/internal-modules.md`
- Also read:
  - `docs/reference/errors.md`

### Core repository operations

- Match: `src/daggerml/_core/commit.py`, `src/daggerml/_core/dag.py`, `src/daggerml/_core/head.py`, `src/daggerml/_core/index.py`
- Read:
  - `docs/architecture/ops-layer.md`
  - `docs/architecture/internal-modules.md`
  - `docs/concepts/codecs-and-values.md` (when touching codec serialization/import behavior)
- Also read:
  - the matching source module under `src/daggerml/_core/`
  - example: editing `src/daggerml/_core/commit.py` -> read `docs/concepts/commits-and-history.md` and `docs/architecture/ops-layer.md`

### Core runtime orchestration

- Match: `src/daggerml/_core/dml.py`, `src/daggerml/_core/config.py`, `src/daggerml/_core/revision.py`, `src/daggerml/_core/uri.py`
- Read:
  - `docs/reference/configuration.md`
  - `docs/concepts/execution.md`
  - `docs/architecture/system-overview.md`
  - `docs/reference/errors.md`

### Core types and contracts

- Match: `src/daggerml/_core/types.py`, `src/daggerml/_core/builtins.py`
- Read:
  - `docs/concepts/refs-and-namespaces.md`
  - `docs/architecture/type-system.md`
  - `docs/reference/errors.md`

### Core JSON/string serde helpers

- Match: `src/daggerml/_core/serde.py`
- Read:
  - `docs/concepts/refs-and-namespaces.md`
  - `docs/concepts/codecs-and-values.md`
  - `docs/architecture/type-system.md`
  - `docs/reference/errors.md`

### Core storage / DB integration

- Match: `src/daggerml/_core/db.pyx`, `src/daggerml/_core/types.py`, `src/daggerml/_core/util.py`
- Read:
  - `docs/concepts/storage.md`
  - `docs/architecture/storage-internals.md`
  - `docs/guides/store-and-load-external-data.md`

### Runtime / execution flow

- Match: `src/daggerml/_core/dml.py`, `src/daggerml/_core/index.py`, `src/daggerml/_core/exec_state.py`, `src/daggerml/util.py`
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

- Match: `src/daggerml/_core/remote.py`, `src/daggerml/_core/exec_state.py`, `src/daggerml/_core/s3_cas.py`, remote-related CLI/config code
- Read:
  - `docs/architecture/remote-protocol.md`
  - `docs/concepts/remotes.md`
  - `docs/architecture/ops-layer.md`

### Commit / DAG / head / index behavior

- Match: `src/daggerml/_core/commit.py`, `src/daggerml/_core/dag.py`, `src/daggerml/_core/head.py`, `src/daggerml/_core/index.py`, related CLI code
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

### Examples and CI helpers

- Match: `examples/**`, `.github/workflows/**`
- Read:
  - `README.md`
  - `CONTRIBUTING.md`
  - `docs/architecture/system-overview.md`

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
