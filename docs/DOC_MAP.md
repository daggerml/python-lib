# DOC_MAP

## Status

specified

## Authority

This document is authoritative for mapping repository paths to required pre-read documentation.
If guidance elsewhere conflicts on what docs to consult before edits, this document is the source of truth.


## Purpose

Map code paths to the docs that must be read before editing.


## How To Use

1. Identify the files you plan to edit.
2. Match each file path against the rules below (first match wins unless marked "also read").
3. Read linked docs before making changes.
4. In your summary or PR notes, list docs consulted.


## Global Docs (Always Read)

- `docs/README.md` - docs index and layout conventions.
- `docs/system.md` - system-level layering and subsystem boundaries.


## Path Rules

### Public Python API

- Match: `src/daggerml/api.py`, `src/daggerml/__init__.py`
- Read:
  - `docs/api.md`
  - `docs/object-model.md`
  - `docs/internal/namespace.md`
  - `docs/errors.md`
  - `docs/codec-system.md`
  - `docs/default-dml-runtime.md`

### Codec module

- Match: `src/daggerml/codecs.py`
- Read:
  - `docs/codec-system.md`
  - `docs/api.md`
  - `docs/contrib/api.md`
  - `docs/errors.md`

### CLI surface

- Match: `src/daggerml/_cli/**`
- Read:
  - `docs/cli.md`
  - `docs/internal/README.md`
- Also read:
  - `docs/errors.md`

### Internal operations

- Match: `src/daggerml/_internal/ops/**`
- Read:
  - `docs/internal/ops/README.md`
  - `docs/internal/ops/base-ops.md`
  - `docs/codec-system.md` (when touching codec serialization/import behavior)
- Also read:
  - operation-specific doc under `docs/internal/ops/` for the file being changed
  - example: editing `src/daggerml/_internal/ops/commit.py` -> read `docs/internal/ops/commit-ops.md`

### Internal codec registry

- Match: `src/daggerml/_internal/codec.py`
- Read:
  - `docs/codec-system.md`
  - `docs/internal/ops/index-ops.md`

### Internal types and contracts

- Match: `src/daggerml/_internal/types.py`, `src/daggerml/_internal/builtins.py`
- Read:
  - `docs/internal/type-system-contracts.md`
  - `docs/object-model.md`
  - `docs/errors.md`

### Internal storage / DB integration

- Match: `src/daggerml/_internal/_db.pyx`, `src/daggerml/_internal/util.py`
- Read:
  - `docs/internal/storage-and-refs.md`
  - `docs/internal/storage.md`
  - `docs/storing-and-retrieving-external-data.md`

### Runtime / execution flow

- Match: `src/daggerml/_config.py`, `src/daggerml/util.py`, execution-related internals
- Read:
  - `docs/configuration.md`
  - `docs/execution-model.md`
  - `docs/adapter-execution-contract.md`
  - `docs/errors.md`

### C implementation and headers

- Match: `c/src/**`, `c/include/**`
- Read:
  - `c/README.md`
  - `docs/internal/storage.md`
  - `docs/object-model.md`
- Also read:
  - `docs/internal/storage-and-refs.md` when touching DB/reference behavior

### Remote and sync behavior

- Match: files related to remote/sync (e.g. `*_remote*`, remote ops/CLI)
- Read:
  - `docs/remote-data-model.md`
  - `docs/remote-protocol.md`
  - `docs/remote-sync.md`
  - `docs/internal/ops/remote-ops.md`

### Commit / DAG / head / index behavior

- Match: files related to commit, dag, head, index (ops or CLI)
- Read:
  - `docs/commit-model.md`
  - `docs/dag-model.md`
  - matching ops docs:
    - `docs/internal/ops/commit-ops.md`
    - `docs/internal/ops/dag-ops.md`
    - `docs/internal/ops/head-ops.md`
    - `docs/internal/ops/index-ops.md`

### Contrib modules

- Match: `src/daggerml/contrib/**`
- Read:
  - `docs/contrib/overview.md`
  - `docs/contrib/README.md`
  - `docs/contrib/runtime-contract.md`
  - matching contrib doc(s):
     - `docs/contrib/api.md`
     - `docs/contrib/codecs.md`
     - `docs/contrib/funks.md`
     - `docs/contrib/testing.md`
    - `docs/contrib/registries.md` (focused adapter/executor registry reference)
    - `docs/contrib/executor-state.md` (focused reference)
    - `docs/contrib/executor-catalog.md` (focused per-executor runtime behavior)
    - `docs/contrib/status.md` (focused status/introspection contract)
    - `docs/contrib/runtime-contract.md` (canonical built-in adapter/executor catalog + runtime contracts)
    - `docs/contrib/s3-store.md`

### Tests

- Match: `tests/**`
- Read:
  - docs corresponding to the code under test (same topic rules above)
- Also read:
  - `docs/internal/README.md` for internal test areas

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
  - `docs/adapter-execution-contract.md`
  - `docs/contrib/runtime-contract.md` (canonical contrib runtime contracts)
  - `docs/contrib/registries.md` (focused adapter/executor registry reference)
  - `docs/contrib/executor-state.md` (focused state reference)
- If changing data/object representation:
  - `docs/object-model.md`
  - `docs/internal/type-system-contracts.md`
- If changing codec behavior or literal write normalization:
  - `docs/codec-system.md`
- If changing storage, references, GC, or artifacts:
  - `docs/internal/storage.md`
  - `docs/storing-and-retrieving-external-data.md`
  - `docs/internal/storage-and-refs.md`
- If changing user-facing errors:
  - `docs/errors.md`


## Ambiguity Rule

If no rule clearly matches:

- Read `docs/system.md` and `docs/README.md`.
- Add or refine a mapping in this file in the same change.


## Maintenance

When adding a new top-level code area or major module, add or update a mapping here in the same PR.

## Scope

This document defines the scope described by its authority and purpose sections.

## Content

See the sections in this document for normative content.

## References

None.
