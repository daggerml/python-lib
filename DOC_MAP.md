# Edit Doc Map

Audience: coding agents and maintainers working on this repository.

Use this file to identify which project docs to read before editing a code path, then list the docs you consulted in your summary or PR notes.

## Global Docs (Always Read)

- `docs/README.md`: audience-first documentation navigation.
- `docs/develop/architecture/system-overview.md`: system-level layers and subsystem boundaries.

## Path Rules

### Public Python API

- Match: `src/daggerml/api.py`, `src/daggerml/__init__.py`
- Read:
  - `docs/use/reference/python-authoring.md`
  - `docs/use/concepts/dags-nodes-results.md`
  - `docs/glossary.md`
  - `docs/use/reference/errors.md`
  - `docs/use/concepts/artifacts-data-codecs.md`
  - `docs/develop/architecture/dag-storage-and-types.md`

### Codec module

- Match: codec implementation in `src/daggerml/api.py` and `src/daggerml/contrib/codecs.py`
- Read:
  - `docs/use/concepts/artifacts-data-codecs.md`
  - `docs/extend/reference/codec-contracts.md`
  - `docs/use/reference/errors.md`

### CLI surface

- Match: `src/daggerml/_cli.py`
- Read:
  - `docs/use/reference/cli.md`
  - `docs/develop/architecture/public-api-and-cli.md`
  - `docs/use/reference/errors.md`

### Core repository operations

- Match: `src/daggerml/_core/commit.py`, `src/daggerml/_core/dag.py`, `src/daggerml/_core/head.py`, `src/daggerml/_core/index.py`
- Read:
  - `docs/develop/architecture/dag-storage-and-types.md`
  - `docs/develop/architecture/execution-and-runtime-state.md`
  - `docs/use/concepts/dags-nodes-results.md`
  - `docs/use/concepts/history-remotes.md`

### Core runtime orchestration

- Match: `src/daggerml/_core/dml.py`, `src/daggerml/_core/config.py`, `src/daggerml/_core/revision.py`, `src/daggerml/_core/uri.py`
- Read:
  - `docs/use/reference/configuration.md`
  - `docs/use/concepts/funks-execution-cache.md`
  - `docs/develop/architecture/execution-and-runtime-state.md`
  - `docs/use/reference/errors.md`

### Core types and serde

- Match: `src/daggerml/_core/types.py`, `src/daggerml/_core/builtins.py`, `src/daggerml/_core/serde.py`
- Read:
  - `docs/glossary.md`
  - `docs/use/concepts/dags-nodes-results.md`
  - `docs/use/concepts/artifacts-data-codecs.md`
  - `docs/develop/architecture/dag-storage-and-types.md`
  - `docs/use/reference/errors.md`

### Core storage and database integration

- Match: `src/daggerml/_core/db.pyx`, `src/daggerml/_core/util.py`
- Read:
  - `docs/use/concepts/artifacts-data-codecs.md`
  - `docs/develop/architecture/dag-storage-and-types.md`
  - `docs/use/guides/artifacts.md`

### Runtime and remote execution

- Match: `src/daggerml/_core/dml.py`, `src/daggerml/_core/index.py`, `src/daggerml/_core/exec_state.py`, `src/daggerml/util.py`, `src/daggerml/_core/remote.py`, `src/daggerml/_core/s3_cas.py`
- Read:
  - `docs/use/concepts/funks-execution-cache.md`
  - `docs/use/concepts/runtimes.md`
  - `docs/use/concepts/history-remotes.md`
  - `docs/develop/architecture/execution-and-runtime-state.md`
  - `docs/develop/architecture/remotes-and-sync.md`

### Contrib modules and integrations

- Match: `src/daggerml/contrib/**`
- Read:
  - `docs/extend/README.md`
  - `docs/extend/concepts/extension-model.md`
  - `docs/extend/concepts/adapters-and-executors.md`
  - `docs/extend/reference/adapter-operations.md`
  - `docs/extend/reference/executor-lifecycle.md`
  - `docs/extend/reference/codec-contracts.md`
  - `docs/extend/reference/plugin-api.md`

### C implementation and headers

- Match: `c/src/**`, `c/include/**`
- Read:
  - `c/README.md`
  - `docs/develop/architecture/dag-storage-and-types.md`
  - `docs/use/concepts/dags-nodes-results.md`

### Tests

- Match: `tests/**`
- Read:
  - docs corresponding to the code under test using the rules above
  - `docs/develop/testing.md`
  - `CONTRIBUTING.md`

### Packaging, build, examples, and CI

- Match: `pyproject.toml`, `uv.lock`, `CMakeLists.txt`, `examples/**`, `.github/workflows/**`
- Read:
  - `README.md`
  - `CONTRIBUTING.md`
  - `docs/develop/architecture/system-overview.md`
  - `c/README.md` when changing the C build

### Documentation edits

- Match: `docs/**`
- Read:
  - `docs/README.md`
  - `docs/getting-started.md` when changing onboarding
  - the target audience landing page under `docs/use/`, `docs/extend/`, or `docs/develop/` when changing an audience path

## Topic Rules (Apply In Addition To Path Rules)

- If changing adapter behavior:
  - `docs/extend/concepts/adapters-and-executors.md`
  - `docs/extend/reference/adapter-operations.md`
  - `docs/extend/reference/executor-lifecycle.md`
  - `docs/develop/architecture/execution-and-runtime-state.md`
- If changing data/object representation:
  - `docs/use/concepts/dags-nodes-results.md`
  - `docs/glossary.md`
  - `docs/develop/architecture/dag-storage-and-types.md`
- If changing codec behavior or literal write normalization:
  - `docs/use/concepts/artifacts-data-codecs.md`
  - `docs/extend/reference/codec-contracts.md`
- If changing storage, references, GC, or artifacts:
  - `docs/use/guides/artifacts.md`
  - `docs/develop/architecture/dag-storage-and-types.md`
- If changing user-facing errors:
  - `docs/use/reference/errors.md`

## Ambiguity Rule

If no rule clearly matches:

- Read `docs/develop/architecture/system-overview.md` and `docs/README.md`.
- Add or refine a mapping in this file in the same change.

## Maintenance

When adding a new top-level code area or major module, add or update a mapping here in the same PR.
