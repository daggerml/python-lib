# Edit Doc Map

Audience: coding agents and maintainers working on this repository.

Use this file to identify which project docs to read before editing a code path, then list the docs you consulted in your summary or PR notes.

## Global Docs (Always Read)

- `docs/index.qmd`: audience-first documentation navigation.
- `docs/develop/architecture/system-overview.qmd`: system-level layers and subsystem boundaries.

## Path Rules

### Public Python API

- Match: `src/daggerml/api.py`, `src/daggerml/__init__.py`
- Read:
  - `docs/use/reference/python-authoring.qmd`
  - `docs/use/concepts/dags-nodes-results.qmd`
  - `docs/glossary.qmd`
  - `docs/use/reference/errors.qmd`
  - `docs/use/concepts/artifacts-data-codecs.qmd`
  - `docs/develop/architecture/dag-storage-and-types.qmd`

### Codec module

- Match: codec implementation in `src/daggerml/api.py` and `src/daggerml/contrib/codecs.py`
- Read:
  - `docs/use/concepts/artifacts-data-codecs.qmd`
  - `docs/extend/reference/codec-contracts.qmd`
  - `docs/use/reference/errors.qmd`

### CLI surface

- Match: `src/daggerml/_cli.py`
- Read:
  - `docs/use/reference/cli.qmd`
  - `docs/develop/architecture/public-api-and-cli.qmd`
  - `docs/use/reference/errors.qmd`

### Core repository operations

- Match: `src/daggerml/_core/commit.py`, `src/daggerml/_core/dag.py`, `src/daggerml/_core/head.py`, `src/daggerml/_core/index.py`
- Read:
  - `docs/develop/architecture/dag-storage-and-types.qmd`
  - `docs/develop/architecture/execution-and-runtime-state.qmd`
  - `docs/use/concepts/dags-nodes-results.qmd`
  - `docs/use/concepts/history-remotes.qmd`

### Core runtime orchestration

- Match: `src/daggerml/_core/dml.py`, `src/daggerml/_core/config.py`, `src/daggerml/_core/revision.py`, `src/daggerml/_core/uri.py`
- Read:
  - `docs/use/reference/configuration.qmd`
  - `docs/use/concepts/funks-execution-cache.qmd`
  - `docs/develop/architecture/execution-and-runtime-state.qmd`
  - `docs/use/reference/errors.qmd`

### Core types and serde

- Match: `src/daggerml/_core/types.py`, `src/daggerml/_core/builtins.py`, `src/daggerml/_core/serde.py`
- Read:
  - `docs/glossary.qmd`
  - `docs/use/concepts/dags-nodes-results.qmd`
  - `docs/use/concepts/artifacts-data-codecs.qmd`
  - `docs/develop/architecture/dag-storage-and-types.qmd`
  - `docs/use/reference/errors.qmd`

### Core storage and database integration

- Match: `src/daggerml/_core/db.pyx`, `src/daggerml/_core/util.py`
- Read:
  - `docs/use/concepts/artifacts-data-codecs.qmd`
  - `docs/develop/architecture/dag-storage-and-types.qmd`
  - `docs/use/guides/artifacts.qmd`

### Runtime and remote execution

- Match: `src/daggerml/_core/dml.py`, `src/daggerml/_core/index.py`, `src/daggerml/_core/exec_state.py`, `src/daggerml/util.py`, `src/daggerml/_core/remote.py`, `src/daggerml/_core/s3_cas.py`
- Read:
  - `docs/use/concepts/funks-execution-cache.qmd`
  - `docs/use/concepts/runtimes.qmd`
  - `docs/use/concepts/history-remotes.qmd`
  - `docs/develop/architecture/execution-and-runtime-state.qmd`
  - `docs/develop/architecture/remotes-and-sync.qmd`

### Contrib modules and integrations

- Match: `src/daggerml/contrib/**`
- Read:
  - `docs/extend/index.qmd`
  - `docs/extend/concepts/extension-model.qmd`
  - `docs/extend/concepts/adapters-and-executors.qmd`
  - `docs/extend/reference/adapter-operations.qmd`
  - `docs/extend/reference/executor-lifecycle.qmd`
  - `docs/extend/reference/codec-contracts.qmd`
  - `docs/extend/reference/plugin-api.qmd`

### C implementation and headers

- Match: `c/src/**`, `c/include/**`
- Read:
  - `c/README.md`
  - `docs/develop/architecture/dag-storage-and-types.qmd`
  - `docs/use/concepts/dags-nodes-results.qmd`

### Tests

- Match: `tests/**`
- Read:
  - docs corresponding to the code under test using the rules above
  - `docs/develop/testing.qmd`
  - `CONTRIBUTING.md`

### Packaging, build, examples, and CI

- Match: `pyproject.toml`, `uv.lock`, `CMakeLists.txt`, `examples/**`, `.github/workflows/**`
- Read:
  - `README.md`
  - `CONTRIBUTING.md`
  - `docs/develop/architecture/system-overview.qmd`
  - `c/README.md` when changing the C build

### Documentation edits

- Match: `docs/**`
- Read:
  - `docs/index.qmd`
  - `docs/getting-started.qmd` when changing onboarding
  - the target audience landing page under `docs/use/`, `docs/extend/`, or `docs/develop/` when changing an audience path

### Local research dashboard

- Match: `src/daggerml/dashboard/**`, `dashboard-ui/**`
- Read:
  - `docs/develop/architecture/dashboard.qmd`
  - `docs/develop/architecture/system-overview.qmd`
  - `docs/develop/architecture/execution-and-runtime-state.qmd`
  - `docs/develop/architecture/remotes-and-sync.qmd`
  - `docs/sharp-bits-and-security.qmd`

## Topic Rules (Apply In Addition To Path Rules)

- If changing adapter behavior:
  - `docs/extend/concepts/adapters-and-executors.qmd`
  - `docs/extend/reference/adapter-operations.qmd`
  - `docs/extend/reference/executor-lifecycle.qmd`
  - `docs/develop/architecture/execution-and-runtime-state.qmd`
- If changing data/object representation:
  - `docs/use/concepts/dags-nodes-results.qmd`
  - `docs/glossary.qmd`
  - `docs/develop/architecture/dag-storage-and-types.qmd`
- If changing codec behavior or literal write normalization:
  - `docs/use/concepts/artifacts-data-codecs.qmd`
  - `docs/extend/reference/codec-contracts.qmd`
- If changing storage, references, GC, or artifacts:
  - `docs/use/guides/artifacts.qmd`
  - `docs/develop/architecture/dag-storage-and-types.qmd`
- If changing user-facing errors:
  - `docs/use/reference/errors.qmd`

## Ambiguity Rule

If no rule clearly matches:

- Read `docs/develop/architecture/system-overview.qmd` and `docs/index.qmd`.
- Add or refine a mapping in this file in the same change.

## Maintenance

When adding a new top-level code area or major module, add or update a mapping here in the same PR.
