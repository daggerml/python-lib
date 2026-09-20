# Edit Doc Map

Audience: coding agents and maintainers working on this repository.

Use this file to identify which project docs to read before editing a code path, then list the docs you consulted in your summary or PR notes.

## Global Docs (Always Read)

- `docs/start-here/index.qmd`: audience-first documentation navigation.
- `src/daggerml/README.md` and `src/daggerml/_core/README.md`: system-level package and core boundaries.

## Path Rules

### Public Python API

- Match: `src/daggerml/api.py`, `src/daggerml/__init__.py`
- Read:
  - `docs/start-here/dags.qmd`
  - `docs/start-here/funks.qmd`
  - `docs/use/artifacts.qmd`
  - `docs/use/inspection.qmd`
  - `docs/glossary.qmd`
  - `src/daggerml/_core/README.md`

### Codec module

- Match: codec implementation in `src/daggerml/api.py` and `src/daggerml/contrib/codecs.py`
- Read:
  - `docs/use/artifacts.qmd`
  - `docs/extend/codecs.qmd`
  - `docs/use/inspection.qmd`

### CLI surface

- Match: `src/daggerml/_cli.py`
- Read:
  - the owning workflow under `docs/use/`
  - `src/daggerml/README.md`
  - `docs/use/inspection.qmd`

### Core repository operations

- Match: `src/daggerml/_core/commit.py`, `src/daggerml/_core/dag.py`, `src/daggerml/_core/head.py`, `src/daggerml/_core/index.py`
- Read:
  - `src/daggerml/_core/README.md`
  - `docs/use/inspection.qmd`
  - `docs/use/sharing.qmd`

### Core runtime orchestration

- Match: `src/daggerml/_core/dml.py`, `src/daggerml/_core/config.py`, `src/daggerml/_core/revision.py`, `src/daggerml/_core/uri.py`
- Read:
  - `docs/use/projects.qmd`
  - `docs/use/execution.qmd`
  - `docs/use/runtimes.qmd`
  - `src/daggerml/_core/README.md`

### Core types and serde

- Match: `src/daggerml/_core/types.py`, `src/daggerml/_core/builtins.py`, `src/daggerml/_core/serde.py`
- Read:
  - `docs/glossary.qmd`
  - `docs/use/artifacts.qmd`
  - `docs/use/inspection.qmd`
  - `src/daggerml/_core/README.md`

### Core storage and database integration

- Match: `src/daggerml/_core/db.pyx`, `src/daggerml/_core/util.py`
- Read:
  - `docs/use/artifacts.qmd`
  - `src/daggerml/_core/README.md`

### Runtime and remote execution

- Match: `src/daggerml/_core/dml.py`, `src/daggerml/_core/index.py`, `src/daggerml/_core/exec_state.py`, `src/daggerml/util.py`, `src/daggerml/_core/remote.py`, `src/daggerml/_core/s3_cas.py`
- Read:
  - `docs/use/execution.qmd`
  - `docs/use/runtimes.qmd`
  - `docs/use/sharing.qmd`
  - `src/daggerml/_core/README.md`

### Contrib modules and integrations

- Match: `src/daggerml/contrib/**`
- Read:
  - `docs/extend/codecs.qmd`
  - `docs/extend/adapters.qmd`
  - `docs/extend/executors.qmd`

### C implementation and headers

- Match: `c/src/**`, `c/include/**`
- Read:
  - `c/README.md`
  - `src/daggerml/_core/README.md`
  - `docs/use/artifacts.qmd`
  - `docs/use/inspection.qmd`

### Tests

- Match: `tests/**`
- Read:
  - docs corresponding to the code under test using the rules above
  - `CONTRIBUTING.md`

### Packaging, build, and CI

- Match: `pyproject.toml`, `uv.lock`, `CMakeLists.txt`, `docs/build*`, `.github/workflows/**`
- Read:
  - `README.md`
  - `CONTRIBUTING.md`
  - `src/daggerml/README.md`
  - `c/README.md` when changing the C build

### Documentation edits

- Match: `docs/**`
- Read:
  - `docs/start-here/index.qmd`
  - `docs/start-here/get-started.qmd` when changing onboarding
  - the target canonical course page under `docs/use/` or `docs/extend/` when changing an audience path

### Local research dashboard

- Match: `src/daggerml/dashboard/**`, `dashboard-ui/**`
- Read:
  - `src/daggerml/dashboard/README.md`
  - `dashboard-ui/README.md`
  - `src/daggerml/_core/README.md`
  - `docs/sharp-bits-and-security.qmd`

## Topic Rules (Apply In Addition To Path Rules)

- If changing adapter behavior:
  - `docs/extend/adapters.qmd`
  - `docs/extend/executors.qmd`
  - `src/daggerml/_core/README.md`
- If changing data/object representation:
  - `docs/use/artifacts.qmd`
  - `docs/use/inspection.qmd`
  - `docs/glossary.qmd`
  - `src/daggerml/_core/README.md`
- If changing codec behavior or literal write normalization:
  - `docs/use/artifacts.qmd`
  - `docs/extend/codecs.qmd`
- If changing Projection, committed collection traversal, or Projection reuse:
  - `docs/use/inspection.qmd`
  - `docs/extend/codecs.qmd` only when changing `ProjectionCodec` internals
- If changing storage, references, GC, or artifacts:
  - `docs/use/artifacts.qmd`
  - `src/daggerml/_core/README.md`
- If changing user-facing errors:
  - `docs/use/inspection.qmd`

## Ambiguity Rule

If no rule clearly matches:

- Read `src/daggerml/README.md`, `src/daggerml/_core/README.md`, and `docs/start-here/index.qmd`.
- Add or refine a mapping in this file in the same change.

## Maintenance

When adding a new top-level code area or major module, add or update a mapping here in the same PR.
