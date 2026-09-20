## Why

Documentation and examples currently live outside the dashboard and their displayed snippets are not verified by rendering. Bring them into the persistent dashboard layout and make documentation builds execute the code they publish so failures block CI and releases.

This completed change's documentation hierarchy and download deltas are superseded by `flatten-executable-docs`. Archive this change with `--skip-specs`; the superseding change owns the final documentation requirements.

## What Changes

- Convert human-facing docs to executable Quarto QMD, retaining the Use, Extend, Develop, and getting-started organization.
- Add global Docs navigation inside the existing dashboard shell, with an examples subtree and downloadable canonical Python scripts.
- Use knitr throughout: Python cells run through reticulate and Bash commands run directly in Bash cells, never Python shell wrappers.
- Execute every non-pseudocode code block on every documentation build, including hidden cells; reject skipped execution and fail the top-level build on unexpected errors.
- Hide disposable service/database setup and environment configuration except when setup is the subject being taught. Support Moto S3/CloudWatch and SSH fixtures where selected examples need them.
- Curate simpler Python examples; exclude shell-oriented push/pull workflows from the embedded examples. Redundant Bash commands that launch Python scripts need not appear.
- Package generated documentation and downloads with dashboard assets. Package dependencies MUST NOT change; build dependencies may change.
- **BREAKING**: replace migrated human-facing `.md` source paths with `.qmd` paths and reserve `/docs` for dashboard documentation, relocating the current Swagger UI.

## Capabilities

### New Capabilities

- `executable-documentation-build`: knitr execution, isolated hidden fixtures, fail-closed CI, and verified downloadable example sources.
- `dashboard-documentation`: static documentation and examples inside the persistent dashboard layout, routes, navigation, downloads, and packaged availability.

### Modified Capabilities

- `human-facing-project-docs`: QMD source format, embedded examples navigation, and updated getting-started path.
- `dashboard-revision-navigation`: Home and Docs are global destinations without project or commit scope.

## Impact

- Affects `docs/**`, selected `examples/**`, `dashboard-ui/**`, `src/daggerml/dashboard/**`, asset packaging, and `.github/workflows/ci.yml`.
- Adds build-only Quarto, R, knitr, rmarkdown, and reticulate tooling with an explicitly selected project Python interpreter; does not add or alter package runtime or optional dependencies.
- Updates repository links, `DOC_MAP.md`, contributor build guidance, and dashboard architecture documentation.
- Does not change DaggerML storage/execution semantics or execute documentation on dashboard requests. Maintainer workflow and OpenSpec artifacts remain outside product docs.
