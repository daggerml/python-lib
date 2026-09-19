## Why

The live Python lessons currently execute through knitr/reticulate, which does not retain inspectable source for functions defined in QMD cells. That forces the documentation to inject definitions from separate Python files, so the authored lesson is no longer the ordinary inline code readers are meant to write.

The mixed-engine and eager source-capture behavior remains current, but `flatten-executable-docs` supersedes this completed change's standalone-download assumptions. Archive this change with `--skip-specs`; the superseding change owns the final documentation structure.

## What Changes

- Permit Python-only executable pages to use a Jupyter kernel while retaining knitr for pages that require native Bash cells.
- Capture a funk's normalized script eagerly when `funkify` decorates the function, while its defining environment can still provide source.
- Replace the Funks and Dagclasses lesson injections with ordinary inline Python cells.
- Provision Jupyter as isolated documentation tooling and validate mixed-engine pages without weakening fail-closed execution.
- Keep downloadable standalone examples file-backed; inline course lessons no longer require a duplicate canonical Python source file.

## Capabilities

### New Capabilities

- `inline-executable-python-docs`: Defines the mixed-engine build contract for inline Python lessons and native Bash lessons.
- `funk-source-capture`: Defines eager, source-only serialization of funkified functions at authoring time.

### Modified Capabilities

None.

## Impact

The change affects `api.funkify`, the script executor handoff, documentation preparation and validation, the isolated docs toolchain, and the Start Here Funks and Dagclasses pages. It adds Jupyter only to build/development dependencies; published package runtime dependencies and source-only worker isolation remain unchanged.
