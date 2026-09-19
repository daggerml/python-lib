## Context

See `proposal.md` for motivation. The documentation build currently binds every page to knitr and injects an R setup cell that configures reticulate. That supports native Python and Bash cells, but reticulate compiles inline Python with a synthetic filename and no durable source cache, so `inspect.getsource()` cannot recover decorated definitions. The Funks and Dagclasses lessons therefore mirror their code in separate Python files and inject regions into QMD.

The existing build contract requires every authored example to execute, native shell examples to remain Bash cells, declared page dependencies to run in order, and build tools to remain outside published runtime dependencies.

## Goals / Non-Goals

**Goals:**

- Make inline Python definitions source-inspectable in the lessons that require it.
- Preserve native Bash execution on pages that teach shell commands.
- Capture funk source once, at the earliest public authoring boundary.
- Keep per-page fixture setup hidden and equivalent across engines.

**Non-Goals:**

- Switching every documentation page to Jupyter.
- Capturing closures, globals, or interpreter state.
- Replacing file-backed downloadable examples with notebook documents.
- Changing DAG result, caching, or worker execution semantics.

## Decisions

### Use a hybrid Quarto engine policy

Python-only pages that require inspectable definitions use a Python Jupyter kernel. Pages containing Bash or R cells remain on knitr. Quarto renders both kinds in the same project and the existing topological page order remains authoritative.

Using Jupyter everywhere was rejected because Jupyter shell escapes would violate the native Bash-cell contract. Keeping knitr everywhere was rejected because eager capture cannot manufacture source that reticulate never registers.

### Generate engine-specific hidden setup

The preparation harness reads each page's engine. For knitr it emits the existing hidden R setup; for Jupyter it emits a hidden Python setup cell that sets fixture environment variables and changes to the declared page working directory before authored cells run. Validation accepts only the supported engine declarations and rejects native Bash/R cells in Jupyter pages.

This keeps `dml-project-home`, `depends-on`, and dependency ordering as one engine-neutral authoring contract.

### Store the canonical script in delayed runnable metadata

`funkify` renders the canonical script when it receives a callable and stores the rendered script and selected function name in the delayed runnable metadata. The script executor prefers those captured values. A compatibility fallback remains for previously constructed delayed runnables that contain only the callable.

Capturing a general Python serializer was rejected: the source-only boundary is deliberate and keeps worker behavior auditable. Deferring `getsource()` was rejected because notebook and REPL source caches are most reliable near definition time.

### Keep Jupyter build-only and use the project interpreter

The Jupyter kernel runs from the documentation build environment that contains the editable project. Jupyter is declared only in development/build tooling and does not enter package runtime or optional dependencies shipped to users.

### Inline narrative examples; retain files for actual downloads

The Funks and Dagclasses course pages execute ordinary inline cells in narrative order and delete their duplicate injection sources. Region injection remains available for standalone examples whose files are published for download.

## Risks / Trade-offs

- **Two engines can drift in setup behavior** → cover preparation and validation with engine-matrix tests and run the full dashboard build.
- **Jupyter availability or kernel selection can vary** → pin it in the build environment and explicitly select the project interpreter/kernel.
- **Eager capture changes the failure point** → specify and test immediate failure when source is unavailable; retain executor fallback only for old delayed metadata.
- **Notebook source behavior can regress upstream** → include a real Quarto/Jupyter render exercising inline `funkify` in the end-to-end docs build.

## Migration Plan

1. Add eager canonical script capture and compatibility handling with unit coverage.
2. Add hybrid-engine preparation, validation, and isolated Jupyter tooling.
3. Convert Funks and Dagclasses to Jupyter and inline their definitions.
4. Remove only the duplicate lesson source files, then run policy checks, unit tests, and the complete dashboard build.

Rollback restores those two pages to knitr and their canonical source injections; captured script metadata is backward-compatible with the executor fallback.
