# Documentation build tooling

Status: implemented

## Purpose

Define the reproducible documentation/dashboard build entrypoint, its automatic
component selection, and its isolated executable-documentation environment.

## Authority

This document is normative for `docs/build.sh` modes, stale-input
detection, packaged-output replacement, and documentation build-tool isolation.

## Scope

Local and CI production builds of the packaged dashboard frontend and its
embedded documentation. It does not define dashboard runtime behavior or the
content contracts of individual documentation pages.

## Content

The executable documentation build is intentionally separate from the published
Python package. `docs/build.sh` installs Quarto 1.7.31, R 4.4.3, knitr
1.49, rmarkdown 2.29, reticulate 1.40.0, and xfun 0.49 into the ignored
repository-local `.tools/` directory. It detects supported macOS/Linux and
x86_64/ARM platforms and keeps Micromamba state, caches, configuration, and
temporary files beneath that directory. Set `DML_DOCS_TOOLS_ROOT` only to move
the complete isolated tool tree elsewhere.

Knitr 1.50 requires R 4.5 on Linux aarch64, so 1.49 is the compatible pinned
release for this R version. Xfun 0.49 retains the knitr API that this pinned
knitr release needs.

Python, Node.js/npm, `uv`, Git, `curl`, `tar`, and native build tools are host
prerequisites. `docs/build.sh` runs `uv sync --group dev --all-extras` and
`npm ci` by default, then uses `DOCS_PYTHON`, or the synchronized repository
`.venv/bin/python`, for both reticulate and Quarto's Jupyter engine. The
interpreter must contain DaggerML, Jupyter, ipykernel, and Moto's server
executable because selected examples use a disposable local S3 endpoint. These
are build dependencies, not published runtime or optional dependencies.

Run `bash docs/build.sh`. The command synchronizes Python and frontend
dependencies, runs frontend tests, and then uses default `--auto` mode to
fingerprint tracked and untracked, non-ignored repository inputs.
Documentation inputs include the docs, package source, and package/build
metadata; frontend inputs are the dashboard UI source and build metadata. The
ignored `.tools/dashboard-build-state` file records the fingerprints from the
last successful component builds.

Missing packaged outputs or a missing state entry makes the corresponding
component stale. A clean checkout therefore runs the complete build, while a
docs-only source change reruns the executable docs without recompiling an
unchanged frontend.

Use `--full` to force selected outputs after an untracked external tool or
environment change. Composable `--no-python-sync`, `--no-npm-ci`, and
`--no-ui-test` options trust existing setup, while `--no-docs` and `--no-ui`
preserve the corresponding complete packaged component. `--full` never
re-enables a disabled component. `bash docs/build.sh --help` is the canonical
reference for prerequisites, every stage and option, skip-state requirements,
and common command examples; help does not bootstrap dependencies.

The docs component bootstraps the build-only toolchain, executes all QMD cells,
and validates and stages the results. The frontend component runs tests and the
production TypeScript/Vite build. Both components build outside the installed
package tree. The coordinator combines new output with any deliberately
preserved component, validates the complete candidate, and only then replaces
`src/daggerml/dashboard/static/`; failures retain the prior packaged dashboard.
Each successful replacement removes stale files. Cached or frozen documentation
execution is rejected before rendering.

All executable pages in one build share a fresh temporary workspace. A page may
declare `depends-on` with a canonical page ID, or a list of IDs; the build
validates that graph and executes pages in stable topological order. Unknown
IDs, duplicate dependencies, self-dependencies, and cycles fail validation.
The `start-here/get-started` page owns the visible creation of the shared example
repository.

An executable QMD that uses one DaggerML project declares its workspace-relative
directory with `dml-project-home` and declares the page that creates it with
`depends-on`. Before executing the page, the build requires that directory to
exist, sets `DML_PROJECT_HOME` to its absolute path, changes the page working
directory to it, and leaves the build-wide isolated `DML_CONFIG_HOME` in place.
The preprocessor never creates or initializes a project implicitly. Pages that
demonstrate multiple projects keep project selection explicit in their examples
instead. The complete temporary workspace and build-only environment disappear
after the build, including on failure.

The documentation may combine prose, pseudocode, and executable examples. All
executable examples are rendered during the build, and any failure fails CI.

Executable pages choose an engine by language. Python-only pages that define
source-inspected functions use Jupyter, so their ordinary inline definitions
remain inspectable. Pages that demonstrate shell commands use knitr and execute
them as native Bash cells under strict shell options. Both engines receive the
same hidden project-home and fixture setup.

Executable examples remain inline in their QMD pages; the packaged documentation
does not stage standalone example downloads or ZIP bundles.

## References

- [Dashboard architecture](../src/daggerml/dashboard/README.md)
- [Contributor workflow](../CONTRIBUTING.md)
