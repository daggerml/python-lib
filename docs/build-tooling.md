# Documentation build tooling

Status: implemented

## Purpose

Define the reproducible documentation/dashboard build entrypoint, its automatic
component selection, and its isolated executable-documentation environment.

## Authority

This document is normative for `build-dashboard.sh` modes, stale-input
detection, packaged-output replacement, and documentation build-tool isolation.

## Scope

Local and CI production builds of the packaged dashboard frontend and its
embedded documentation. It does not define dashboard runtime behavior or the
content contracts of individual documentation pages.

## Content

The executable documentation build is intentionally separate from the published
Python package. `build-dashboard.sh` installs Quarto 1.7.31, R 4.4.3, knitr
1.49, rmarkdown 2.29, reticulate 1.40.0, and xfun 0.49 into the ignored
repository-local `.tools/` directory. It detects supported macOS/Linux and
x86_64/ARM platforms and keeps Micromamba state, caches, configuration, and
temporary files beneath that directory. Set `DML_DOCS_TOOLS_ROOT` only to move
the complete isolated tool tree elsewhere.

Knitr 1.50 requires R 4.5 on Linux aarch64, so 1.49 is the compatible pinned
release for this R version. Xfun 0.49 retains the knitr API that this pinned
knitr release needs.

`docs/build.sh` selects Python with `DOCS_PYTHON`, or the `python` on `PATH`,
and exports that absolute path for both reticulate and Quarto's Jupyter engine.
The interpreter must be the project development environment with DaggerML,
Jupyter, and ipykernel installed. Moto's server executable is also required
because selected examples use a disposable local S3 endpoint. These are build
dependencies, not published runtime or optional dependencies.

Run `bash build-dashboard.sh`. Its default `--auto` mode fingerprints tracked
and untracked, non-ignored repository inputs. Documentation inputs include the
docs, examples, package source, and package/build metadata; frontend inputs are
the dashboard UI source and build metadata. The ignored
`.tools/dashboard-build-state` file records the fingerprints from the last
successful component builds.

Missing packaged outputs or a missing state entry makes the corresponding
component stale. A clean checkout therefore runs the complete build, while a
docs-only source change reruns the executable docs without recompiling an
unchanged frontend.

Use `--full` to force both components from scratch, `--docs-only` to rebuild and
replace only packaged docs, or `--ui-only` to rebuild the frontend while
preserving existing packaged docs. Use `--full` when an untracked external tool
or environment change is not represented by repository inputs. `--help` lists
the modes without bootstrapping build dependencies.

The docs component bootstraps the build-only toolchain, executes all QMD cells,
validates and stages the results, and copies only verified docs into the
packaged static tree. The frontend component runs the production Vite build.
Each replacement removes stale files from that component. Cached or frozen
documentation execution is rejected before rendering.

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

The documentation has two teaching modes. Start here is a learn-by-doing course:
important examples are executable, their rendered output is produced during the
build, and any failure fails CI. Concepts explains behavior with prose and code
that is explicitly labeled as pseudocode. The build rejects executable cells in
the Concepts subtree so illustrative code cannot accidentally imply verification.

Executable pages choose an engine by language. Python-only pages that define
source-inspected functions use Jupyter, so their ordinary inline definitions
remain inspectable. Pages that demonstrate shell commands use knitr and execute
them as native Bash cells under strict shell options. Both engines receive the
same hidden project-home and fixture setup.

Canonical downloadable Python source files live below `docs/examples/`. A
`{{< dml-source path.py >}}` marker displays the file verbatim and expands to a
hidden `runpy.run_path` cell at build time. This makes the same unchanged file
both the executed example and the published download. Narrative course examples
that are not downloads stay inline in their QMD pages.

## References

- [Local research dashboard](develop/architecture/dashboard.qmd)
- [Contributor workflow](../CONTRIBUTING.md)
