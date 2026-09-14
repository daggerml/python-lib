# Documentation build tooling

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

Run `bash build-dashboard.sh`. It bootstraps the build-only toolchain, executes
all QMD cells, validates and stages the results, builds the frontend, and copies
only verified docs into the packaged static tree. Cached or frozen execution is
rejected before rendering.

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
