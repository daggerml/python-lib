# Contributing to DaggerML

Thank you for your interest in contributing! We welcome contributions via pull
requests and appreciate your help in improving this project.

## Contributor Workflow References

- `AGENTS.md`: agent-specific working notes and script-executor caveats.
- `DOC_MAP.md`: which project docs to read before editing a given code path.
- `openspec/README.md`: change-planning artifacts and current OpenSpec spec surfaces.

## Reporting Issues

- Search [existing issues](https://github.com/daggerml/python-lib/issues) before submitting a new one.
- When reporting a bug, please include:
  - A clear, descriptive title.
  - Steps to reproduce the issue.
  - Expected and actual behavior.
  - Python version and operating system.
  - Relevant code snippets or error messages.

## How to Contribute Code

1. Create a new branch for your feature or bugfix (with the github issue in the name).
2. Clone the repository and set it up:
   ```bash
   git clone https://github.com/daggerml/python-lib.git
   ```
3. Make your changes in the new branch.
4. Write or update tests as needed.
5. Ensure all tests pass locally.
6. Push to your branch on GitHub.
7. Open a pull request against the `master` branch of this repository.

## Development Setup

- Python 3.11 or newer, [uv](https://docs.astral.sh/uv/), and a C/C++ toolchain
  with CMake are required for the Cython-backed LMDB extension.
- Set up a checkout with `uv sync --dev --all-extras`.
- The dashboard source and package-manager commands live in `dashboard-ui/`.
  Distribution changes must include built static assets in
  `src/daggerml/dashboard/`; installed `dml-dashboard` must not require Node.js.

## Repository Orientation

- `src/daggerml/README.md`: public package and CLI boundary.
- `src/daggerml/_core/README.md`: repository, storage, execution, and remote boundary.
- `src/daggerml/dashboard/README.md` and `dashboard-ui/README.md`: dashboard server and frontend boundary.
- `openspec/spec-overview.md`: normative architecture and capability ownership.

## Coding Standards

- Follow [PEP 8](https://pep8.org/) for Python code style.
- Use [numpy style docstrings](https://numpydoc.readthedocs.io/en/latest/format.html) for all public modules, classes, functions, and methods.
- Write clear, concise commit messages.
- Keep pull requests focused and minimal.

## Testing Guidelines

- Add or update unit tests for any new features or bug fixes.
- Use [pytest](https://pytest.org/) for running tests.
- Standard local dev command pattern is:
  ```bash
  uv run --dev <python command>
  ```
- When a command needs optional dependencies, include all extras:
  ```bash
  uv run --dev --all-extras <python command>
  ```
- Run tests with:
  ```bash
  uv run --dev --all-extras pytest .
  ```
- Run lint with:
  ```bash
  uv run --dev --all-extras ruff check --fix .
  ```
- We mark tests with `@pytest.mark.slow` for those that take longer to run. You can run only the fast tests with:
  ```
  uv run --dev --all-extras pytest -m "not slow" .
  ```
- CI continues to run the full suite (`uv run pytest .`) to preserve complete coverage while local quick loops use `-m "not slow"`.
- We mark tests under `tests/_core/` with `@pytest.mark.core`. Core tests are included by default. You can select or skip them with:
  ```
  uv run --dev --all-extras pytest -m core .
  uv run --dev --all-extras pytest -m "not core" .
  ```
- Run all tests locally before submitting a pull request:
- Ensure your code passes all tests and does not decrease code coverage.
- If your changes introduce new dependencies, please update `pyproject.toml`, but we prefer to keep the dependencies to a minimum.

## Documentation Build

- Python, Node.js/npm, `uv`, Git, `curl`, `tar`, and the native package build
  toolchain are host prerequisites. Executable documentation also requires
  Docker with a running daemon to build and run the course's container image.
  The build command owns project dependency
  setup, frontend tests, the pinned Quarto/R bootstrap, documentation execution,
  frontend compilation, and packaged-output validation:
  ```bash
  bash docs/build.sh
  ```
- The default automatic mode runs `uv sync`, `npm ci`, and frontend tests, then
  fingerprints repository inputs and rebuilds stale components. A clean checkout
  therefore performs the complete verified build. Use composable `--no-*` flags
  to trust existing dependencies or preserve one packaged component, and use
  `--full` to force selected outputs. Run `bash docs/build.sh --help` for every
  option, stage interaction, and common command example.
- Documentation and frontend output are assembled away from the installed
  package tree. The existing packaged dashboard is replaced only after the
  complete candidate validates, so failed builds retain the prior output.
- The script installs its pinned Quarto/R toolchain and all related caches under
  the ignored `.tools/` directory. Set `DOCS_PYTHON` when the project interpreter
  is not `.venv/bin/python`:
  ```bash
  DOCS_PYTHON="/path/to/python" bash docs/build.sh
  ```
- The build executes all QMD examples against disposable fixtures; do not use it as a substitute for the full test suite.
- CI builds and verifies the dashboard and executable documentation once in the
  Linux `dashboard` job. Wheel and source-distribution jobs download its
  `dashboard-assets` artifact and package those same verified files. macOS wheel
  runners therefore do not need Docker or the documentation build toolchain.

### GitHub Pages

The separate `Documentation Pages` workflow runs after `CI` completes successfully
for a push to `master`. It checks out the exact commit that passed CI, executes
the documentation build, and replaces the entire organization website by publishing
to the root of `daggerml/daggerml.github.io`'s `gh-pages` branch. Stale website
files are removed, and the `daggerml.com` custom domain is preserved.
Pull requests, tags, other branches, and unsuccessful CI runs do not deploy.
The site URL is <https://daggerml.com/> (the organization Pages site).
Configure `DOCS_PUBLISH_TOKEN` in this repository or its `github-pages` environment
as a fine-grained token with **Contents: read and write** on
`daggerml/daggerml.github.io`. The built-in `GITHUB_TOKEN` cannot publish to a
different repository. In the destination repository's **Settings → Pages**, keep
the source set to **Deploy from a branch**, `gh-pages`, `/ (root)`.

To generate the same standalone output locally:

```bash
DOCS_SITE_OUTPUT="$PWD/.tools/pages" bash docs/build.sh
```

`DOCS_SITE_OUTPUT` must be an absolute output directory. It forces documentation
rendering even when the packaged docs are up to date and cannot be combined with
`--no-docs`. The output uses the dashboard's actual React documentation component
and stylesheet, including its docs sidebar, page outline, filtering, copy buttons,
diagrams, and theme switcher. The dashboard application sidebar is omitted.
The landing page is served directly at `/`, and each documentation route has a
static entry page so direct links and reloads work on GitHub Pages.
Serve that directory at the root of a static HTTP server to preview it; this
output uses root-relative URLs rather than a repository URL prefix.

Both the dashboard and standalone site include an **API reference** section at `/docs/api/`,
generated by pdoc from the public `daggerml` exports, `daggerml.api`,
`daggerml.contrib` and its public submodules, and `daggerml.util`. Signatures,
type annotations, and NumPy-style docstrings come directly from the checked-out
code. API pages use the same documentation viewer, stylesheet, theme switcher,
page filter, and heading outline as Start here, Use, and Extend. The page filter
matches module names; it is not a full-text symbol search. Published `/api/` URLs
redirect to the integrated pages. pdoc is a development dependency; API generation
runs during every documentation rebuild.

### Test taxonomy and naming

This section is for contributors maintaining or restructuring the test suite.

#### Directory layout

- `tests/contracts/`: fast, isolated tests that verify one documented requirement or invariant.
- `tests/integration/`: multi-component or infrastructure-dependent tests.
- `tests/_core/contracts/`: fast, isolated tests for `daggerml._core` contracts.
- `tests/_core/integration/`: multi-component or infrastructure-dependent tests for `daggerml._core`.
- Subsystem-owned suites such as `tests/_core/`, `tests/api/`, and `tests/contrib/` keep `contracts/` and `integration/` subdirectories under the subsystem root.

#### File naming

- Contract tests should use `test_<surface>_<contract>.py`.
- Integration tests should use `test_<surface>_<scenario>_integration.py`.
- Avoid generic names such as `test_core.py` when a more specific contract surface is known.

#### Function naming and contract IDs

- Name test functions directly for the behavior they verify: `test_<behavior>()`.
- Let the test module path own subsystem, surface, and contract-versus-integration context; do not repeat that context or numeric contract IDs in function names.
- Group tests in a `Test<Subject>` class only when they share a public subject or fixture-backed scenario. Keep unrelated tests at module level.
- Specify canonical contract IDs directly as literal strings where traceability is needed.
- Use uppercase category prefixes and numeric suffixes such as `ADP-OUT-001`, `EXEC-LC-003`, and `EST-LOCK-004`.
- For parameterized cases, retain the canonical ID in `id=`, for example `id="EXEC-LC-003:resume-uses-launch-state"`.

#### Lifecycle parameterization

- Tests that exercise a lifecycle should prefer one parameterized test per contract family over multiple near-duplicate tests.
- Make lifecycle stages explicit in case IDs, for example `kickoff`, `resume`, `terminal-succeeded`, and `terminal-failed`.

#### Marker policy

- Integration tests that require external processes, polling loops, remote roundtrips, or significant runtime orchestration must be marked `@pytest.mark.slow`.
- Contract tests in `tests/contracts/` should stay unmarked and fast by default.
- Tests under `tests/_core/` are marked `@pytest.mark.core` by `tests/_core/conftest.py` and remain included in default pytest runs.

#### Taxonomy maintenance

- Keep each maintained behavior in one taxonomy-aligned location.
- Remove a superseded or duplicate test when its replacement coverage is added.
- Preserve canonical contract IDs in replacement parameterized case IDs where applicable.

## Migration Rollout Policy

When migrating storage or execution paths, use phased rollouts with tests at each phase:

1. Implement the new destination path first and test it.
2. Write to both old and new paths and test.
3. Read from the new path and test.
4. Stop writing to the old path and test.
5. Remove the old path and test.

Thank you for helping make this project better!
