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
- We mark tests that require `daggerml-cli` to be installed with `@pytest.mark.needs_dml`. You can exclude those tests with:
  ```
  uv run --dev --all-extras pytest -m "not needs_dml" .
  ```
- Run all tests locally before submitting a pull request:
- Ensure your code passes all tests and does not decrease code coverage.
- If your changes introduce new dependencies, please update `pyproject.toml`, but we prefer to keep the dependencies to a minimum.

### Test taxonomy and naming

This section is for contributors maintaining or restructuring the test suite.

#### Directory layout

- `tests/contracts/`: fast, isolated tests that verify one documented requirement or invariant.
- `tests/integration/`: multi-component or infrastructure-dependent tests.
- Existing folders such as `tests/_internal/` and `tests/contrib/` may remain during migration, but new or refactored suites should target `tests/contracts/` or `tests/integration/`.

#### File naming

- Contract tests should use `test_<surface>_<contract>.py`.
- Integration tests should use `test_<surface>_<scenario>_integration.py`.
- Avoid generic names such as `test_core.py` when a more specific contract surface is known.

#### Function naming and contract IDs

- Prefer `test_<contract_id_slug>__<behavior>()` where practical.
- Example: `test_exec_lc_003__resume_uses_launch_state()`.
- Specify canonical contract IDs directly as literal strings.
- Use uppercase category prefixes and numeric suffixes such as `ADP-OUT-001`, `EXEC-LC-003`, and `EST-LOCK-004`.
- For parameterized cases, include the canonical ID in `id=`, for example `id="EXEC-LC-003:resume-uses-launch-state"`.

#### Lifecycle parameterization

- Tests that exercise a lifecycle should prefer one parameterized test per contract family over multiple near-duplicate tests.
- Make lifecycle stages explicit in case IDs, for example `kickoff`, `resume`, `terminal-succeeded`, and `terminal-failed`.

#### Marker policy

- Integration tests that require external processes, polling loops, remote roundtrips, or significant runtime orchestration must be marked `@pytest.mark.slow`.
- Contract tests in `tests/contracts/` should stay unmarked and fast by default.

#### Migration policy

- Migration is full replacement, not indefinite dual maintenance.
- When a legacy test is superseded by a new contract-structured test, remove the legacy test in the same change set or immediately after parity is confirmed.
- During migration, preserve traceability by carrying canonical contract IDs into new parameterized case IDs.
- The end state is for all maintained tests to align to this taxonomy.

## Migration Rollout Policy

When migrating storage or execution paths, use phased rollouts with tests at each phase:

1. Implement the new destination path first and test it.
2. Write to both old and new paths and test.
3. Read from the new path and test.
4. Stop writing to the old path and test.
5. Remove the old path and test.

Thank you for helping make this project better!
