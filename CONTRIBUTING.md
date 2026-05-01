# Contributing to DaggerML

Thank you for your interest in contributing! We welcome contributions via pull
requests and appreciate your help in improving this project.

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
  uv run --dev --all-extras ruff check .
  ```
- We mark tests with `@pytest.mark.slow` for those that take longer to run. You can run only the fast tests with:
  ```
  uv run --dev --all-extras pytest -m "not slow" .
  ```
- Contract-first test layout guidance:
  - fast, isolated contract tests live under `tests/contracts/`,
  - integration or infrastructure-heavy coverage lives under `tests/integration/` and should be marked `@pytest.mark.slow`,
  - canonical contract IDs should be embedded directly in test names or parameterized case IDs for migrated contract suites.
- CI continues to run the full suite (`uv run pytest .`) to preserve complete coverage while local quick loops use `-m "not slow"`.
- We mark tests that require `daggerml-cli` to be installed with `@pytest.mark.needs_dml`. You can exclude those tests with:
  ```
  uv run --dev --all-extras pytest -m "not needs_dml" .
  ```
- Run all tests locally before submitting a pull request:
- Ensure your code passes all tests and does not decrease code coverage.
- If your changes introduce new dependencies, please update `pyproject.toml`, but we prefer to keep the dependencies to a minimum.

## Migration Rollout Policy

When migrating storage or execution paths, use phased rollouts with tests at each phase:

1. Implement the new destination path first and test it.
2. Write to both old and new paths and test.
3. Read from the new path and test.
4. Stop writing to the old path and test.
5. Remove the old path and test.

Thank you for helping make this project better!
