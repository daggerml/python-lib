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
   git clone --recurse-submodules https://github.com/daggerml/python-lib.git
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
- The testing requirements are included in the `test` feature for the library.
  - You can run tests using [hatch](https://hatch.pypa.io/):  
    ```
    hatch run pytest .
    ```
  - If you're using vscode, you can create a venv with the `test` feature and run tests with the command palette:
    ```
    Python: Run Tests
    ```
  - Or install the `test` feature with pip and run tests:  
    ```
    pip install -e </path/to/library>[test]
    pytest .
    ```
- We mark tests with `@pytest.mark.slow` for those that take longer to run. You can run only the fast tests with:
  ```
  pytest -m "not slow" .
  ```
- Run all tests locally before submitting a pull request:
- Ensure your code passes all tests and does not decrease code coverage.
- If your changes introduce new dependencies, please update `pyproject.toml`, but we prefer to keep the dependencies to a minimum.

Thank you for helping make this project better!
