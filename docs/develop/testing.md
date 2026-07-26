# Testing DaggerML

Use `uv` to run the suite in the repository development environment:

```bash
uv run --dev --all-extras pytest .
```

For a faster local loop, exclude slow tests:

```bash
uv run --dev --all-extras pytest -m "not slow" .
```

Useful selections include a path, a test name, or the `core` marker:

```bash
uv run --dev --all-extras pytest tests/_core/contracts/test_dag_tree_contracts.py
uv run --dev --all-extras pytest -m core .
```

Run lint before submitting a change:

```bash
uv run --dev --all-extras ruff check .
```

`tests/api/`, `tests/_core/`, and `tests/contrib/` broadly follow the public,
repository-core, and integration-extension boundaries. Contract versus
integration placement, markers, naming, and migration requirements are
maintained in the canonical [test policy](../../CONTRIBUTING.md#testing-guidelines),
not duplicated here.
