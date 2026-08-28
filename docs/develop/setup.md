# Development Setup

## Prerequisites

- Python 3.11 or newer.
- [uv](https://docs.astral.sh/uv/) for the repository's development commands.
- A C/C++ build toolchain and CMake, because the package builds a Cython-backed
  LMDB extension.

## Create a checkout

```bash
git clone https://github.com/daggerml/python-lib.git
cd python-lib
uv sync --dev --all-extras
```

Run commands through `uv` so they use the resolved development environment:

```bash
uv run --dev --all-extras pytest -m "not slow" .
uv run --dev --all-extras ruff check .
```

The test suite builds the package and its native database layer as needed. See
[Testing](testing.md) for focused commands and the canonical test policy.

## Working with remote-backed features

Core execution coordination and remote sync use S3 through `boto3`. Most
contract tests are local; integration tests may require their declared test
fixtures or external-service emulation. Start with a focused local test before
attempting a remote integration scenario.

## Working on the dashboard

Install all Python extras before running dashboard server or packaging tests:

```bash
uv sync --dev --all-extras
```

The React/TypeScript source and its package-manager commands live under
`dashboard-ui/`. Production distribution builds must compile that source and
include the resulting static assets under `src/daggerml/dashboard/`. Verify both
wheel and source-distribution contents when changing the frontend build; an
installed `dml-dashboard` must not require Node.js.
