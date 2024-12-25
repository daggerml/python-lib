# Dagger-ML Python Library

## Prerequisites

- [pipx](https://pypa.github.io/pipx/installation/)
- [hatch](https://hatch.pypa.io/latest/install/#pipx) (via `pipx`)

## Setup

install hatch however you want and clone the repo with submodules.

## Usage

See unit tests (or example) for usage.

## How to run tests:

```bash
hatch -e test run pytest .
```

To build:

```console
hatch -e test run dml-build pypi
```

Note: You might have to reinstall the cli with the editable flag set (e.g. `pip uninstall daggerml-cli; pip install -e ./submodules/daggerml_cli/`)
