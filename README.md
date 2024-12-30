# Dagger-ML Python Library

## Installation

### Prerequisites

If you don't already have `daggerml_cli` installed in your path (not necessarily in this environment), please do so.

## Usage

See unit tests (or example) for usage.

## Dev work

### Prerequisites

- [pipx](https://pypa.github.io/pipx/installation/)
- [hatch](https://hatch.pypa.io/latest/install/#pipx) (via `pipx`)

### Setup

install hatch however you want and clone the repo with submodules.

### How to run tests:

```bash
hatch -e test run pytest .
```

To build:

```console
hatch -e test run dml-build pypi
```

Note: You might have to reinstall the cli with the editable flag set (e.g. `pip uninstall daggerml-cli; pip install -e ./submodules/daggerml_cli/`)
