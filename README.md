# daggerml [![PyPI - Version](https://img.shields.io/pypi/v/daggerml.svg)](https://pypi.org/project/daggerml) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/daggerml.svg)](https://pypi.org/project/daggerml)

DaggerML Python library for creating and managing DAGs (Directed Acyclic Graphs) for machine learning workflows.

## Getting started

## Installation

Install [`daggerml`](https://github.com/daggerml/python-lib) in whichever [virtual environment](https://docs.python.org/3/tutorial/venv.html) you prefer.

```bash
pip install daggerml
```

For the CLI functionality, you have two options:

**Option 1: Install CLI with daggerml (recommended for most users)**
```bash
pip install daggerml[cli]
```

**Option 2: Install CLI separately with pipx (keeps dependencies isolated)**
```bash
pipx install daggerml-cli
```

## Setting up a repo

Now we create a repo using the commandline.

```bash
dml config user ${EMAIL}
dml repo create ${REPO_NAME}
dml config repo ${REPO_NAME}
```

Now we can create dags or whatever we want using this repo.

```python
from daggerml import Dml

with Dml().new("test", "this dag is a test") as dag:
  dag.result = 42
```

Now we can list repos, dags, etc.

```bash
dml dag list
```

## Clean up

```bash
dml repo delete ${REPO_NAME}
```

## Docs

For more info, check out the docs at [daggerml.com](https://daggerml.com).

## Contributing

If you want to contribute, please check out the [contributing guide](CONTRIBUTING.md).

## License

`daggerml` is distributed under the terms of the [MIT](LICENSE) license.
