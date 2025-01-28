# Getting started

## Installation

First install [`daggerml-cli`](https://github.com/daggerml/daggerml-cli) via

```bash
pipx install daggerml-cli
```

Install [`daggerml`](https://github.com/daggerml/python-lib) in whichever [virtual environment](https://docs.python.org/3/tutorial/venv.html) you prefer.

```bash
pip install daggerml
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
