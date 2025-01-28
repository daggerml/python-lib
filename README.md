# Getting started

## Installation

First install `daggerml-cli` via

```bash
pipx install daggerml-cli
```

Install `daggerml` in whatever [virtual environment](https://docs.python.org/3/tutorial/venv.html) you want:

```bash
pip install daggerml
```

## Setting up a repo

Now we create a repo using the commandline.

```bash
dml config user testy@mctesterstein.org
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
dml repo delete $repo_name
```

## Docs

For more info, check out the docs at [daggerml.com](https://daggerml.com).
