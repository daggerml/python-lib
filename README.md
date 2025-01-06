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
dml repo create $repo_name
```

We initialize our current project (e.g. tell the project which repo and branch to use).

```bash
dml project init $repo_name
```

Now we can create dags or whatever we want using this repo.

```python
import daggerml as dml

dag = dml.new("test", "this dag is a test")
_ = dag.commit(42)
```

Now we can list repos, dags, 

```bash
dml dag list
```

## Clean up

```bash
dml repo delete $repo_name
```

## Docs

For more info, check out the docs at [daggerml.com](https://daggerml.com).
