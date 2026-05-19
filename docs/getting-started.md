# Getting Started

Use this page to get a local DaggerML repo running, create one DAG, and inspect it from the CLI.

## Install

DaggerML requires Python 3.10+.

Install `daggerml` in whichever Python environment you want to use:

```bash
pip install "daggerml"
```

## Initialize A Local Repo

`dml init` initializes an existing directory, so create one first:

```bash
mkdir demo && cd demo
dml init
```

That creates `./demo/.dml/` with the local database and config.

## Create Your First DAG

Run the following code in any Python environment you like, such as a script, a notebook, or a REPL.

```python
import daggerml as dml

dag = dml.new(name="hello", message="add hello dag")
result = dag.put({"message": "hello", "value": 42}, name="result")
dag.put([1, 2, 3], name="inputs")
dag.commit(result)
```

This creates a committed DAG named `hello` on the local `main` branch.

## Inspect It

```bash
dml status
dml dag list
dml dag get hello
dml show
```

Use these commands to answer the first questions you usually have:

- `status`: current HEAD, branches, visible DAGs, and open indexes
- `dag list`: DAG names at the selected revision
- `dag get hello`: the stored DAG payload, including named nodes
- `show`: the current commit plus DAG-level changes from its parent

## Next Steps

- Read [reference/cli.md](reference/cli.md) for the generated command surface.
- Read [reference/python-api.md](reference/python-api.md) for the Python entrypoints: `Dml`, `new()`, and `load()`.
- Read [concepts/dags-and-nodes.md](concepts/dags-and-nodes.md) and [concepts/commits-and-history.md](concepts/commits-and-history.md) for the model behind what you just created.
