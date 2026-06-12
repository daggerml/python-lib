# Getting Started

Use this page to get a local DaggerML repo running.

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

## Next Steps

- Read [reference/cli.md](reference/cli.md) for the generated command surface.
- Read [reference/python-api.md](reference/python-api.md) for the Python entrypoints: `Dml`, `new()`, and `load()`.
- Read [concepts/dags-and-nodes.md](concepts/dags-and-nodes.md) and [concepts/commits-and-history.md](concepts/commits-and-history.md) for the model behind what you just created.
