# Dagger-ML Python Library

## Prerequisites

* [pipx](https://pypa.github.io/pipx/installation/)
* [hatch](https://hatch.pypa.io/latest/install/#pipx) (via `pipx`)

## Configure

Get the DaggerML API endpoint from the `api-endpoint` output of your DaggerML
infrastructure Terraform CDK stack. Then start a hatch shell with a Python
virtual environment:

```bash
# start a hatch shell
hatch shell
```

Within this shell you can use the `dml` executable to configure the DaggerML
SDK for this project:

```
# configure API endpoint (in the ./.dml directory)
dml configure --api-endpoint ${DML_API_ENDPOINT}
```

You can enable command completion for `dml` if you want:

```bash
# bash command completion
pip install argcomplete

# enable completion for dml in this shell, or add to your .bashrc file, or
# see: https://kislyuk.github.io/argcomplete/#activating-global-completion
eval "$(python "$(which register-python-argcomplete)" dml)"
```

You can also use `python -m daggerml` instead of `dml` if you prefer that.

## Usage

You need AWS credentials for the region in which your DaggerML infrastructure
is located.

```python
import daggerml as dml

d0 = dml.Dag.new('my-dag')
d0.commit(42)

d1 = dml.Dag.new('another-dag')
fourty_two = d1.load('my-dag')
d1.commit(fourty_two.to_py() + 1)
```

## Tests

Run test suite with `pytest`:

```bash
pytest -vsx
```

## Docs

```bash
# build the docs
hatch run docs:build

# start a server to view the docs
hatch run docs:serve
```
