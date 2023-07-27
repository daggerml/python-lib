# Dagger-ML Python Library

## Prerequisites

*pipx*

If [pipx](https://pypa.github.io/pipx/) is not installed, first do that.

*hatch*

Then install [hatch](https://hatch.pypa.io/latest/) via:

```bash
pipx install hatch
```

## Configuration

```bash
# help
dml --help

# configure API endpoint (in the ./.dml directory)
dml configure --api-endpoint ${DML_API_ENDPOINT}
```

```bash
# bash command completion
pip install argcomplete

# enable completion for dml in this shell
eval "$(python "$(which register-python-argcomplete)" dml)"

# enable completion for dml via bashrc
cat <<'EOT' >> ~/.bashrc
eval "$(python "$(which register-python-argcomplete)" dml)"
EOT

# or see: https://kislyuk.github.io/argcomplete/#activating-global-completion
```

You can also use `python -m daggerml` instead of the `dml` executable if you
prefer:

```bash
# help
python -m daggerml --help
```

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

Run test suite with `pytest`.

## Docs

To build the docs, first make sure `bootstrap-docker.py` has been run, then
run: `hatch run docs:build`

To serve the docs: `hatch run docs:serve`

## Tests

To run the tests: `hatch run test:cov`
