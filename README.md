# Dagger-ML Python Library

## Prerequisites

*pipx*

If [pipx](https://pypa.github.io/pipx/) is not installed, first do that.

*hatch*
Then install [hatch](https://hatch.pypa.io/latest/) via: `pipx install hatch`.

## Usage

You currently need `AWS_DEFAULT_REGION` and `DML_ZONE` environment variables
set. Then you can run `python bootstrap-docker.py`, for instance.

`bootstrap-docker.py` sets up the docker-build func, so you can now run docker
stuff in your dags (e.g. as we do in the docs/examples/ directory).

## Run Locally

```bash
# Start local postgres:
sudo systemctl start postgresql

# Connect to local postgres:
psql -h localhost postgres postgres

# Start local DML API server:
python infra/lib/api/server.py

# Run dag locally
DML_LOCAL_DB=1 python mydag.py
```

## Docs

To build the docs, first make sure `bootstrap-docker.py` has been run, then
run: `hatch run docs:build`

To serve the docs: `hatch run docs:serve`

## Tests

To run the tests: `hatch run test:cov`
