# Dagger-ML Python Library

## Prerequisites

*pipx*

If [pipx](https://pypa.github.io/pipx/) is not installed, first do that.

*hatch*
Then install [hatch](https://hatch.pypa.io/latest/) via: `pipx install hatch`.

## Configuration

```bash
# help
python -m daggerml --help

# configure global API endpoint
python -m daggerml configure \
  --global \
  --profile ${DML_PROFILE} \ # optional
  --api-endpoint https://api.${DML_ZONE}-${AWS_REGION}.${DOMAIN}

# configure API key
python -m daggerml login \
  --profile ${DML_PROFILE} \ # optional
  --username ${USERNAME}

# configure group ID for local project
python -m daggerml configure --group-id ${DML_GROUP_ID}
```

```bash
# bash command completion
pip install argcomplete

# enable completion for dml in this shell
eval "$(register-python-argcomplete dml)"

# enable completion for dml via bashrc
cat <<'EOT' >> ~/.bashrc
eval "$(register-python-argcomplete dml)"
EOT

# or see: https://kislyuk.github.io/argcomplete/#activating-global-completion
```

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
