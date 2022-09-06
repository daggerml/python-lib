# Dagger-ML Python Library

## Usage

TODO

## Setup

To set this up, you need to run:

1. `. ./bash-env.sh <ZONE>`
2. `python bootstrap-docker.py`

Then you can either run individual examples from the **docs/examples/**
directory, or run the docs (see below).

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

To build the docs, run:
`cd docs && make html && (cd _build/html && python3 -m http.server 8080)`

Then, wait for everything to run, and then check it out in the browser.
