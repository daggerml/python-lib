#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="${repo_root}/examples"
# shellcheck source=examples/example_helpers.sh
source "${examples_dir}/example_helpers.sh"

require_env DML_REMOTE_ROOT
dag_namespace="examples/docker-and-ssh"

log "Using remote env: ${AWS_ENDPOINT_URL}"
log "Remote root: ${DML_REMOTE_ROOT}"

log "DML repo initialized. Current status:"
dml status | jq .

log "Running example: 00-hello_world.py"
python "${examples_dir}/python/00-hello_world.py" "${dag_namespace}/hello-world"

log "Running example: 01-docker_dataset.py"
python "${examples_dir}/python/01-docker_dataset.py" "${dag_namespace}/docker-dataset"

log "Running example: 02-ssh_docker_dataset.py"
python "${examples_dir}/python/02-ssh_docker_dataset.py" "${dag_namespace}/ssh-docker-dataset" "${dag_namespace}/docker-dataset"

log "All examples completed successfully."
