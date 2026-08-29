#!/usr/bin/env bash
# Note: you might have to run this script in `uv`

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="${repo_root}/examples"
# shellcheck source=examples/example_helpers.sh
source "${examples_dir}/example_helpers.sh"

require_env DML_REMOTE_ROOT
require_env DML_EXAMPLE_PROJECT_HOME
require_env DML_EXAMPLE_SCRATCH

dml config set user "cool-guy" --scope global
work_dir="${DML_EXAMPLE_SCRATCH}/work"
dag_namespace="examples/push-load"
hello_dag_name="${dag_namespace}/hello-world"

project0="project-0"
log "Initializing DML repo in ${project0}"
mkdir -p "${work_dir}/${project0}"
cd "${work_dir}/${project0}"
dml init | jq .

log "DML repo initialized. Current status"
dml status | jq .

log "Running example: 00-hello_world.py"
python "${examples_dir}/python/00-hello_world.py" "${hello_dag_name}"

branch="push-load-$(uuidgen | tr -d '-' | tr '[:upper:]' '[:lower:]')"
log "Publishing example branch ${branch}:"
dml branch create "${branch}"
dml checkout "${branch}"
dml push

log "Fetching into the runner-managed repository"
cd "${DML_EXAMPLE_PROJECT_HOME}"
dml fetch "${branch}"
dml dag checkout "${branch}" "${hello_dag_name}" --remote

log "Running example: 01b-load_fn.py"
python "${examples_dir}/python/01b-load_fn.py" "${dag_namespace}/load-fn" "${hello_dag_name}"

log "All examples completed successfully."
