#!/usr/bin/env bash
# Note: you might have to run this script in `uv`

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="${repo_root}/examples"
# shellcheck source=examples/example_helpers.sh
source "${examples_dir}/example_helpers.sh"
ignore_dir="${repo_root}/ignore"
example_name="$(basename "${BASH_SOURCE[0]}" .sh)"
scratch_dir="${ignore_dir}/examples/${example_name}"
export DML_CONFIG_HOME="${scratch_dir}/dml_config"

cleanup() {
  if [[ "${KEEP_EXAMPLE_SCRATCH:-0}" == "1" ]]; then
    log "Keeping scratch directory: ${scratch_dir}"
    return
  fi
  rm -rf "${scratch_dir}"
}
trap cleanup EXIT

require_env DML_REMOTE_ROOT

mkdir -p "${DML_CONFIG_HOME}"
dml_user="cool-guy"
dml config set --scope global user $dml_user

log "Setting up DML repo in ${ignore_dir}"
mkdir -p "${ignore_dir}/examples"
rm -rf "${scratch_dir}"
mkdir -p "${scratch_dir}"
printf '*\n' > "${ignore_dir}/.gitignore"
cd "${scratch_dir}"

project0="project-0"
log "Initializing DML repo in ${project0}"
rm -rf "${scratch_dir}/${project0}" || true
mkdir "${scratch_dir}/${project0}"
cd "${scratch_dir}/${project0}"
dml init --remote-project "dml://${dml_user}/${project0}" | jq .

log "DML repo initialized. Current status"
dml status | jq .

log "Running example: 00-hello_world.py"
python "${examples_dir}/00-hello_world.py"

log "Listing DML refs after running all examples:"
dml push --create

log "Cleaning up first project to test fresh init with existing remote"
cd .. && rm -rf "${project0}"

## Second "project"
project1="project-1"
log "Initializing DML repo in ${project1}"
rm -rf "${scratch_dir}/${project1}" || true
mkdir "${scratch_dir}/${project1}"
cd "${scratch_dir}/${project1}"
dml init --remote-project "dml://${dml_user}/${project1}" | jq .
dml fetch "dml://${dml_user}/${project0}"
dml dag checkout "dml://${dml_user}/${project0}#main" "examples/00-hello-world" --target-name examples/00-hello-world

log "Running example: 01b-load_fn.py"
python "${examples_dir}/01b-load_fn.py"

log "All examples completed successfully."
