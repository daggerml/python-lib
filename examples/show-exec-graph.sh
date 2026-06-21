#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="${repo_root}/examples"
# shellcheck source=examples/example_helpers.sh
source "${examples_dir}/example_helpers.sh"
example_name="$(basename "${BASH_SOURCE[0]}" .sh)"
scratch_dir="${repo_root}/ignore/examples/${example_name}"
project_home="${scratch_dir}/project"
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
log "Setting up DML repo in ${scratch_dir}"
rm -rf "${scratch_dir}"
mkdir -p "${project_home}"
mkdir -p "${DML_CONFIG_HOME}"
cd "${project_home}"
dml init > /dev/null

log "Running wait_fn.py in the background"
python "${examples_dir}/wait_fn.py" &
wait_pid=$!

log "Waiting for the runtime to start and show the execution graph"
sleep 0.5
dml runtime describe-graph --visual
index_id=$(dml runtime list | jq -r '.[0].id')
log "Canceling runtime with index ID: ${index_id}"
dml runtime cancel $index_id &
wait_pid2=$!
sleep 0.1
dml runtime describe-graph --visual

# for i in {1..4}; do
#   sleep 1
#   dml runtime describe-graph --visual
# done
wait "${wait_pid}"
wait "${wait_pid2}"
dml runtime describe-graph --visual
