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

run_hello_world() {
  python "${examples_dir}/python/00-hello_world.py" > /dev/null
  dag_id=$(dml show | jq '.dags["examples/00-hello-world"]' -r)
  node_id=$(dml dag describe "${dag_id}" | jq .names.greeting -r)
  dml dag get-node "${node_id}" | jq '.[1]' -r
}

require_env DML_REMOTE_ROOT
log "Setting up DML repo in ${scratch_dir}"
rm -rf "${scratch_dir}"
mkdir -p "${project_home}"
mkdir -p "${DML_CONFIG_HOME}"
cd "${project_home}"
dml init > /dev/null

first_run=$(run_hello_world)
log "First run result:   ${first_run}"
log "Cached result:      $(run_hello_world)"

dag_id=$(dml show | jq -r '.dags["examples/00-hello-world"]')
node_id=$(dml dag describe "${dag_id}" | jq '.names.greeting' -r)
fndag_id=$(dml dag describe-node "${node_id}" | jq .dag -r)
cache_key=$(dml dag describe "${fndag_id}" | jq .cache_key -r)
dml admin remote invalidate-cache "${cache_key}" > /dev/null

new_run=$(run_hello_world)
log "New result:         ${new_run}"

if [[ "${first_run}" != "${new_run}" ]]; then
  log "Cache invalidation successfully changed the result of hello-world"
else
  log "Cache invalidation did not change the result of hello-world, expected a different result"
  exit 1
fi
log "Cache invalidation changed the hello-world result as expected"
