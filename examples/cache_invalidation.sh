#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="${repo_root}/examples"
# shellcheck source=examples/example_helpers.sh
source "${examples_dir}/example_helpers.sh"
dag_name="examples/cache-invalidation/hello-world"

run_hello_world() {
  python "${examples_dir}/python/00-hello_world.py" "${dag_name}" > /dev/null
  dag_id=$(dml show | jq --arg dag_name "${dag_name}" '.dags[$dag_name]' -r)
  node_id=$(dml dag describe "${dag_id}" | jq .names.greeting -r)
  dml dag get-node "${node_id}" | jq '.[1]' -r
}

require_env DML_REMOTE_ROOT

first_run=$(run_hello_world)
log "First run result:   ${first_run}"
log "Cached result:      $(run_hello_world)"

dag_id=$(dml show | jq -r --arg dag_name "${dag_name}" '.dags[$dag_name]')
node_id=$(dml dag describe "${dag_id}" | jq '.names.greeting' -r)
fndag_id=$(dml dag describe-node "${node_id}" | jq .dag -r)
cache_key=$(dml dag describe "${fndag_id}" | jq .cache_key -r)
dml cache invalidate "${cache_key}" > /dev/null

new_run=$(run_hello_world)
log "New result:         ${new_run}"

if [[ "${first_run}" != "${new_run}" ]]; then
  log "Cache invalidation successfully changed the result of hello-world"
else
  log "Cache invalidation did not change the result of hello-world, expected a different result"
  exit 1
fi
log "Cache invalidation changed the hello-world result as expected"
