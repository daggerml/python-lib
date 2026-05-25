#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=examples/example_helpers.sh
source "${repo_root}/examples/example_helpers.sh"
example_name="$(basename "${BASH_SOURCE[0]}" .sh)"
scratch_dir="${repo_root}/ignore/examples/${example_name}"
dag_name="examples/bash-dag-execution"
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
log "Initializing DML repo"
dml init | jq . -C

log "Creating runtime and getting index_id"
index_id="$(dml runtime create)"
printf '%s\n' "${index_id}" | jq . -C
index_id="$(printf '%s\n' "${index_id}" | jq -r .)"

log "Putting literals and getting payload_node"
raw="$(printf '%s\n' '["dict", {"message": ["scalar", "hello"], "value": ["scalar", 42]}]' | dml runtime put-literal "${index_id}" - --name payload)"
printf '%s\n' "${raw}" | jq . -C
payload_node="$(printf '%s\n' "${raw}" | jq -r .)"

log "Putting list literal and getting inputs_node"
printf '%s\n' '["list", [["scalar", 1], ["scalar", 2], ["scalar", 3]]]' \
  | dml runtime put-literal "${index_id}" - --name inputs \
  | jq . -C

log "Committing DAG"
commit_ref="$(dml runtime commit "${index_id}" "${payload_node}" --head main --message "Create CLI example DAG" --dag-name "${dag_name}")"
printf '%s\n' "${commit_ref}" | jq . -C

log "Inspecting DAG status and details"
dml status | jq . -C
dml dag get "${dag_name}" | jq . -C
