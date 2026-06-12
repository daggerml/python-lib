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

python "${examples_dir}/00-errors.py"

dml show | jq . -C
tmpdag=$(dml show | jq -r '.dags["examples/00-errors"]')
dml dag describe "${tmpdag}" | jq . -C
bad_node=$(dml dag get-node-by-name "${tmpdag}" bad)
dml dag describe-node "${bad_node}" | jq . -C
