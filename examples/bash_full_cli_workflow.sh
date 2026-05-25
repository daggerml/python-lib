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

log "Using remote env: ${AWS_ENDPOINT_URL}"
log "Remote root: ${DML_REMOTE_ROOT}"

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

log "Configuring and inspecting CLI-visible settings"
dml config set --scope local remote.fetch_workers 2 | jq .
dml config get remote.root | jq .
dml config get remote.fetch_workers | jq .
dml config show | jq .
dml config show --contrib | jq .

log "DML repo initialized. Current status"
dml status | jq .

log "Running example: 00-hello_world.py"
python "${examples_dir}/00-hello_world.py"

log "Inspecting committed history and DAG state after 00-hello_world.py"
dml branch | jq .
dml log --revision HEAD --limit 10 | jq .
dml show --revision HEAD | jq .
dml diff --left HEAD~1 --right HEAD | jq .
dml show --revision HEAD | jq .
dml dag get examples/00-hello-world | jq .
dml dag describe-node greeting --dag examples/00-hello-world | jq .
dml dag get-node greeting --dag examples/00-hello-world | jq .
dml dag get-node greeting --dag examples/00-hello-world --recursive | jq .

hello_dag_ref="$(dml dag get examples/00-hello-world | jq -r '.ref')"
hello_fn_ref="$(dml dag describe-node hello_fn --dag examples/00-hello-world | jq -r '.ref')"
greeting_ref="$(dml dag describe-node greeting --dag examples/00-hello-world | jq -r '.ref')"

log "Exercising low-level runtime and admin CLI commands"
runtime_idx="$(dml runtime create | jq -r .)"
scratch_idx="$(dml runtime create | jq -r .)"
cancel_idx="$(dml runtime create | jq -r .)"
dml runtime list | jq .
dml runtime describe "${runtime_idx}" | jq .

seed_ref="$(printf '%s\n' '["scalar","cli-seed"]' | dml runtime put-literal "${runtime_idx}" - --name seed | jq -r .)"
imported_greeting_ref="$(dml runtime put-import "${runtime_idx}" "${hello_dag_ref}" --node "${greeting_ref}" --name imported-greeting | jq -r .)"
hello_runtime_ref="$(dml runtime put-import "${runtime_idx}" "${hello_dag_ref}" --node "${hello_fn_ref}" --name hello-fn | jq -r .)"
dml runtime get-node "${runtime_idx}" seed | jq .
dml runtime get-node "${runtime_idx}" imported-greeting | jq .

dml runtime set-node-name "${runtime_idx}" cli-greeting-alias "${imported_greeting_ref}" | jq .
dml runtime get-node "${runtime_idx}" cli-greeting-alias | jq .
dml runtime describe "${runtime_idx}" | jq .
dml runtime cancel "${cancel_idx}" | jq .
dml runtime delete "${scratch_idx}" | jq .
dml admin gc --dry-run | jq .
dml admin gc | jq .

log "Exercising top-level checkout workflows"
dml checkout HEAD~1 | jq .
dml status | jq .
dml checkout main | jq .
dml show --revision HEAD | jq .

log "Listing DML refs after running all examples:"
dml admin remote list --owner "${dml_user}" | jq .
dml push --create | jq .
dml push --tag cli-demo-tag | jq .
dml admin remote list | jq .
dml admin remote gc --min-age-seconds 0 --malformed warn | jq .

log "Cleaning up first project to test fresh init with existing remote"
cd .. && rm -rf "${project0}"

## Second "project"
project1="project-1"
log "Initializing DML repo in ${project1}"
rm -rf "${scratch_dir}/${project1}" || true
mkdir "${scratch_dir}/${project1}"
cd "${scratch_dir}/${project1}"
dml init --remote-project "dml://${dml_user}/${project1}"
dml fetch "dml://${dml_user}/${project0}" | jq .
dml branch --remote | jq .
dml dag checkout "dml://${dml_user}/${project0}#main" "examples/00-hello-world" --target-name examples/00-hello-world-copy | jq .
dml status | jq .
dml revert HEAD "${dml_user}" | jq .
dml merge "dml://${dml_user}/${project0}#main" "${dml_user}" | jq .
dml pull "dml://${dml_user}/${project0}" "${dml_user}" | jq .
dml dag checkout "dml://${dml_user}/${project0}#main" "examples/00-hello-world" | jq .
dml status | jq .

log "Running example: 01b-load_fn.py"
python "${examples_dir}/01b-load_fn.py"

log "Inspecting fetched and pulled history from the second project"
dml branch | jq .
dml branch --remote | jq .
dml log --revision HEAD --limit 10 | jq .
dml show --revision HEAD | jq .
dml dag get examples/01b-load-fn --revision HEAD | jq .
dml dag get-node old_result --dag examples/01b-load-fn --revision HEAD | jq .

log "All examples completed successfully."
