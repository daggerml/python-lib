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

dml_user="cool-guy"
dml config set user "${dml_user}" --scope global
work_dir="${DML_EXAMPLE_SCRATCH}/work"
dag_namespace="examples/bash-full-cli-workflow"
hello_dag_name="${dag_namespace}/hello-world"

log "Using remote env: ${AWS_ENDPOINT_URL}"
log "Remote root: ${DML_REMOTE_ROOT}"

project0="project-0"
log "Initializing DML repo in ${project0}"
mkdir -p "${work_dir}/${project0}"
cd "${work_dir}/${project0}"
dml --remote-project "dml://${dml_user}/${project0}" init | jq .

log "Configuring and inspecting CLI-visible settings"
dml config set --scope local remote.fetch_workers 2
dml config get remote.root
dml config get remote.fetch_workers
dml config show | jq .
dml config show --contrib | jq .

log "DML repo initialized. Current status"
dml status | jq .
dml rev-parse HEAD | jq .

log "Running example: 00-hello_world.py"
python "${examples_dir}/python/00-hello_world.py" "${hello_dag_name}"

log "Inspecting committed history and DAG state after 00-hello_world.py"
dml status | jq '.branches'
dml log --revision HEAD --limit 10 | jq .
dml show --revision HEAD | jq .
hello_dag_ref="$(dml show --revision HEAD | jq -r --arg dag_name "${hello_dag_name}" '.dags[$dag_name]')"
dml dag delete "${hello_dag_name}"
dml diff --revision HEAD --relative-to HEAD~1 | jq .
dml show --revision HEAD | jq .
greeting_ref="$(dml dag get-node-by-name "${hello_dag_ref}" greeting)"
hello_fn_ref="$(dml dag get-node-by-name "${hello_dag_ref}" hello_fn)"
greeting_fn_dag_ref="$(dml dag describe-node "${greeting_ref}" | jq -r '.dag')"
greeting_cache_key="$(dml dag describe "${greeting_fn_dag_ref}" | jq -r '.cache_key')"
dml dag describe "${hello_dag_ref}" | jq .
dml dag describe-node "${greeting_ref}" | jq .
dml dag get-argv "${greeting_fn_dag_ref}"
dml dag get-node "${greeting_ref}" | jq .
dml dag get-node "${greeting_ref}" --recursive | jq .

log "Exercising low-level runtime and admin CLI commands"
runtime_idx="$(dml runtime create)"
scratch_idx="$(dml runtime create)"
cancel_idx="$(dml runtime create)"
dml runtime list | jq .
dml runtime describe "${runtime_idx}" | jq .

seed_ref="$(printf '%s\n' '["scalar","cli-seed"]' | dml runtime put-literal "${runtime_idx}" - --name seed)"
imported_greeting_ref="$(dml runtime put-import "${runtime_idx}" "${hello_dag_ref}" --node "${greeting_ref}" --name imported-greeting)"
hello_runtime_ref="$(dml runtime put-import "${runtime_idx}" "${hello_dag_ref}" --node "${hello_fn_ref}" --name hello-fn)"
if dml runtime get-argv "${runtime_idx}" >/dev/null 2>&1; then
  fail "Expected runtime get-argv to fail for a non-function runtime index"
fi
dml runtime get-node "${runtime_idx}" seed
dml runtime get-node "${runtime_idx}" imported-greeting
if printf '["%s","%s"]\n' "${hello_runtime_ref}" "${seed_ref}" | dml runtime start-fn "${runtime_idx}" - --name runtime-hello >/dev/null 2>&1; then
  fail "Expected runtime start-fn CLI JSON argv parsing to reject list[Ref] inputs"
fi
dml admin remote get-cache "${greeting_cache_key}"
dml admin remote invalidate-cache "${greeting_cache_key}" | jq .
dml admin remote get-cache "${greeting_cache_key}" | jq .

dml runtime set-node-name "${runtime_idx}" cli-greeting-alias "${imported_greeting_ref}"
dml runtime get-node "${runtime_idx}" cli-greeting-alias
dml runtime describe "${runtime_idx}" | jq .
dml runtime cancel "${cancel_idx}" | jq .
dml runtime describe "${scratch_idx}" | jq .
dml admin gc | jq .

log "Exercising top-level checkout workflows"
dml checkout HEAD~1 | jq .
dml status | jq .
dml checkout main | jq .
dml show --revision HEAD | jq .

log "Listing DML refs after running all examples:"
dml admin remote list-projects --owner "${dml_user}" | jq .
dml push
dml push --revision '#main'
dml admin remote list-projects | jq .
dml admin remote list-refs "dml://${dml_user}/${project0}" | jq .
dml admin remote gc | jq .

log "Fetching into the runner-managed repository"
cd "${DML_EXAMPLE_PROJECT_HOME}"
dml fetch "dml://${dml_user}/${project0}" | jq .
dml rev-parse "dml://${dml_user}/${project0}#main" | jq .
dml admin remote list-refs "dml://${dml_user}/${project0}" | jq .
dml branch move main "dml://${dml_user}/${project0}#main"
remote_hello_dag_ref="${hello_dag_ref}"
log "Checking out remote DAG ref ${remote_hello_dag_ref} into ${dag_namespace}/hello-world-copy"
dml dag checkout "${remote_hello_dag_ref}" "${dag_namespace}/hello-world-copy"
dml status | jq .
dml revert HEAD | jq .
merge_demo_branch="merge-demo"
dml branch create "${merge_demo_branch}"
dml checkout "${merge_demo_branch}" | jq .
merge_demo_idx="$(dml runtime create)"
merge_demo_ref="$(printf '%s\n' '["scalar","merge-demo"]' | dml runtime put-literal "${merge_demo_idx}" - --name merge-demo)"
dml runtime commit "${merge_demo_idx}" "${merge_demo_ref}" --message "Add merge demo DAG" --name "${dag_namespace}/merge-demo"
dml checkout main | jq .
dml merge "${merge_demo_branch}" | jq .
rebase_demo_branch="rebase-demo"
renamed_rebase_branch="rebase-demo-renamed"
dml branch create "${rebase_demo_branch}" --revision "dml://${dml_user}/${project0}#main"
dml checkout "${rebase_demo_branch}" | jq .
dml rebase main | jq .
dml checkout main | jq .
dml branch rename "${rebase_demo_branch}" "${renamed_rebase_branch}"
dml branch list | jq .
dml config set remote.project "dml://${dml_user}/${project0}"
dml pull | jq .
tag_name="cli-demo-tag"
dml tag create "${tag_name}"
dml tag list | jq .
dml tag delete "${tag_name}"
dml dag checkout "${remote_hello_dag_ref}" "${hello_dag_name}"
dml dag delete "${dag_namespace}/merge-demo"
dml branch delete "${renamed_rebase_branch}"
dml status | jq .

log "Running example: 01b-load_fn.py"
python "${examples_dir}/python/01b-load_fn.py" "${dag_namespace}/load-fn" "${hello_dag_name}"

log "Inspecting fetched and pulled history from the second project"
dml status | jq '.branches'
dml admin remote list-refs "dml://${dml_user}/${project0}" | jq .
dml log --revision HEAD --limit 10 | jq .
dml show --revision HEAD | jq .
load_fn_dag_ref="$(dml show --revision HEAD | jq -r --arg dag_name "${dag_namespace}/load-fn" '.dags[$dag_name]')"
dml dag describe "${load_fn_dag_ref}" | jq .
old_result_ref="$(dml dag get-node-by-name "${load_fn_dag_ref}" old_result)"
dml dag get-node "${old_result_ref}" | jq .

log "All examples completed successfully."
