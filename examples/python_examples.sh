#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="${repo_root}/examples"
# shellcheck source=examples/example_helpers.sh
source "${examples_dir}/example_helpers.sh"

require_env DML_REMOTE_ROOT
dag_namespace="examples/python-examples"

log "Running Python-only examples"
log "Running 00-hello_world.py"
python "${examples_dir}/python/00-hello_world.py" "${dag_namespace}/hello-world"
log "Running 01b-load_fn.py"
python "${examples_dir}/python/01b-load_fn.py" "${dag_namespace}/load-fn" "${dag_namespace}/hello-world"
log "Running wait_fn.py"
python "${examples_dir}/python/wait_fn.py" "${dag_namespace}/wait-fn"
log "Running 03-dagclass.py"
python "${examples_dir}/python/03-dagclass.py" "${dag_namespace}/dagclass"

log "Running 00-errors.py; failure is expected"
if python "${examples_dir}/python/00-errors.py" "${dag_namespace}/errors"; then
  log "00-errors.py unexpectedly succeeded" 2>/dev/null
  exit 1
fi
log "00-errors.py exited with a failure status as expected"

dml show | jq . -C
tmpdag=$(dml show | jq -r --arg dag_name "${dag_namespace}/errors" '.dags[$dag_name]')
dml dag describe "${tmpdag}" | jq . -C
bad_node=$(dml dag get-node-by-name "${tmpdag}" bad)
dml dag describe-node "${bad_node}" | jq . -C
