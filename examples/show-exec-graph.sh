#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="${repo_root}/examples"
# shellcheck source=examples/example_helpers.sh
source "${examples_dir}/example_helpers.sh"

require_env DML_REMOTE_ROOT
dag_name="examples/show-exec-graph/wait-fn"

log "Running wait_fn.py in the background"
python "${examples_dir}/python/wait_fn.py" "${dag_name}" &
wait_pid=$!

log "Waiting for the runtime to start and show the execution graph"
sleep 1
dml runtime describe-graph --visual
index_id=$(dml runtime list | jq -r '.[0].id')
log "Canceling runtime with index ID: ${index_id}"
dml runtime cancel $index_id &
wait_pid2=$!
sleep 0.2
dml runtime describe-graph --visual

for i in {1..4}; do
  sleep 0.2
  dml runtime describe-graph --visual
done
wait "${wait_pid}"
wait "${wait_pid2}"
# for i in {1..4}; do
#   sleep 0.2
#   dml runtime describe-graph --visual
# done
dml runtime describe-graph --visual
echo
