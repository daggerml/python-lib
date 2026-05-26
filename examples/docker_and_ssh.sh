#!/usr/bin/env bash
# exit fast if no docker or ssh support
if ! docker info > /dev/null 2>&1; then
  echo "Docker does not seem to be available, skipping docker examples"
  exit 0
fi
if ! ssh -o BatchMode=yes -o ConnectTimeout=5 localhost true > /dev/null 2>&1; then
  echo "SSH does not seem to be available, skipping SSH examples"
  exit 0
fi

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

log "Initializing DML repo in ${example_name}"
rm -rf "${scratch_dir}/${example_name}" || true
mkdir "${scratch_dir}/${example_name}"
cd "${scratch_dir}/${example_name}"
dml init --remote-project "dml://${dml_user}/${example_name}"

log "DML repo initialized. Current status:"
dml status | jq .

log "Running example: 00-hello_world.py"
python "${examples_dir}/00-hello_world.py"

log "Running example: 01-docker_dataset.py"
python "${examples_dir}/01-docker_dataset.py"

log "Running example: 02-ssh_docker_dataset.py"
python "${examples_dir}/02-ssh_docker_dataset.py"

log "All examples completed successfully."
