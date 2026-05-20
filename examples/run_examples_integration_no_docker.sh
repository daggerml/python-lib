#!/usr/bin/env bash
# Note: you might have to run this script in `uv`

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="${repo_root}/examples"
ignore_dir="${repo_root}/ignore"
scratch_dir="${ignore_dir}/scratch"
moto_dir="${ignore_dir}/.integration-moto-$(date +%s)-$$"
moto_env_file="${moto_dir}/moto.env"
moto_log_file="${moto_dir}/moto.log"
moto_pid=""
export DML_CONFIG_HOME="${scratch_dir}/dml_config"

log() {
  echo >&2
  echo "*** $* ***" >&2
}

pretty_dml() {
  log "Calling: dml $*"
  dml "$@" | jq .
}

json_scalar() {
  jq -r .
}

s3_ls_recursive() {
  local s3_uri="$1"
  python - "$s3_uri" <<'PY'
from __future__ import annotations

import os
import sys
from urllib.parse import urlparse

import boto3


def main() -> None:
    uri = sys.argv[1]
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise RuntimeError(f"expected s3://bucket[/prefix], got: {uri!r}")

    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL") or None

    client = boto3.client("s3", endpoint_url=endpoint_url)
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            dt = obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S")
            size = obj["Size"]
            key = obj["Key"]
            print(f"{dt} {size:>10d} {key}")


if __name__ == "__main__":
    main()
PY
}

cleanup() {
  if [[ -n "${moto_pid}" ]]; then
    kill "${moto_pid}" >/dev/null 2>&1 || true
    wait "${moto_pid}" >/dev/null 2>&1 || true
  fi
  rm -rf "${moto_dir}"
  if [[ "${KEEP_EXAMPLE_SCRATCH:-0}" == "1" ]]; then
    log "Keeping scratch directory: ${scratch_dir}"
    return
  fi
  rm -rf "${scratch_dir}"
}
trap cleanup EXIT

mkdir -p "${moto_dir}"
mkdir -p "${DML_CONFIG_HOME}"
dml_user="cool-guy"
dml config set --scope global user $dml_user

log "Starting moto server and preparing env..."
python - "${moto_env_file}" >"${moto_log_file}" 2>&1 <<'PY' &
from __future__ import annotations

import os
import shlex
import signal
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
from moto.server import ThreadedMotoServer


def main() -> None:
    env_file = Path(sys.argv[1])
    remote_root = os.environ.get("DML_EXAMPLE_REMOTE_ROOT", "s3://daggerml-example/artifacts")
    parsed = urlparse(remote_root)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise RuntimeError(f"DML_EXAMPLE_REMOTE_ROOT must be s3://bucket[/prefix], got: {remote_root!r}")
    bucket = parsed.netloc

    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"

    env_values = {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": "us-east-1",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_ENDPOINT_URL": endpoint,
        "DML_REMOTE_ROOT": remote_root,
    }

    for key, value in env_values.items():
        os.environ[key] = value

    boto3.client("s3", endpoint_url=endpoint).create_bucket(Bucket=bucket)
    env_file.write_text("\n".join(f"export {k}={shlex.quote(v)}" for k, v in env_values.items()) + "\n")

    stop = False

    def _handle_signal(_signum, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    while not stop:
        time.sleep(0.2)

    server.stop()


if __name__ == "__main__":
    main()
PY
moto_pid="$!"

for _ in $(seq 1 100); do
  if [[ -s "${moto_env_file}" ]]; then
    break
  fi
  if ! kill -0 "${moto_pid}" >/dev/null 2>&1; then
    log "Moto bootstrap failed. Log output:" >&2
    cat "${moto_log_file}" >&2
    exit 1
  fi
  sleep 0.1
done

if [[ ! -s "${moto_env_file}" ]]; then
  log "Timed out waiting for moto env file. Log output:" >&2
  cat "${moto_log_file}" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${moto_env_file}"
log "Moto ready: ${AWS_ENDPOINT_URL}"
log "Remote root: ${DML_REMOTE_ROOT}"

log "Setting up DML repo in ${ignore_dir}"
mkdir -p "${scratch_dir}" || true
printf '*\n' > "${ignore_dir}/.gitignore"
cd "${scratch_dir}"

project0="project-0"
log "Initializing DML repo in ${project0}"
rm -rf "${scratch_dir}/${project0}" || true
mkdir "${scratch_dir}/${project0}"
cd "${scratch_dir}/${project0}"
dml init --remote-project "dml://${dml_user}/${project0}"

log "Configuring and inspecting CLI-visible settings"
pretty_dml config set --scope local remote.fetch_workers 2
pretty_dml config get remote.root
pretty_dml config get remote.fetch_workers
pretty_dml config show
pretty_dml config show --contrib

log "DML repo initialized. Current status"
pretty_dml status

log "Running example: 00-hello_world.py"
python "${examples_dir}/00-hello_world.py"

log "Inspecting committed history and DAG state after 00-hello_world.py"
pretty_dml branch
pretty_dml log --revision HEAD --limit 10
pretty_dml show --revision HEAD
pretty_dml diff --left HEAD~1 --right HEAD
pretty_dml show --revision HEAD
pretty_dml dag get examples/00-hello-world
pretty_dml dag describe-node greeting --dag examples/00-hello-world
pretty_dml dag get-node greeting --dag examples/00-hello-world
pretty_dml dag get-node greeting --dag examples/00-hello-world --recursive

hello_dag_ref="$(dml dag get examples/00-hello-world | jq -r '.ref')"
hello_fn_ref="$(dml dag describe-node hello_fn --dag examples/00-hello-world | jq -r '.ref')"
greeting_ref="$(dml dag describe-node greeting --dag examples/00-hello-world | jq -r '.ref')"

log "Exercising low-level runtime and admin CLI commands"
runtime_idx="$(dml runtime create | json_scalar)"
scratch_idx="$(dml runtime create | json_scalar)"
cancel_idx="$(dml runtime create | json_scalar)"
pretty_dml runtime list
pretty_dml runtime describe "${runtime_idx}"

seed_ref="$(dml runtime put-literal "${runtime_idx}" cli-seed --name seed | json_scalar)"
imported_greeting_ref="$(dml runtime put-import "${runtime_idx}" "${hello_dag_ref}" --node "${greeting_ref}" --name imported-greeting | json_scalar)"
hello_runtime_ref="$(dml runtime put-import "${runtime_idx}" "${hello_dag_ref}" --node "${hello_fn_ref}" --name hello-fn | json_scalar)"
pretty_dml runtime get-node "${runtime_idx}" seed
pretty_dml runtime get-node "${runtime_idx}" imported-greeting

pretty_dml runtime set-node-name "${runtime_idx}" cli-greeting-alias "${imported_greeting_ref}"
pretty_dml runtime get-node "${runtime_idx}" cli-greeting-alias
pretty_dml runtime describe "${runtime_idx}"
pretty_dml runtime cancel "${cancel_idx}"
pretty_dml runtime delete "${scratch_idx}"
pretty_dml admin gc --dry-run
pretty_dml admin gc

log "Exercising top-level checkout workflows"
pretty_dml checkout HEAD~1
pretty_dml status
pretty_dml checkout main
pretty_dml show --revision HEAD

log "Listing DML refs after running all examples:"
s3_ls_recursive "${DML_REMOTE_ROOT}/dml/refs/projects/"
pretty_dml admin remote list --owner "${dml_user}"
pretty_dml push --create
pretty_dml push --tag cli-demo-tag
pretty_dml admin remote list
pretty_dml admin remote gc --min-age-seconds 0 --malformed warn
s3_ls_recursive "${DML_REMOTE_ROOT}/dml/refs/projects/"

log "Cleaning up first project to test fresh init with existing remote"
cd .. && rm -rf "${project0}"

## Second "project"
project1="project-1"
log "Initializing DML repo in ${project1}"
rm -rf "${scratch_dir}/${project1}" || true
mkdir "${scratch_dir}/${project1}"
cd "${scratch_dir}/${project1}"
dml init --remote-project "dml://${dml_user}/${project1}"
pretty_dml fetch "dml://${dml_user}/${project0}"
pretty_dml branch --remote
pretty_dml dag checkout "dml://${dml_user}/${project0}#main" "examples/00-hello-world" --target-name examples/00-hello-world-copy
pretty_dml status
pretty_dml revert HEAD "${dml_user}"
pretty_dml merge "dml://${dml_user}/${project0}#main" "${dml_user}"
pretty_dml pull "dml://${dml_user}/${project0}" "${dml_user}"
pretty_dml dag checkout "dml://${dml_user}/${project0}#main" "examples/00-hello-world"
pretty_dml status

log "Running example: 01b-load_fn.py"
python "${examples_dir}/01b-load_fn.py"

log "Inspecting fetched and pulled history from the second project"
pretty_dml branch
pretty_dml branch --remote
pretty_dml log --revision HEAD --limit 10
pretty_dml show --revision HEAD
pretty_dml dag get examples/01b-load-fn --revision HEAD
pretty_dml dag get-node old_result --dag examples/01b-load-fn --revision HEAD

log "All examples completed successfully."
