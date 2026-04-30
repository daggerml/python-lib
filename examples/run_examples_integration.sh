#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="${repo_root}/examples"
ignore_dir="${repo_root}/ignore"
scratch_dir="${ignore_dir}/scratch"
moto_dir="${ignore_dir}/.integration-moto-$(date +%s)-$$"
moto_env_file="${moto_dir}/moto.env"
moto_log_file="${moto_dir}/moto.log"
moto_pid=""

log() {
    echo
    echo "*** $* ***"
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
    remote_uri = os.environ.get("DML_EXAMPLE_REMOTE_URI", "s3://daggerml-example/artifacts")
    parsed = urlparse(remote_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise RuntimeError(f"DML_EXAMPLE_REMOTE_URI must be s3://bucket[/prefix], got: {remote_uri!r}")
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
        "DML_REMOTE_URI": remote_uri,
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
log "Remote URI: ${DML_REMOTE_URI}"

log "Setting up DML repo in ${ignore_dir}"
mkdir -p "${scratch_dir}" || true
printf '*\n' > "${ignore_dir}/.gitignore"
cd "${scratch_dir}"

project_name="project-0"
log "Initializing DML repo in ${project_name}"
# mkdir "${scratch_dir}/${project_name}"
dml init $project_name
cd "${scratch_dir}/${project_name}"

if [[ ! -d "./.dml/db" ]]; then
  log "dml init did not create .dml/db" >&2
  exit 1
fi

cat .dml/config.toml

log "DML repo initialized. Current status:"
dml status | jq .

log "Running example: 00-hello_world.py"
python "${examples_dir}/00-hello_world.py"

log "Listing DML cache after running examples:"
dml cache list

log "Running example: 01-docker_dataset.py"
python "${examples_dir}/01-docker_dataset.py"

log "Running example: 02-ssh_docker_dataset.py"
python "${examples_dir}/02-ssh_docker_dataset.py"

log "All examples completed successfully."
