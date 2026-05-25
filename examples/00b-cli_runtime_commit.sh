#!/usr/bin/env bash

set -euo pipefail

dag_name="examples/00b-cli-runtime-commit"
user_name="cli-example-user"
tmpdir="$(mktemp -d -t daggerml-cli-example-XXXXXX)"
project_home="${tmpdir}/project"
moto_dir="${tmpdir}/moto"
moto_env_file="${moto_dir}/moto.env"
moto_log_file="${moto_dir}/moto.log"
moto_pid=""
export DML_CONFIG_HOME="${tmpdir}/dml_config"

if command -v python >/dev/null 2>&1; then
  python_cmd=python
else
  python_cmd=python3
fi

json_string() {
  local value
  IFS= read -r value
  value="${value#\"}"
  value="${value%\"}"
  printf '%s\n' "${value}"
}

pretty_json() {
  jq . -C
}

run_dml_capture() {
  local __outvar="$1"
  shift
  local raw
  raw="$(dml --project-home "${project_home}" "$@")"
  printf '%s\n' "${raw}" | pretty_json
  printf -v "${__outvar}" '%s' "$(printf '%s\n' "${raw}" | json_string)"
}

run_dml_stdin_pretty() {
  local stdin_text="$1"
  shift
  printf '%s\n' "${stdin_text}" | dml --project-home "${project_home}" "$@" | pretty_json
}

cleanup() {
  if [[ -n "${moto_pid}" ]]; then
    kill "${moto_pid}" >/dev/null 2>&1 || true
    wait "${moto_pid}" >/dev/null 2>&1 || true
  fi
  rm -rf "${tmpdir}"
}
trap cleanup EXIT

mkdir -p "${project_home}"
mkdir -p "${moto_dir}"
mkdir -p "${DML_CONFIG_HOME}"

"${python_cmd}" - "${moto_env_file}" >"${moto_log_file}" 2>&1 <<'PY' &
from __future__ import annotations

import os
import shlex
import signal
import sys
import time
from pathlib import Path

import boto3
from moto.server import ThreadedMotoServer


def main() -> None:
    env_file = Path(sys.argv[1])
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
        "DML_REMOTE_ROOT": "s3://daggerml-example/artifacts",
    }

    for key, value in env_values.items():
        os.environ[key] = value

    boto3.client("s3", endpoint_url=endpoint).create_bucket(Bucket="daggerml-example")
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
    cat "${moto_log_file}" >&2
    exit 1
  fi
  sleep 0.1
done

if [[ ! -s "${moto_env_file}" ]]; then
  cat "${moto_log_file}" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "${moto_env_file}"

dml_init_args=(--project-home "${project_home}" --user "${user_name}")
if [[ -n "${DML_REMOTE_ROOT:-}" ]]; then
  dml_init_args+=(--remote-root "${DML_REMOTE_ROOT}")
fi

dml init "${dml_init_args[@]}" | pretty_json

run_dml_capture index_id runtime create

raw="$(printf '%s\n' '["dict", {"message": ["scalar", "hello"], "value": ["scalar", 42]}]' | dml --project-home "${project_home}" runtime put-literal "${index_id}" - --name payload)"
printf '%s\n' "${raw}" | pretty_json
payload_node="$(printf '%s\n' "${raw}" | json_string)"

run_dml_stdin_pretty '["list", [["scalar", 1], ["scalar", 2], ["scalar", 3]]]' runtime put-literal "${index_id}" - --name inputs

run_dml_capture commit_ref runtime commit "${index_id}" "${payload_node}" --head main --message "Create CLI example DAG" --dag-name "${dag_name}"

dml --project-home "${project_home}" status | pretty_json
dml --project-home "${project_home}" dag get "${dag_name}" | pretty_json
