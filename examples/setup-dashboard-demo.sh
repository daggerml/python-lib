#!/usr/bin/env bash

# Set up the local fixture used while iterating on the dashboard. This file is
# intentionally non-executable; invoke it with bash and keep that shell open.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

runner() {
  UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/daggerml-uv-cache}" uv run --project "${repo_root}" "$@"
}

cleanup() {
  trap - EXIT INT TERM
  printf '\nStopping dashboard demo fixture...\n'
  runner python "${repo_root}/examples/moto_server_env.py" down --moto-dir "${moto_dir}" >/dev/null 2>&1 || true
  rm -rf "${demo_root}" "${config_home}"
  echo "Removed dashboard demo fixture."
}

if (( $# != 0 )); then
  echo "Usage: bash examples/setup-dashboard-demo.sh" >&2
  exit 2
fi

demo_root="$(mktemp -d "${TMPDIR:-/tmp}/dml-dashboard-demo.XXXXXX")"
config_home="$(mktemp -d "${TMPDIR:-/tmp}/dml-dashboard-config.XXXXXX")"
moto_dir="${demo_root}/moto"
primary_project="${demo_root}/primary-project"
secondary_project="${demo_root}/secondary-project"
dashboard_envfile="${demo_root}/dashboard.env"
demo_clock_dir="${demo_root}/clock"
demo_run_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# The dashboard's Home calendar reads immutable commit timestamps. Keep the
# disposable fixture useful by distributing its example commits over the week
# leading up to this script run, while leaving live runtime state at the
# current wall-clock time. sitecustomize runs before each example imports DML.
mkdir -p "${demo_clock_dir}"
cat > "${demo_clock_dir}/sitecustomize.py" <<'PY'
"""Give the disposable dashboard fixture a recent, varied commit history."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


anchor_text = os.environ.get("DML_DASHBOARD_DEMO_RUN_AT")
if anchor_text:
    anchor = datetime.fromisoformat(anchor_text.replace("Z", "+00:00")).astimezone(timezone.utc)
    offsets_by_script = {
        "00-hello_world.py": [timedelta(days=6, hours=9)],
        "01b-load_fn.py": [timedelta(days=5, hours=14)],
        "wait_fn.py": [timedelta(days=4, hours=10)],
        "03-dagclass.py": [timedelta(days=3, hours=15)],
        "00-errors.py": [timedelta(days=1, hours=6)],
        "dashboard_secondary_examples.py": [
            timedelta(days=6, hours=4),
            timedelta(days=4, hours=2),
            timedelta(days=2, hours=5),
            timedelta(hours=14),
        ],
    }
    offsets = offsets_by_script.get(Path(sys.argv[0]).name, [])
    if offsets:
        from daggerml._core import index as index_ops

        timestamps = iter((anchor - offset).isoformat() for offset in offsets)

        def fixture_now() -> str:
            return next(timestamps, anchor.isoformat())

        index_ops.now = fixture_now
PY

timed_runner() {
  DML_DASHBOARD_DEMO_RUN_AT="${demo_run_at}" \
    PYTHONPATH="${demo_clock_dir}${PYTHONPATH:+:${PYTHONPATH}}" \
    runner "$@"
}

envfile="$(runner python "${repo_root}/examples/moto_server_env.py" up --moto-dir "${moto_dir}" --remote-root "s3://daggerml-dashboard-primary/artifacts")"
# shellcheck disable=SC1090
source "${envfile}"
while IFS= read -r name; do
  unset "${name}"
done < <(env | awk -F= '/^DML_/ {print $1}')
export DML_CONFIG_HOME="${config_home}"
test "$(env | awk -F= '/^DML_/ {print $1}' | sort | tr '\n' ' ')" = "DML_CONFIG_HOME "

mkdir -p "${primary_project}" "${secondary_project}"
runner dml init --project-home "${primary_project}" >/dev/null
(
  cd "${primary_project}"
  runner dml config set remote.root "s3://daggerml-dashboard-primary/artifacts" >/dev/null
  export DML_REMOTE_ROOT="s3://daggerml-dashboard-primary/artifacts"
  timed_runner bash "${repo_root}/examples/python_examples.sh"
  timed_runner python "${repo_root}/examples/python/04-freeze_dag.py" "examples/python-examples/frozen-inputs"
)

runner python -c 'import boto3; boto3.client("s3").create_bucket(Bucket="daggerml-dashboard-secondary")'
runner dml init --project-home "${secondary_project}" >/dev/null
(
  cd "${secondary_project}"
  runner dml config set remote.root "s3://daggerml-dashboard-secondary/artifacts" >/dev/null
  timed_runner python "${repo_root}/examples/python/dashboard_secondary_examples.py"
)

runner python -c '
import sys
import json
from pathlib import Path
from daggerml.dashboard.config import DashboardProjects
config_home, primary, secondary = map(Path, sys.argv[1:])
(config_home / "config.json").write_text(json.dumps({"project_home": str(primary)}) + "\n")
registry = DashboardProjects(config_home)
registry.register(primary, name="Primary examples")
registry.register(secondary, name="Secondary examples")
' "${config_home}" "${primary_project}" "${secondary_project}"

{
  runner python "${repo_root}/examples/moto_server_env.py" print-env --moto-dir "${moto_dir}"
  printf 'export DML_CONFIG_HOME=%q\n' "${config_home}"
} > "${dashboard_envfile}"

printf '\nDashboard demo fixture is ready.\n\n'
printf 'In a second shell, load the temporary AWS and DaggerML environment:\n\n'
printf 'source %q\n' "${dashboard_envfile}"
printf '\nThen start the dashboard:\n\n'
printf 'uv run --all-extras dml-dashboard --config-home %q --no-open\n' "${config_home}"
printf '\nRegistered projects:\n  Primary examples: %s\n  Secondary examples: %s\n' \
  "${primary_project}" "${secondary_project}"
printf '\nKeep this shell open while using the dashboard. Press Ctrl-C to stop Moto and remove the temporary fixture.\n'

while true; do
  sleep 3600 &
  wait $!
done
