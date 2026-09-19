#!/usr/bin/env bash

# Set up disposable projects with recent data for local dashboard development.
# This file is intentionally non-executable; invoke it with bash and keep that
# shell open so the fixture remains available.

set -euo pipefail

if (( $# != 0 )); then
  echo "Usage: bash dashboard-ui/scripts/setup-dashboard-demo.sh" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

runner() {
  UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/daggerml-uv-cache}" uv run --dev --all-extras --project "${repo_root}" "$@"
}

cleanup() {
  trap - EXIT INT TERM
  printf '\nStopping dashboard demo fixture...\n'
  runner python "${repo_root}/docs/moto_server_env.py" down --moto-dir "${moto_dir}" >/dev/null 2>&1 || true
  rm -rf "${demo_root}" "${config_home}"
  echo "Removed dashboard demo fixture."
}

demo_root="$(mktemp -d "${TMPDIR:-/tmp}/dml-dashboard-demo.XXXXXX")"
config_home="$(mktemp -d "${TMPDIR:-/tmp}/dml-dashboard-config.XXXXXX")"
moto_dir="${demo_root}/moto"
primary_project="${demo_root}/forecasting"
secondary_project="${demo_root}/model-evaluation"
dashboard_envfile="${demo_root}/dashboard.env"
demo_run_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

envfile="$(runner python "${repo_root}/docs/moto_server_env.py" up \
  --moto-dir "${moto_dir}" \
  --remote-root "s3://daggerml-dashboard-forecasting/artifacts")"
# shellcheck disable=SC1090
source "${envfile}"

# Keep the fixture independent of any DaggerML configuration in the invoking
# shell while retaining the Moto AWS variables loaded above.
while IFS= read -r name; do
  unset "${name}"
done < <(env | awk -F= '/^DML_/ {print $1}')
export DML_CONFIG_HOME="${config_home}"

mkdir -p "${primary_project}" "${secondary_project}"
runner dml init --project-home "${primary_project}" >/dev/null
runner dml init --project-home "${secondary_project}" >/dev/null
DML_PROJECT_HOME="${primary_project}" runner dml config set remote.root \
  "s3://daggerml-dashboard-forecasting/artifacts" >/dev/null

runner python -c 'import boto3; boto3.client("s3").create_bucket(Bucket="daggerml-dashboard-evaluation")'
DML_PROJECT_HOME="${secondary_project}" runner dml config set remote.root \
  "s3://daggerml-dashboard-evaluation/artifacts" >/dev/null

populate_project() {
  local project_home="$1"
  local profile="$2"
  DML_PROJECT_HOME="${project_home}" DML_DASHBOARD_DEMO_RUN_AT="${demo_run_at}" runner python - "${profile}" <<'PY'
import os
import sys
from datetime import datetime, timedelta, timezone

from daggerml import Dml
from daggerml._core import index as index_ops


profiles = {
    "forecasting": [
        ("ingest/weather-stations", "Refresh station observations", "stations", {"rows": 18420, "missing": 17}),
        ("features/daily-weather", "Build daily weather features", "features", ["temperature", "humidity", "wind"]),
        ("models/demand-forecast", "Train weekly demand forecast", "metrics", {"mae": 3.18, "coverage": 0.91}),
        ("reports/weekly-outlook", "Publish weekly outlook", "outlook", {"region": "north", "units": 12840}),
    ],
    "evaluation": [
        ("data/validation", "Validate evaluation dataset", "checks", {"passed": 24, "warnings": 2}),
        ("models/champion", "Evaluate champion model", "metrics", {"accuracy": 0.94, "loss": 0.18}),
        ("models/challenger", "Evaluate challenger model", "metrics", {"accuracy": 0.95, "loss": 0.16}),
        ("release/gate", "Record release decision", "decision", {"approved": True, "threshold": 0.92}),
    ],
}

profile = sys.argv[1]
anchor = datetime.fromisoformat(os.environ["DML_DASHBOARD_DEMO_RUN_AT"].replace("Z", "+00:00")).astimezone(timezone.utc)
dml = Dml()
for position, (name, message, node_name, value) in enumerate(profiles[profile]):
    created = anchor - timedelta(days=6 - position * 2, hours=position + (2 if profile == "evaluation" else 0))
    index_ops.now = lambda created=created: created.isoformat()
    index = dml.runtime.create()
    node = dml.runtime.put_literal(index, value, name=node_name)
    dml.runtime.commit(index, node, name=name, message=message)
PY
}

populate_project "${primary_project}" forecasting
populate_project "${secondary_project}" evaluation

runner python - "${config_home}" "${primary_project}" "${secondary_project}" <<'PY'
import json
import sys
from pathlib import Path

from daggerml.dashboard.config import DashboardProjects


config_home, primary, secondary = map(Path, sys.argv[1:])
(config_home / "config.json").write_text(json.dumps({"project_home": str(primary)}) + "\n", encoding="utf-8")
registry = DashboardProjects(config_home)
registry.register(primary, name="Forecasting research")
registry.register(secondary, name="Model evaluation")
PY

{
  runner python "${repo_root}/docs/moto_server_env.py" print-env --moto-dir "${moto_dir}"
  printf 'export DML_CONFIG_HOME=%q\n' "${config_home}"
} > "${dashboard_envfile}"

printf '\nDashboard demo fixture is ready.\n\n'
printf 'In a second shell, load the temporary environment:\n\n'
printf 'source %q\n' "${dashboard_envfile}"
printf '\nThen start the dashboard:\n\n'
printf 'uv run --dev --all-extras --project %q dml-dashboard --config-home %q --no-open\n' \
  "${repo_root}" "${config_home}"
printf '\nRegistered projects:\n  Forecasting research: %s\n  Model evaluation: %s\n' \
  "${primary_project}" "${secondary_project}"
printf '\nKeep this shell open while using the dashboard. Press Ctrl-C to clean up.\n'

while true; do
  sleep 3600 &
  wait $!
done
