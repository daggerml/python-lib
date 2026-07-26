#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<EOF
Usage: $0  [--make-temp] [--run PATTERN] [--help]

Options:
  --make-temp    Make a temporary directory for the moto server environment (instead of using RUNNER_TEMP). The temporary directory will be deleted on exit.
  --run PATTERN  Run only examples matching the given regex pattern (e.g., --run "s3|dynamodb"). If not specified, all examples will be run.
  --help         Show this help message and exit
EOF
}

# parse arguments
make_temp=false
run_pattern=".*"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --help)
      usage
      exit 0
      ;;
    --make-temp)
      make_temp=true
      shift
      ;;
    --run)
      if [[ $# -lt 2 ]]; then
        echo "Error: --run requires a pattern argument"
        usage
        exit 1
      fi
      run_pattern="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

log() {
  echo "[**ALL_EXAMPLES** $(date +'%Y-%m-%d %H:%M:%S')] $*"
}


if [[ "$make_temp" == true ]]; then
  # template includes `dml.` to avoid collisions for clarity
  RUNNER_TEMP="$(mktemp -d -t dml.moto-example-env.XXXXXX)"
  log "Using temporary directory: $RUNNER_TEMP"
  trap 'rm -rf "$RUNNER_TEMP"' EXIT
fi

moto_dir="${RUNNER_TEMP}/moto-example-env"
cleanup() {
  uv run python examples/moto_server_env.py down --moto-dir "$moto_dir"
}
trap cleanup EXIT

envfile="$(uv run python examples/moto_server_env.py up --moto-dir "$moto_dir")"
# shellcheck disable=SC1090
source "$envfile"

for f in examples/*.sh; do
  [[ -x "$f" ]] || continue
  [[ "$f" =~ $run_pattern ]] || continue
  log "Running $f"
  uv run "$f"
done
