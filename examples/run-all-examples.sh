#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 [--make-temp] [--use-existing-environment] [--run PATTERN] [--help]

Options:
  --make-temp                 Make a temporary directory for the moto server environment (instead of using RUNNER_TEMP). The temporary directory will be deleted on exit.
  --use-existing-environment  Use the current DML project and existing Moto/AWS environment instead of creating them.
  --run PATTERN               Run only examples matching the given regex pattern (e.g., --run "s3|dynamodb"). If not specified, all examples will be run.
  --help                      Show this help message and exit
EOF
}

# parse arguments
make_temp=false
use_existing_environment=false
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
    --use-existing-environment)
      use_existing_environment=true
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

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="${repo_root}/examples"
scratch_dirs=()

setup_example() {
  local example_name="$1"
  local scratch_dir="${repo_root}/ignore/examples/${example_name}"
  example_project_home="${scratch_dir}/project"

  scratch_dirs+=("${scratch_dir}")
  export DML_CONFIG_HOME="${scratch_dir}/dml_config"
  export DML_EXAMPLE_PROJECT_HOME="${example_project_home}"
  export DML_EXAMPLE_SCRATCH="${scratch_dir}"
  rm -rf "${scratch_dir}"
  mkdir -p "${example_project_home}" "${DML_CONFIG_HOME}"
  (
    cd "${example_project_home}"
    if [[ "${example_name}" == "push_load" || "${example_name}" == "bash_full_cli_workflow" ]]; then
      uv run dml --remote-project "dml://cool-guy/${example_name}-managed" init > /dev/null
    else
      uv run dml init > /dev/null
    fi
  )
}

setup_existing_environment() {
  for name in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION AWS_ENDPOINT_URL DML_REMOTE_ROOT; do
    if [[ -z "${!name:-}" ]]; then
      echo "Error: --use-existing-environment requires ${name}" >&2
      exit 1
    fi
  done
  if [[ ! -d .dml ]]; then
    echo "Error: --use-existing-environment must run from an initialized DML project" >&2
    exit 1
  fi

  example_project_home="$(pwd)"
  existing_scratch="$(mktemp -d -t dml.existing-example-env.XXXXXX)"
  scratch_dirs+=("${existing_scratch}")
  export DML_EXAMPLE_PROJECT_HOME="${example_project_home}"
}

setup_existing_example() {
  local example_name="$1"
  export DML_EXAMPLE_SCRATCH="${existing_scratch}/${example_name}"
  mkdir -p "${DML_EXAMPLE_SCRATCH}"
}

can_run_example() {
  local example_name="$1"

  if [[ "${example_name}" != "docker_and_ssh" ]]; then
    return 0
  fi
  if ! docker info > /dev/null 2>&1; then
    log "Docker does not seem to be available, skipping docker examples"
    return 1
  fi
  if ! command -v ssh > /dev/null 2>&1; then
    log "SSH client does not seem to be available, skipping SSH examples"
    return 1
  fi
  return 0
}

cleanup() {
  local status=$?
  if [[ "${use_existing_environment}" == false ]]; then
    uv run python "${examples_dir}/moto_server_env.py" down --moto-dir "${moto_dir}"
  fi
  if [[ "${KEEP_EXAMPLE_SCRATCH:-0}" != "1" ]]; then
    rm -rf "${scratch_dirs[@]}"
  fi
  if [[ "$make_temp" == true ]]; then
    rm -rf "${RUNNER_TEMP}"
  fi
  return "${status}"
}

if [[ "${make_temp}" == true && "${use_existing_environment}" == true ]]; then
  echo "Error: --make-temp cannot be used with --use-existing-environment" >&2
  exit 1
fi

if [[ "$make_temp" == true ]]; then
  # template includes `dml.` to avoid collisions for clarity
  RUNNER_TEMP="$(mktemp -d -t dml.moto-example-env.XXXXXX)"
  log "Using temporary directory: $RUNNER_TEMP"
fi

trap cleanup EXIT

if [[ "${use_existing_environment}" == true ]]; then
  setup_existing_environment
else
  moto_dir="${RUNNER_TEMP}/moto-example-env"
  envfile="$(uv run python "${examples_dir}/moto_server_env.py" up --moto-dir "$moto_dir")"
  # shellcheck disable=SC1090
  source "$envfile"
fi

for f in "${examples_dir}"/*.sh; do
  [[ -x "$f" ]] || continue
  example_name="$(basename "$f" .sh)"
  relative_f="examples/$(basename "$f")"
  [[ "$relative_f" =~ $run_pattern ]] || continue
  can_run_example "${example_name}" || continue
  if [[ "${use_existing_environment}" == true ]]; then
    setup_existing_example "${example_name}"
  else
    setup_example "${example_name}"
  fi
  log "Running $f"
  (
    cd "${example_project_home}"
    uv run "$f"
  )
done
