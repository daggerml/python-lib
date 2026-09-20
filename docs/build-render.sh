#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
staging="${DOCS_BUILD_STAGING:-$root/docs/build-staging}"
rm -rf "$staging"
python="${DOCS_PYTHON:-}"
if [[ -z "$python" ]] && command -v python >/dev/null; then
  python="$(command -v python)"
fi
if [[ -z "$python" ]] && command -v uv >/dev/null; then
  python="$(uv run --dev python -c 'import sys; print(sys.executable)')"
fi

if [[ -z "$python" ]]; then
  printf '%s\n' 'Documentation build requires Python; set DOCS_PYTHON to the project interpreter.' >&2
  exit 127
fi
if ! command -v quarto >/dev/null; then
  printf '%s\n' 'Documentation build requires Quarto 1.7.31; see docs/build-tooling.md.' >&2
  exit 127
fi
if [[ "$(quarto --version)" != "1.7.31" ]]; then
  printf '%s\n' 'Documentation build requires Quarto 1.7.31; see docs/build-tooling.md.' >&2
  exit 1
fi
if ! command -v R >/dev/null; then
  printf '%s\n' 'Documentation build requires R 4.4.3 with knitr, rmarkdown, and reticulate; see docs/build-tooling.md.' >&2
  exit 127
fi
if ! Rscript --vanilla "$root/docs/build-requirements.R"; then
  printf '%s\n' 'Documentation build requires the pinned R packages; see docs/build-tooling.md.' >&2
  exit 1
fi
if ! "$python" -c 'import ipykernel, jupyter' >/dev/null 2>&1; then
  printf '%s\n' 'Documentation build requires Jupyter in the project development environment; run uv sync --group dev --all-extras.' >&2
  exit 127
fi
export QUARTO_PYTHON="$python"

work="$(mktemp -d "${TMPDIR:-/tmp}/daggerml-docs.XXXXXX")"
mkdir -p "$work/workspace"
cleanup() {
  local status=$?
  local teardown_status=0
  DOCS_BUILD_ROOT="$root" DOCS_BUILD_WORK="$work" RETICULATE_PYTHON="$python" DOCS_BUILD_LIB="$root/docs/build-lib.sh" \
    quarto render "$root/docs/build-teardown.qmd" --output-dir "$work/teardown" || teardown_status=$?
  rm -rf "$work"
  if [[ $status -ne 0 || $teardown_status -ne 0 ]]; then
    rm -rf "$staging"
  fi
  if [[ $status -eq 0 && $teardown_status -ne 0 ]]; then
    status=$teardown_status
  fi
  exit "$status"
}
trap cleanup EXIT

export DOCS_BUILD_ROOT="$root" DOCS_BUILD_WORK="$work" DOCS_WORKSPACE_ROOT="$work/workspace" RETICULATE_PYTHON="$("$python" -c 'import sys; print(sys.executable)')" DOCS_BUILD_LIB="$root/docs/build-lib.sh"
export PATH="$(dirname "$RETICULATE_PYTHON"):$PATH"
# Do not inherit developer projects, credentials, profiles, or service endpoints.
for name in ${!DML_@} ${!AWS_@}; do
  unset "$name"
done
export DML_CONFIG_HOME="$work/config" XDG_CONFIG_HOME="$work/config"
export AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_EC2_METADATA_DISABLED=true
"$python" "$root/docs/build.py" validate
quarto render "$root/docs/build-bootstrap.qmd" --output-dir "$work/bootstrap"
source "$work/fixture.env"
"$python" "$root/docs/build.py" prepare --work "$work/source"
export DOCS_SOURCE_ROOT="$work/source"
quarto render "$work/source" --output-dir "$work/render"
"$python" "$root/docs/build.py" stage --render "$work/render" --staging "$staging"
if [[ -n "${DOCS_SITE_OUTPUT:-}" ]]; then
  "$python" "$root/docs/build.py" site --render "$work/render" --output "$DOCS_SITE_OUTPUT"
fi
