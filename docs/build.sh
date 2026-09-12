#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rm -rf "$root/docs/build-staging"
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

work="$(mktemp -d "${TMPDIR:-/tmp}/daggerml-docs.XXXXXX")"
mkdir -p "$work/workspace"
cleanup() {
  local status=$?
  local teardown_status=0
  DOCS_BUILD_ROOT="$root" DOCS_BUILD_WORK="$work" RETICULATE_PYTHON="$python" DOCS_BUILD_LIB="$root/docs/build-lib.sh" \
    quarto render "$root/docs/build-teardown.qmd" --output-dir "$work/teardown" || teardown_status=$?
  rm -rf "$work"
  if [[ $status -ne 0 || $teardown_status -ne 0 ]]; then
    rm -rf "$root/docs/build-staging"
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
# Quarto excludes README.qmd from project discovery, even with an explicit glob.
shopt -s globstar nullglob
for page in "$work/source"/**/README.qmd; do
  quarto render "$page" --metadata-file "$work/source/_quarto.yml"
  target="$work/render/${page#"$work/source/"}"
  mkdir -p "$(dirname "$target")"
  cp "${page%.qmd}.html" "${target%.qmd}.html"
  if [[ -d "${page%.qmd}_files" ]]; then
    cp -R "${page%.qmd}_files" "$(dirname "$target")/"
  fi
done
"$python" "$root/docs/build.py" stage --render "$work/render" --staging "$root/docs/build-staging"
