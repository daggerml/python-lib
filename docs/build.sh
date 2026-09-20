#!/usr/bin/env bash
set -euo pipefail

usage() {
  command_name="$(basename "$0")"
  cat <<EOF
Usage: $command_name [OPTIONS]

Prepare dependencies, verify the frontend, and build the executable
documentation and packaged dashboard frontend.

Host prerequisites:
  Python, Node.js/npm, uv, Git, curl, tar, and native build tools must already
  be installed. This script installs repository Python and npm dependencies and
  bootstraps the pinned Quarto/R documentation toolchain under .tools/.

Environment:
  DOCS_SITE_OUTPUT    Also export standalone HTML and a generated API reference
                      to this absolute directory.
                      Forces documentation rendering; incompatible with --no-docs.

Default stages:
  1. Synchronize Python dependencies with uv sync --group dev --all-extras.
  2. Install locked frontend dependencies with npm ci.
  3. Run frontend tests with npm test.
  4. Rebuild stale executable documentation and frontend output.
  5. Assemble and validate a complete candidate dashboard, then replace the
     packaged static tree without exposing partial output.

Options:
  --auto             Rebuild stale selected outputs from input fingerprints
                     (default).
  --full             Force every selected output to rebuild.
  --no-python-sync   Use the existing Python environment without running uv.
  --no-npm-ci        Use existing node_modules without running npm ci.
  --no-ui-test       Skip frontend tests.
  --no-docs          Preserve packaged documentation instead of rebuilding it.
  --no-ui            Preserve the packaged frontend instead of rebuilding it.
  -h, --help         Show this help without changing dependencies or outputs.

Skipped setup stages trust the existing environment. A selected downstream
stage fails if that environment is unusable. Skipped output stages require a
complete packaged component to preserve. --full forces only selected outputs;
it never re-enables a disabled stage. Generated components are built outside
the package tree and installed only after the complete candidate validates.

Examples:
  # Complete clean CI/release build
  bash docs/build.sh

  # Fast local rebuild with existing dependency environments
  bash docs/build.sh --no-python-sync --no-npm-ci

  # Documentation only
  bash docs/build.sh --no-npm-ci --no-ui-test --no-ui

  # Frontend only, preserving packaged docs
  bash docs/build.sh --no-python-sync --no-docs
EOF
}

mode="auto"
sync_python=true
install_ui=true
test_ui=true
select_docs=true
select_ui=true
for option in "$@"; do
  case "$option" in
    --auto) mode="auto" ;;
    --full) mode="full" ;;
    --no-python-sync) sync_python=false ;;
    --no-npm-ci) install_ui=false ;;
    --no-ui-test) test_ui=false ;;
    --no-docs) select_docs=false ;;
    --no-ui) select_ui=false ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown option: %s\n\n' "$option" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -n "${DOCS_SITE_OUTPUT:-}" ]]; then
  if [[ "$select_docs" == false || "$DOCS_SITE_OUTPUT" != /* ]]; then
    printf 'DOCS_SITE_OUTPUT requires an absolute output path and enabled documentation.\n' >&2
    exit 2
  fi
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
static="$root/src/daggerml/dashboard/static"
static_parent="$(dirname "$static")"
tools="${DML_DOCS_TOOLS_ROOT:-$root/.tools}"
toolchain="$tools/docs"
micromamba="$tools/bin/micromamba"
toolchain_stamp="$toolchain/.daggerml-toolchain"
build_state="$tools/dashboard-build-state"
toolchain_spec="quarto=1.7.31 r-base=4.4.3 r-knitr=1.49 r-rmarkdown=2.29 r-reticulate=1.40.0 r-xfun=0.49"
python="${DOCS_PYTHON:-$root/.venv/bin/python}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Dashboard build requires host-provided %s.\n' "$1" >&2
    exit 127
  fi
}

require_command git
if [[ "$sync_python" == true ]]; then require_command uv; fi
if [[ "$install_ui" == true || "$test_ui" == true || "$select_ui" == true ]]; then
  require_command node
  require_command npm
fi
if [[ "$select_docs" == true ]]; then
  require_command curl
  require_command tar
fi

docs_output_ready() {
  local output="${1:-$static}"
  [[ -f "$output/docs/manifest.json" && -d "$output/docs/fragments" && -d "$output/docs/assets" ]]
}

ui_output_ready() {
  local output="${1:-$static}"
  [[ -f "$output/index.html" && -d "$output/assets" ]]
}

if [[ "$select_docs" == false ]] && ! docs_output_ready; then
  printf 'Cannot disable documentation: packaged documentation is incomplete at %s.\n' "$static/docs" >&2
  exit 1
fi
if [[ "$select_ui" == false ]] && ! ui_output_ready; then
  printf 'Cannot disable the frontend: packaged frontend is incomplete at %s.\n' "$static" >&2
  exit 1
fi

if [[ "$sync_python" == true ]]; then
  (cd "$root" && uv sync --group dev --all-extras)
fi
if [[ ! -x "$python" ]]; then
  printf 'Project Python not found at %s; enable Python sync or set DOCS_PYTHON.\n' "$python" >&2
  exit 127
fi
if [[ "$install_ui" == true ]]; then
  (cd "$root/dashboard-ui" && npm ci)
fi
if [[ "$test_ui" == true ]]; then
  (cd "$root/dashboard-ui" && npm test)
fi

mkdir -p "$tools/tmp" "$static_parent"
work="$(mktemp -d "$tools/tmp/dashboard-build.XXXXXX")"
candidate=""
backup=""
installed_candidate=false
had_static=false
cleanup() {
  local status=$?
  trap - EXIT
  if [[ $status -ne 0 && "$installed_candidate" == true ]]; then
    rm -rf "$static"
    if [[ "$had_static" == true && -n "$backup" && -d "$backup" ]]; then
      mv "$backup" "$static"
      backup=""
    fi
  fi
  [[ -n "$candidate" && -d "$candidate" ]] && rm -rf "$candidate"
  [[ -n "$backup" && -d "$backup" ]] && rm -rf "$backup"
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT

fingerprint() {
  { git -C "$root" ls-files -co --exclude-standard -- "$@" | LC_ALL=C sort -u | while IFS= read -r path; do
    [[ -f "$root/$path" ]] || continue
    printf '%s\0' "$path"; git -C "$root" hash-object -- "$path"
  done; } | git -C "$root" hash-object --stdin
}

state_value() {
  local key="$1"
  [[ -f "$build_state" ]] || return 0
  awk -F= -v key="$key" '$1 == key { print substr($0, length(key) + 2); exit }' "$build_state"
}

docs_fingerprint="$(fingerprint docs src/daggerml pyproject.toml uv.lock)"
ui_fingerprint="$(fingerprint docs/build.sh dashboard-ui)"
stored_docs_fingerprint="$(state_value docs)"
stored_ui_fingerprint="$(state_value ui)"
build_docs=false
build_ui=false
if [[ "$select_docs" == true ]] && { [[ "$mode" == "full" || -n "${DOCS_SITE_OUTPUT:-}" ]] || [[ "$docs_fingerprint" != "$stored_docs_fingerprint" ]] || ! docs_output_ready; }; then
  build_docs=true
fi
if [[ "$select_ui" == true ]] && { [[ "$mode" == "full" ]] || [[ "$ui_fingerprint" != "$stored_ui_fingerprint" ]] || ! ui_output_ready; }; then
  build_ui=true
fi

plan=()
[[ "$sync_python" == true ]] && plan+=(python-sync)
[[ "$install_ui" == true ]] && plan+=(npm-ci)
[[ "$test_ui" == true ]] && plan+=(ui-test)
[[ "$build_docs" == true ]] && plan+=(documentation)
[[ "$build_ui" == true ]] && plan+=(frontend)
printf 'Build plan (%s): %s\n' "$mode" "${plan[*]:-validate-existing}"

docs_stage="$work/docs"
ui_stage="$work/ui"
if [[ "$build_docs" == true ]]; then
  case "$(uname -s):$(uname -m)" in
    Darwin:arm64) micromamba_platform="osx-arm64" ;; Darwin:x86_64) micromamba_platform="osx-64" ;;
    Linux:aarch64|Linux:arm64) micromamba_platform="linux-aarch64" ;; Linux:x86_64) micromamba_platform="linux-64" ;;
    *) printf 'Unsupported documentation build platform: %s %s\n' "$(uname -s)" "$(uname -m)" >&2; exit 1 ;;
  esac
  docs_home="$tools/home"; docs_cache="$tools/cache"; docs_config="$tools/config"; docs_mamba="$tools/mamba"; docs_tmp="$tools/tmp"
  mkdir -p "$tools/bin" "$docs_home" "$docs_cache" "$docs_config" "$docs_mamba/pkgs" "$docs_tmp"
  if [[ ! -x "$micromamba" ]]; then
    curl --fail --location --silent --show-error "https://micro.mamba.pm/api/micromamba/$micromamba_platform/latest" | tar -xj -C "$tools/bin" --strip-components=1 bin/micromamba
  fi
  current_spec=""; [[ -f "$toolchain_stamp" ]] && current_spec="$(<"$toolchain_stamp")"
  if [[ "$current_spec" != "$toolchain_spec" ]]; then
    read -r -a packages <<< "$toolchain_spec"; [[ -d "$toolchain/conda-meta" ]] && action=install || action=create
    env HOME="$docs_home" XDG_CACHE_HOME="$docs_cache" XDG_CONFIG_HOME="$docs_config" MAMBA_ROOT_PREFIX="$docs_mamba" CONDA_PKGS_DIRS="$docs_mamba/pkgs" TMPDIR="$docs_tmp" "$micromamba" --no-rc "$action" --yes --prefix "$toolchain" --override-channels --channel conda-forge "${packages[@]}"
    printf '%s\n' "$toolchain_spec" > "$toolchain_stamp"
  fi
  env HOME="$docs_home" XDG_CACHE_HOME="$docs_cache" XDG_CONFIG_HOME="$docs_config" MAMBA_ROOT_PREFIX="$docs_mamba" CONDA_PKGS_DIRS="$docs_mamba/pkgs" TMPDIR="$docs_tmp" DOCS_BUILD_STAGING="$docs_stage" DOCS_PYTHON="$python" QUARTO_PYTHON="$python" PATH="$toolchain/bin:$(dirname "$python"):$PATH" QUARTO_SHARE_PATH="$toolchain/share/quarto" QUARTO_DENO="$toolchain/bin/deno" QUARTO_DENO_DOM="$toolchain/lib/deno_dom.dylib" QUARTO_PANDOC="$toolchain/bin/pandoc" QUARTO_ESBUILD="$toolchain/bin/esbuild" QUARTO_TYPST="$toolchain/bin/typst" QUARTO_DART_SASS="$toolchain/bin/sass" bash "$root/docs/build-render.sh"
  for path in manifest.json fragments assets; do [[ -e "$docs_stage/$path" ]] || { printf 'Documentation build did not stage %s.\n' "$path" >&2; exit 1; }; done
  "$python" -c 'import json, sys; json.load(open(sys.argv[1], encoding="utf-8"))' "$docs_stage/manifest.json"
fi

if [[ "$build_ui" == true ]]; then
  (cd "$root/dashboard-ui" && npm run build -- --outDir "$ui_stage")
  ui_output_ready "$ui_stage" || { printf 'Frontend build output is incomplete at %s.\n' "$ui_stage" >&2; exit 1; }
fi

if [[ "$build_docs" == false && "$build_ui" == false ]]; then
  docs_output_ready || { printf 'Packaged documentation is incomplete at %s.\n' "$static/docs" >&2; exit 1; }
  ui_output_ready || { printf 'Packaged frontend is incomplete at %s.\n' "$static" >&2; exit 1; }
  "$python" -c 'import json, sys; json.load(open(sys.argv[1], encoding="utf-8"))' "$static/docs/manifest.json"
  printf 'Dashboard documentation and frontend are up to date.\n'
  exit 0
fi

candidate="$(mktemp -d "$static_parent/.static-candidate.XXXXXX")"
if [[ "$build_ui" == true ]]; then
  cp -R "$ui_stage/." "$candidate/"
else
  cp -R "$static/." "$candidate/"
  rm -rf "$candidate/docs"
fi
if [[ "$build_docs" == true ]]; then
  mkdir -p "$candidate/docs"
  cp -R "$docs_stage/." "$candidate/docs/"
else
  mkdir -p "$candidate/docs"
  cp -R "$static/docs/." "$candidate/docs/"
fi

ui_output_ready "$candidate" || { printf 'Candidate frontend is incomplete.\n' >&2; exit 1; }
docs_output_ready "$candidate" || { printf 'Candidate documentation is incomplete.\n' >&2; exit 1; }
"$python" -c 'import json, sys; json.load(open(sys.argv[1], encoding="utf-8"))' "$candidate/docs/manifest.json"

backup="$static_parent/.static-backup.$$"
if [[ -e "$static" ]]; then
  had_static=true
  mv "$static" "$backup"
fi
if ! mv "$candidate" "$static"; then
  if [[ "$had_static" == true && -d "$backup" ]]; then mv "$backup" "$static"; backup=""; fi
  exit 1
fi
candidate=""
installed_candidate=true
if [[ "$build_docs" == true ]]; then stored_docs_fingerprint="$docs_fingerprint"; fi
if [[ "$build_ui" == true ]]; then stored_ui_fingerprint="$ui_fingerprint"; fi
state_candidate="$work/dashboard-build-state"
printf 'docs=%s\nui=%s\n' "$stored_docs_fingerprint" "$stored_ui_fingerprint" > "$state_candidate"
mv "$state_candidate" "$build_state"
if [[ "$had_static" == true ]]; then rm -rf "$backup"; backup=""; fi
installed_candidate=false
