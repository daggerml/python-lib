#!/usr/bin/env bash
set -euo pipefail

usage() {
  command_name="$(basename "$0")"
  printf '%s\n' \
    "Usage: $command_name [--auto|--full|--docs-only|--ui-only]" \
    "" \
    "Build the executable documentation and packaged dashboard frontend." \
    "" \
    "Modes:" \
    "  --auto       Rebuild stale components from repository input fingerprints (default)." \
    "  --full       Rebuild documentation and frontend from scratch." \
    "  --docs-only  Rebuild and package documentation without rebuilding the frontend." \
    "  --ui-only    Rebuild the frontend while preserving packaged documentation." \
    "  -h, --help   Show this help message." \
    "" \
    "Use --full after changing untracked build tools or the external build environment."
}

mode="auto"
if [[ $# -gt 1 ]]; then
  usage >&2
  exit 2
fi
if [[ $# -eq 1 ]]; then
  case "$1" in
    --auto) mode="auto" ;;
    --full) mode="full" ;;
    --docs-only) mode="docs" ;;
    --ui-only) mode="ui" ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
staging="$root/docs/build-staging"
static="$root/src/daggerml/dashboard/static"
tools="${DML_DOCS_TOOLS_ROOT:-$root/.tools}"
toolchain="$tools/docs"
micromamba="$tools/bin/micromamba"
toolchain_stamp="$toolchain/.daggerml-toolchain"
build_state="$tools/dashboard-build-state"
toolchain_spec="quarto=1.7.31 r-base=4.4.3 r-knitr=1.49 r-rmarkdown=2.29 r-reticulate=1.40.0 r-xfun=0.49"
python="${DOCS_PYTHON:-$root/.venv/bin/python}"

mkdir -p "$tools/tmp"

fingerprint() {
  {
    git -C "$root" ls-files -co --exclude-standard -- "$@" \
      | LC_ALL=C sort -u \
      | while IFS= read -r path; do
          [[ -f "$root/$path" ]] || continue
          printf '%s\0' "$path"
          git -C "$root" hash-object -- "$path"
        done
  } | git -C "$root" hash-object --stdin
}

state_value() {
  key="$1"
  [[ -f "$build_state" ]] || return 0
  awk -F= -v key="$key" '$1 == key { print substr($0, length(key) + 2); exit }' "$build_state"
}

docs_output_ready() {
  [[ -f "$static/docs/manifest.json" \
    && -d "$static/docs/fragments" \
    && -d "$static/docs/assets" \
    && -d "$static/docs/downloads" ]]
}

ui_output_ready() {
  [[ -f "$static/index.html" && -d "$static/assets" ]]
}

docs_fingerprint="$(fingerprint build-dashboard.sh docs examples src/daggerml pyproject.toml uv.lock)"
ui_fingerprint="$(fingerprint build-dashboard.sh dashboard-ui)"
stored_docs_fingerprint="$(state_value docs)"
stored_ui_fingerprint="$(state_value ui)"
build_docs=false
build_ui=false

case "$mode" in
  auto)
    if [[ "$docs_fingerprint" != "$stored_docs_fingerprint" ]] || ! docs_output_ready; then
      build_docs=true
    fi
    if [[ "$ui_fingerprint" != "$stored_ui_fingerprint" ]] || ! ui_output_ready; then
      build_ui=true
    fi
    ;;
  full)
    build_docs=true
    build_ui=true
    ;;
  docs) build_docs=true ;;
  ui) build_ui=true ;;
esac

if [[ "$build_docs" == false && "$build_ui" == false ]]; then
  printf 'Dashboard documentation and frontend are up to date.\n'
  exit 0
fi

plan=()
[[ "$build_docs" == true ]] && plan+=(documentation)
[[ "$build_ui" == true ]] && plan+=(frontend)
printf 'Build plan (%s): %s\n' "$mode" "${plan[*]}"

if [[ "$build_docs" == true ]]; then
  if [[ ! -x "$python" ]]; then
    printf 'Project Python not found at %s; run uv sync --group dev --all-extras or set DOCS_PYTHON.\n' "$python" >&2
    exit 127
  fi

  case "$(uname -s):$(uname -m)" in
    Darwin:arm64) micromamba_platform="osx-arm64" ;;
    Darwin:x86_64) micromamba_platform="osx-64" ;;
    Linux:aarch64 | Linux:arm64) micromamba_platform="linux-aarch64" ;;
    Linux:x86_64) micromamba_platform="linux-64" ;;
    *)
      printf 'Unsupported documentation build platform: %s %s\n' "$(uname -s)" "$(uname -m)" >&2
      exit 1
      ;;
  esac

  docs_home="$tools/home"
  docs_cache="$tools/cache"
  docs_config="$tools/config"
  docs_mamba="$tools/mamba"
  docs_tmp="$tools/tmp"
  mkdir -p "$tools/bin" "$docs_home" "$docs_cache" "$docs_config" "$docs_mamba/pkgs" "$docs_tmp"

  if [[ ! -x "$micromamba" ]]; then
    if ! command -v curl >/dev/null || ! command -v tar >/dev/null; then
      printf 'Documentation tool bootstrap requires curl and tar.\n' >&2
      exit 127
    fi
    curl --fail --location --silent --show-error \
      "https://micro.mamba.pm/api/micromamba/$micromamba_platform/latest" \
      | tar -xj -C "$tools/bin" --strip-components=1 bin/micromamba
  fi

  current_spec=""
  if [[ -f "$toolchain_stamp" ]]; then
    current_spec="$(<"$toolchain_stamp")"
  fi
  if [[ "$current_spec" != "$toolchain_spec" ]]; then
    read -r -a packages <<< "$toolchain_spec"
    if [[ -d "$toolchain/conda-meta" ]]; then
      action=install
    else
      action=create
    fi
    env \
      HOME="$docs_home" \
      XDG_CACHE_HOME="$docs_cache" \
      XDG_CONFIG_HOME="$docs_config" \
      MAMBA_ROOT_PREFIX="$docs_mamba" \
      CONDA_PKGS_DIRS="$docs_mamba/pkgs" \
      TMPDIR="$docs_tmp" \
      "$micromamba" --no-rc "$action" --yes --prefix "$toolchain" --override-channels --channel conda-forge \
      "${packages[@]}"
    printf '%s\n' "$toolchain_spec" > "$toolchain_stamp"
  fi

  # A failed docs build must not leave a prior staging tree eligible for packaging.
  rm -rf "$staging"
  env \
    HOME="$docs_home" \
    XDG_CACHE_HOME="$docs_cache" \
    XDG_CONFIG_HOME="$docs_config" \
    MAMBA_ROOT_PREFIX="$docs_mamba" \
    CONDA_PKGS_DIRS="$docs_mamba/pkgs" \
    TMPDIR="$docs_tmp" \
    DOCS_PYTHON="$python" \
    QUARTO_PYTHON="$python" \
    PATH="$toolchain/bin:$(dirname "$python"):$PATH" \
    QUARTO_SHARE_PATH="$toolchain/share/quarto" \
    QUARTO_DENO="$toolchain/bin/deno" \
    QUARTO_DENO_DOM="$toolchain/lib/deno_dom.dylib" \
    QUARTO_PANDOC="$toolchain/bin/pandoc" \
    QUARTO_ESBUILD="$toolchain/bin/esbuild" \
    QUARTO_TYPST="$toolchain/bin/typst" \
    QUARTO_DART_SASS="$toolchain/bin/sass" \
    bash "$root/docs/build.sh"

  for path in manifest.json fragments assets downloads; do
    if [[ ! -e "$staging/$path" ]]; then
      printf 'Documentation build did not stage %s.\n' "$path" >&2
      exit 1
    fi
  done
  "$python" -c 'import json, sys; json.load(open(sys.argv[1], encoding="utf-8"))' "$staging/manifest.json"
fi

preserved_docs=""
cleanup() {
  if [[ -n "$preserved_docs" && -d "$preserved_docs" ]]; then
    rm -rf "$preserved_docs"
  fi
}
trap cleanup EXIT

if [[ "$build_ui" == true ]]; then
  if [[ "$build_docs" == false && -d "$static/docs" ]]; then
    preserved_docs="$(mktemp -d "$tools/tmp/dashboard-docs.XXXXXX")"
    mkdir -p "$preserved_docs/docs"
    cp -R "$static/docs/." "$preserved_docs/docs/"
  fi
  (
    cd "$root/dashboard-ui"
    npm run build
  )
  if [[ -n "$preserved_docs" ]]; then
    mkdir -p "$static/docs"
    cp -R "$preserved_docs/docs/." "$static/docs/"
  fi
fi

if [[ "$build_docs" == true ]]; then
  rm -rf "$static/docs"
  mkdir -p "$static/docs"
  cp -R "$staging/." "$static/docs/"
fi

if [[ "$build_docs" == true ]]; then
  stored_docs_fingerprint="$docs_fingerprint"
fi
if [[ "$build_ui" == true ]]; then
  stored_ui_fingerprint="$ui_fingerprint"
fi
printf 'docs=%s\nui=%s\n' "$stored_docs_fingerprint" "$stored_ui_fingerprint" > "$build_state"
