#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
staging="$root/docs/build-staging"
static="$root/src/daggerml/dashboard/static"
tools="${DML_DOCS_TOOLS_ROOT:-$root/.tools}"
toolchain="$tools/docs"
micromamba="$tools/bin/micromamba"
stamp="$toolchain/.daggerml-toolchain"
toolchain_spec="quarto=1.7.31 r-base=4.4.3 r-knitr=1.49 r-rmarkdown=2.29 r-reticulate=1.40.0 r-xfun=0.49"

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

mkdir -p "$tools/bin" "$tools/home" "$tools/cache" "$tools/config" "$tools/mamba/pkgs" "$tools/tmp"
export HOME="$tools/home"
export XDG_CACHE_HOME="$tools/cache"
export XDG_CONFIG_HOME="$tools/config"
export MAMBA_ROOT_PREFIX="$tools/mamba"
export CONDA_PKGS_DIRS="$tools/mamba/pkgs"
export TMPDIR="$tools/tmp"

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
if [[ -f "$stamp" ]]; then
  current_spec="$(<"$stamp")"
fi
if [[ "$current_spec" != "$toolchain_spec" ]]; then
  read -r -a packages <<< "$toolchain_spec"
  if [[ -d "$toolchain/conda-meta" ]]; then
    action=install
  else
    action=create
  fi
  "$micromamba" --no-rc "$action" --yes --prefix "$toolchain" --override-channels --channel conda-forge \
    "${packages[@]}"
  printf '%s\n' "$toolchain_spec" > "$stamp"
fi

python="${DOCS_PYTHON:-$root/.venv/bin/python}"
if [[ ! -x "$python" ]]; then
  printf 'Project Python not found at %s; run uv sync --group dev --all-extras or set DOCS_PYTHON.\n' "$python" >&2
  exit 127
fi
export DOCS_PYTHON="$python"
export PATH="$toolchain/bin:$(dirname "$python"):$PATH"
# Set relocatable Quarto paths directly instead of relying on shell activation.
export QUARTO_SHARE_PATH="$toolchain/share/quarto"
export QUARTO_DENO="$toolchain/bin/deno"
export QUARTO_DENO_DOM="$toolchain/lib/deno_dom.dylib"
export QUARTO_PANDOC="$toolchain/bin/pandoc"
export QUARTO_ESBUILD="$toolchain/bin/esbuild"
export QUARTO_TYPST="$toolchain/bin/typst"
export QUARTO_DART_SASS="$toolchain/bin/sass"

# A failed docs build must not leave a prior staging tree eligible for packaging.
rm -rf "$staging"
bash "$root/docs/build.sh"

for path in manifest.json fragments assets downloads; do
  if [[ ! -e "$staging/$path" ]]; then
    printf 'Documentation build did not stage %s.\n' "$path" >&2
    exit 1
  fi
done
"$python" -c 'import json, sys; json.load(open(sys.argv[1], encoding="utf-8"))' "$staging/manifest.json"

(
  cd "$root/dashboard-ui"
  npm run build
)

mkdir -p "$static/docs"
cp -R "$staging/." "$static/docs/"
