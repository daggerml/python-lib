#!/usr/bin/env bash

docs_bootstrap() {
  : "${DOCS_BUILD_ROOT:?DOCS_BUILD_ROOT is required}"
  : "${DOCS_BUILD_WORK:?DOCS_BUILD_WORK is required}"
  command -v moto_server >/dev/null || {
    printf '%s\n' 'docs fixtures require moto_server; install moto[server] in the documentation Python environment' >&2
    return 1
  }
  mkdir -p "$DOCS_BUILD_WORK/moto" "$DOCS_BUILD_WORK/pages"
  local envfile
  envfile="$("$RETICULATE_PYTHON" "$DOCS_BUILD_ROOT/docs/moto_server_env.py" up --moto-dir "$DOCS_BUILD_WORK/moto" --remote-root "s3://daggerml-docs/artifacts")"
  test -s "$envfile"
  cp "$envfile" "$DOCS_BUILD_WORK/fixture.env"
}

docs_teardown() {
  : "${DOCS_BUILD_ROOT:?DOCS_BUILD_ROOT is required}"
  : "${DOCS_BUILD_WORK:?DOCS_BUILD_WORK is required}"
  "$RETICULATE_PYTHON" "$DOCS_BUILD_ROOT/docs/moto_server_env.py" down --moto-dir "$DOCS_BUILD_WORK/moto"
}
