#!/usr/bin/env bash

log() {
  printf '\n*** %s ***\n' "$*" >&2
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    printf 'error: required environment variable %s is not set\n' "${name}" >&2
    exit 1
  fi
}
