# Default Dml Runtime

## Status

specified

## Authority

This document is authoritative for default `Dml` runtime behavior in the public Python API.

## Purpose

Define how top-level `daggerml` entrypoints resolve and use an implicit process runtime.

## Scope

This document defines default runtime resolution, `get/set/use/clear` APIs, module-level convenience entrypoints, and `daggerml.status()`.

## Contract References

- API and runtime behavior: [execution-model.md](execution-model.md)
- Configuration resolution and schema: [configuration.md](configuration.md)

## Resolution Model

The default `Dml` instance resolves in this order:

1. active scoped override from `use_default_dml(...)`
2. process default set by `set_default_dml(...)`
3. lazily created implicit default `Dml()` instance

Rules:

- scoped override takes precedence over process default,
- implicit default creation occurs only when no scoped or process default is set,
- the implicit default is cached as the process default after first creation,
- constructing `DmlOps` from the default runtime requires `remote.uri` to be configured.

## Runtime API

The top-level module exposes:

- `get_default_dml() -> Dml`
- `set_default_dml(dml: Dml) -> None`
- `use_default_dml(dml: Dml)` as a context manager
- `clear_default_dml() -> None`

## Convenience Entrypoints

The top-level module exposes:

- `new(...) -> Dag`
- `load(...) -> Dag`

These delegate to `get_default_dml()`.

## `daggerml.status()` Contract

`status() -> dict[str, object]` returns a JSON-serializable dictionary with this shape:

- `default`:
  - `source`: `scoped|process|implicit`
  - `has_scoped_override`: `bool`
  - `has_process_default`: `bool`
- `config`:
  - `project` with `home`, `uri`, and derived `branch`
  - `db` with `path`
  - `remote` with `uri`
  - `user`
  - `default_branch`
  - `hooks` with `post-init`
  - `config_home`
- `runtime`:
  - `ops_initialized`: `bool`
  - `head_ref`: `str`

Rules:

- `status()` uses `get_default_dml()` resolution semantics,
- `status()` MUST NOT return custom object instances,
- `status()` MUST be safe to serialize as JSON,
- `config` key naming and shape MUST follow [configuration.md](configuration.md).

## Boundary to Contrib

Contrib modules MAY read the active default runtime via `get_default_dml()` when needed. They MUST NOT redefine the default-runtime resolution model.

## References

- [configuration.md](configuration.md)
- [execution-model.md](execution-model.md)
