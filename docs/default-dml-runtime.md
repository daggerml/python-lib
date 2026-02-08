# Default Dml Runtime

## Status

specified

## Authority

This document is authoritative for default `Dml` runtime behavior in the public Python API:

- default `Dml` resolution,
- default runtime lifecycle APIs,
- module-level convenience entrypoints that use the default runtime,
- `daggerml.status()` output contract.

If related docs conflict on this scope, this document is the source of truth.


## Purpose

The default `Dml` runtime defines how top-level `daggerml` entrypoints resolve and use an implicit process runtime.


## Scope

This document defines:

- default runtime resolution order,
- `get/set/use/clear` default-runtime APIs,
- `daggerml.new(...)` and `daggerml.load(...)` delegation behavior,
- `daggerml.status()` return contract.

This document does not redefine DAG execution, adapter contracts, or storage protocol contracts.


## Contract References

- API/runtime behavior: [execution-model.md](execution-model.md)
- Configuration resolution and schema: [configuration.md](configuration.md)


## Resolution Model

The default `Dml` instance MUST resolve in this order:

1. active scoped override from `use_default_dml(...)`,
2. process default set by `set_default_dml(...)`,
3. lazily-created implicit default `Dml()` instance.

Rules:

- Scoped override MUST take precedence over process default.
- Implicit default creation MUST occur only when no scoped or process default is set.
- Implicit default MUST be cached as the process default after first creation.


## Runtime API

The `daggerml` top-level module MUST expose:

- `get_default_dml() -> Dml`
- `set_default_dml(dml: Dml) -> None`
- `use_default_dml(dml: Dml)` as a context manager
- `clear_default_dml() -> None`

Rules:

- `set_default_dml` MUST set the process default runtime.
- `use_default_dml` MUST set a scoped override for the active context and MUST restore the previous value on exit.
- `clear_default_dml` MUST remove only the process default; active scoped override behavior is unchanged.
- `get_default_dml` MUST be side-effect free except for implicit default creation when required by the resolution model.


## Scoped Runtime Semantics

Scoped overrides from `use_default_dml(...)` MUST be context-local.

Rules:

- Nested scopes MUST behave as stack discipline (inner scope overrides outer scope; exit restores prior runtime).
- Scoped overrides MUST NOT mutate process-default runtime state.


## Convenience Entrypoints

The `daggerml` top-level module MUST expose:

- `new(...) -> Dag`
- `load(...) -> Dag`

Rules:

- These entrypoints MUST delegate to `get_default_dml()`.
- Signatures MUST mirror corresponding `Dml` methods.
- Delegation MUST preserve behavior of the underlying `Dml` method.


## `daggerml.status()` Contract

The top-level API MUST expose:

- `status() -> dict[str, object]`

`status()` MUST return a vanilla-Python dictionary (nested dictionaries/lists/scalars only) with this shape:

- `default` (dict):
  - `source`: one of `scoped`, `process`, `implicit`,
  - `has_scoped_override`: `bool`,
  - `has_process_default`: `bool`.
- `config` (dict):
  - resolved config values for the active default runtime.
  - MUST include: `repo`, `branch`, `user`, `config_dir`, `remote`.
  - `remote` MUST be a dict containing:
    - `root`,
    - `cache`.
- `runtime` (dict):
  - `ops_initialized`: `bool`,
  - `head_ref`: `str`.

Rules:

- `status()` MUST use `get_default_dml()` resolution semantics.
- `status()` MUST NOT return custom object instances (`Ref`, `Uri`, `Runnable`, dataclasses, or classes).
- `status()` MUST be safe to serialize as JSON without custom encoders.
- `config` payload key naming and shape MUST follow [configuration.md](configuration.md).
- `config.remote.cache` MUST satisfy cache-namespace constraints defined in [remote-data-model.md](remote-data-model.md).


## Boundary to Contrib

Contrib modules MAY read the active default runtime via `get_default_dml()` when needed for lazy materialization behavior.
Contrib modules MUST NOT redefine the default-runtime resolution model.

## Content

See the sections in this document for normative content.

## References

None.
