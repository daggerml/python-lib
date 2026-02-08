# Configuration Model

## Status

specified

## Authority

This document is authoritative for runtime configuration contracts:

- resolved configuration schema and field naming,
- configuration resolution precedence,
- configuration environment-variable key mapping,
- configuration naming used by runtime/status and ops boundaries.

If related docs conflict on these items, this document is the source of truth.


## Purpose

The configuration model defines the canonical runtime configuration shape used by `Dml`, `DmlOps`, and default-runtime status surfaces.


## Scope

This document defines:

- canonical config object keys and nested shape,
- resolution precedence (`defaults < env < explicit`),
- environment-variable names used for config inputs.

This document does not define adapter execution sequencing, remote sync protocol behavior, or repository object schemas.


## Contract References

- Default runtime behavior and `status()` contract: [default-dml-runtime.md](default-dml-runtime.md)
- Internal runtime facade config usage: [internal/ops/dml-ops.md](internal/ops/dml-ops.md)
- Adapter invocation remote field naming: [adapter-execution-contract.md](adapter-execution-contract.md)
- Remote cache namespace constraints: [remote-data-model.md](remote-data-model.md)


## Content

## Canonical Resolved Config Shape

Resolved runtime config MUST use this shape:

```json
{
  "repo": "string-or-null",
  "branch": "string",
  "user": "string-or-null",
  "config_dir": "string-or-null",
  "remote": {
    "root": "string-or-null",
    "cache": "string-or-null"
  }
}
```

Rules:

- top-level keys MUST include exactly: `repo`, `branch`, `user`, `config_dir`, `remote`.
- `remote` MUST be a dictionary with keys `root` and `cache`.
- config key names MUST remain stable across API/runtime/ops boundaries.


## Resolution Precedence

Configuration resolution MUST apply in this order:

1. defaults,
2. environment variables,
3. explicit runtime arguments/overrides.

Rules:

- later layers MUST override earlier layers per key.
- absent values in a higher-precedence layer MUST NOT erase an already-set value.


## Environment Variable Mapping

Canonical environment variable inputs:

- `repo`: `DML_REPO`
- `branch`: `DML_BRANCH`
- `user`: `DML_USER`
- `config_dir`: `DML_CONFIG_DIR`
- `remote.root`: `DML_REMOTE_ROOT`
- `remote.cache`: `DML_REMOTE_CACHE`

Rules:

- runtime and tooling docs MUST use these key names when describing environment-based configuration.
- config surfaces that expose resolved values MUST map to the canonical shape in this document.


## Field Constraints

- `branch` default MUST be `main` unless explicitly overridden.
- `remote.root`, when present, MUST be an `s3://bucket` or `s3://bucket/prefix` URI designating the project root.
- `remote.cache`, when present, MUST satisfy cache-namespace constraints in [remote-data-model.md](remote-data-model.md).


## Derived Defaults

Rules:

- when `remote.root` is present, runtime MUST derive remote protocol root as `<remote.root>/dml/` for remote CAS/refs/transport operations.


## References

- [default-dml-runtime.md](default-dml-runtime.md)
- [adapter-execution-contract.md](adapter-execution-contract.md)
- [internal/ops/dml-ops.md](internal/ops/dml-ops.md)
- [remote-data-model.md](remote-data-model.md)
