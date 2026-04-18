# Configuration Model

## Status

specified

## Authority

This document is authoritative for runtime configuration schema, resolution precedence, environment-variable mapping, and configuration naming used by runtime and ops boundaries.

## Purpose

Define the canonical runtime configuration shape used by `Dml`, `DmlOps`, and default-runtime status surfaces.

## Scope

This document defines canonical config keys and shape, resolution precedence, and environment variable inputs.

## Contract References

- Default runtime behavior and `status()` contract: [default-dml-runtime.md](default-dml-runtime.md)
- Internal runtime facade config usage: [internal/ops/dml-ops.md](internal/ops/dml-ops.md)
- Adapter invocation remote field naming: [adapter-execution-contract.md](adapter-execution-contract.md)

## Canonical Resolved Config Shape

```json
{
  "repo": "string-or-null",
  "branch": "string",
  "user": "string-or-null",
  "config_dir": "string-or-null",
  "remote": {
    "root": "string-or-null"
  }
}
```

Rules:

- top-level keys are `repo`, `branch`, `user`, `config_dir`, and `remote`.
- `remote` contains key `root`.
- config key names MUST remain stable across API, runtime, and ops boundaries.

## Resolution Precedence

Configuration resolution MUST apply in this order:

1. defaults
2. environment variables
3. explicit runtime arguments or overrides

Rules:

- later layers override earlier layers per key,
- absent higher-precedence values do not erase already-set values.

## Environment Variable Mapping

- `repo`: `DML_REPO`
- `branch`: `DML_BRANCH`
- `user`: `DML_USER`
- `config_dir`: `DML_CONFIG_DIR`
- `remote.root`: `DML_REMOTE_ROOT`

Rules:

- runtime and tooling docs MUST use these names for environment-based configuration,
- config surfaces that expose resolved values MUST map to the canonical shape in this document.

## Field Constraints

- `branch` default MUST be `main` unless explicitly overridden.
- `remote.root`, when present, MUST be an `s3://bucket` or `s3://bucket/prefix` URI designating the project root.

## Derived Defaults

- when `remote.root` is present, runtime derives the protocol root as `<remote.root>/dml/`.

## References

- [default-dml-runtime.md](default-dml-runtime.md)
- [adapter-execution-contract.md](adapter-execution-contract.md)
- [internal/ops/dml-ops.md](internal/ops/dml-ops.md)
- [remote-data-model.md](remote-data-model.md)
