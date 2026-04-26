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
  "remote": {
    "root": "string-or-null"
  }
}
```

Rules:

- top-level keys are `repo`, `branch`, `user`, and `remote`.
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
- `remote.root`: `DML_REMOTE_ROOT`

Rules:

- runtime and tooling docs MUST use these names for environment-based configuration,
- config surfaces that expose resolved values MUST map to the canonical shape in this document.

## Field Constraints

- `branch` default MUST be `main` unless explicitly overridden.
- `remote.root`, when present, MUST be an `s3://bucket` or `s3://bucket/prefix` URI designating the project root.

## Project Config

Git-like project commands store local state under `<project>/.dml/`:

- `.dml/config.toml` contains `[project]`, `[branch]`, and `[remotes.<name>]` tables.
- `.dml/db/` contains the local object database.
- `.dml/.gitignore` contains `*`.

Global project config is loaded from `$DML_CONFIG_HOME/config.toml`, `$XDG_CONFIG_HOME/dml/config.toml`, or `~/.config/dml/config.toml`. It may define `[user].name`, `[defaults].branch`, and ordered `[hooks]` lists for `post-init` and `post-clone`.

The global config home is referred to as `config_home` in CLI and internal project-config APIs.

Project command resolution uses explicit CLI/API values, then `DML_*` environment variables such as `DML_BRANCH`, `DML_REMOTE_BUCKET`, and `DML_REMOTE_PREFIX`, then config-file values.

## Derived Defaults

- when `remote.root` is present, runtime derives the protocol root as `<remote.root>/dml/`.

## References

- [default-dml-runtime.md](default-dml-runtime.md)
- [adapter-execution-contract.md](adapter-execution-contract.md)
- [internal/ops/dml-ops.md](internal/ops/dml-ops.md)
- [remote-data-model.md](remote-data-model.md)
