# Configuration Model

## Status

specified

## Authority

This document is authoritative for runtime configuration schema, resolution precedence, environment-variable mapping, and configuration naming used by runtime and ops boundaries.

## Purpose

Define the canonical runtime configuration shape resolved by `daggerml._internal.config.DmlConfig` and consumed by API, CLI, and remote-aware helpers.

## Scope

This document defines canonical config keys and shape, resolution precedence, and environment variable inputs.

## Contract References

- Default runtime behavior and `status()` contract: [default-dml-runtime.md](default-dml-runtime.md)
- Internal runtime facade config usage: [internal/ops/dml-ops.md](internal/ops/dml-ops.md)
- Adapter invocation remote field naming: [adapter-execution-contract.md](adapter-execution-contract.md)

## Shared Internal Boundary

`daggerml._internal.config.DmlConfig` is the canonical configuration boundary.

Rules:

- `daggerml.api` and `daggerml._cli` MUST delegate configuration precedence, validation, and derivation to this shared internal resolver.
- frontend-specific config translation layers MUST NOT redefine precedence or naming.
- some API-only behaviors remain unavailable in the CLI when Python object/function serialization cannot be represented cleanly at the command line; those gaps are product-surface limits, not config-model differences.

## Canonical Resolved Config Shape

```json
{
  "project": {
    "home": "string-or-null",
    "uri": "string-or-null",
    "branch": "string"
  },
  "db": {
    "path": "string-or-null"
  },
  "remote": {
    "uri": "string-or-empty"
  },
  "user": "string-or-null",
  "default_branch": "string",
  "hooks": {
    "post-init": ["string"]
  },
  "config_home": "string"
}
```

Rules:

- canonical config parameters are `project.home`, `project.uri`, `db.path`, `remote.uri`, `user`, `default_branch`, `hooks.post-init`, and `config_home`.
- `project.branch` is a helper derived from resolved `project.uri` when a project URI is present; runtime callers MAY also observe the effective branch helper when only branch override inputs are available.
- config key names MUST remain stable across API, CLI, runtime, and ops boundaries.

## Resolution Precedence

Configuration resolution MUST apply in this order:

1. defaults
2. global config
3. project config (`project/runtime` scope only)
4. environment variables
5. explicit runtime arguments or overrides

Rules:

- later layers override earlier layers per key,
- absent higher-precedence values do not erase already-set values.
- `project/runtime` scope resolves `explicit > env > project config > global config > defaults`.
- `global` scope resolves `explicit > env > global config > defaults` and does not require a project config file.

## Environment Variable Mapping

- `project.home`: `DML_PROJECT_HOME`
- `project.uri`: `DML_PROJECT_URI`
- `db.path`: `DML_DB_PATH`
- `remote.uri`: `DML_REMOTE_URI`
- `user`: `DML_USER`
- `default_branch`: `DML_DEFAULT_BRANCH`
- `config_home`: `DML_CONFIG_HOME`

Rules:

- runtime and tooling docs MUST use these names for environment-based configuration,
- config surfaces that expose resolved values MUST map to the canonical shape in this document.

## Field Constraints

- `default_branch` default MUST be `main` unless explicitly overridden.
- `project.uri`, when resolved for project/runtime use, MUST include a branch selector and MUST NOT use a tag selector.
- `remote.uri`, when present, MUST be an `s3://bucket` or `s3://bucket/prefix` URI designating the project root.

## Project Config

Git-like project commands store local state under `<project>/.dml/`:

- `.dml/config.toml` contains `[project]`, `[branch]`, and optional `[remote]` tables.
- `.dml/db/` contains the local object database.
- `.dml/.gitignore` contains `*`.

Global project config is loaded from `$DML_CONFIG_HOME/config.toml`, `$XDG_CONFIG_HOME/dml/config.toml`, or `~/.config/dml/config.toml`. It may define `[user].name`, `[defaults].branch`, and ordered `[hooks]` list for `post-init`.

The global config home is referred to as `config_home` in CLI and internal project-config APIs.

Project command resolution uses the shared internal resolver instead of frontend-specific env/config translation.

## Derived Defaults

- when `db.path` is unset and `project.home` is present in `project/runtime` scope, `db.path` defaults to `<project.home>/.dml/db/`.
- when `project.uri` omits a branch, the resolver appends the effective `default_branch`.
- when `remote.uri` is present, runtime derives the protocol root as `<remote.uri>/dml/`.

## CLI Limitations

The CLI shares the same configuration model as the Python API, but it does not expose API features that require Python object or function serialization.

Examples:

- Python callable staging and execution flows built around in-process values are API-only.
- There is no CLI equivalent for `@api.funkify` or for passing live Python callables/objects into execution.
- contrib helpers that rely on Python function objects, decorators, or direct object serialization do not imply a separate CLI configuration model.
- CLI-safe inputs remain the command-line-representable values exposed by the CLI surface itself, such as strings, numbers, booleans, explicit `namespace:id` ref strings, and other JSON-serializable arguments accepted by the command.

## References

- [default-dml-runtime.md](default-dml-runtime.md)
- [adapter-execution-contract.md](adapter-execution-contract.md)
- [internal/ops/dml-ops.md](internal/ops/dml-ops.md)
- [remote-data-model.md](remote-data-model.md)
