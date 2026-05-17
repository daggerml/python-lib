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
    "home": "string-or-null"
  },
  "db": {
    "path": "string-or-null"
  },
  "remote": {
    "project": "string-or-null",
    "root": "string-or-empty",
    "fetch_workers": "positive-integer"
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

- canonical config parameters are `project.home`, `remote.project`, `db.path`, `remote.root`, `remote.fetch_workers`, `user`, `default_branch`, `hooks.post-init`, and `config_home`.
- `remote.project` for local project config is optional branchless project identity only: `dml://<owner>/<project>` when present.
- `remote.root` is the capability gate for remote-backed mutation and execution.
- `remote.project` is the additional capability gate for project-addressed sync such as push, pull, and fetch.
- checkout state is not part of resolved configuration and MUST be read from `.dml/HEAD`.
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
- `remote.project`: `DML_REMOTE_PROJECT`
- `db.path`: `DML_DB_PATH`
- `remote.root`: `DML_REMOTE_ROOT`
- `remote.fetch_workers`: `DML_REMOTE_FETCH_WORKERS`
- `user`: `DML_USER`
- `default_branch`: `DML_DEFAULT_BRANCH`
- `config_home`: `DML_CONFIG_HOME`

Rules:

- runtime and tooling docs MUST use these names for environment-based configuration,
- config surfaces that expose resolved values MUST map to the canonical shape in this document.

## Field Constraints

- `default_branch` default MUST be `main` unless explicitly overridden.
- `remote.project`, when resolved from local project config, MUST NOT include a branch or tag selector.
- local project config MAY omit `remote.project`.
- `remote.root`, when present, MUST be an `s3://bucket` or `s3://bucket/prefix` URI designating the project root.
- `remote.fetch_workers` MUST be a positive integer and defaults to `16`.

## Project Config

Git-like project commands store local state under `<project>/.dml/`:

- `.dml/config.toml` contains a `[remote]` table and optional remote settings.
- `.dml/HEAD` contains the current checkout state as either `ref: refs/local/heads/<branch>` or `commit:<id>`.
- `.dml/db/` contains the local object database.
- `.dml/.gitignore` contains `*`.

Rules:

- init creates `.dml/config.toml` even when neither `remote.root` nor `remote.project` is configured.
- local repos without `remote.root` are read-only at the remote-backed runtime boundary.
- local repos with `remote.root` but without `remote.project` may execute remote-backed runtime flows but cannot use project-addressed sync.

Global project config is loaded from `$DML_CONFIG_HOME/config.toml`, `$XDG_CONFIG_HOME/dml/config.toml`, or `~/.config/dml/config.toml`. It may define `[user].name`, `[defaults].branch`, and ordered `[hooks]` list for `post-init`.

The global config home is referred to as `config_home` in CLI and internal project-config APIs.

Project command resolution uses the shared internal resolver instead of frontend-specific env/config translation.

## Derived Defaults

- when `db.path` is unset and `project.home` is present in `project/runtime` scope, `db.path` defaults to `<project.home>/.dml/db/`.
- `default_branch` is bootstrap and fetch fallback state; it is not the active checkout branch.
- when `remote.root` is present, runtime derives the protocol root as `<remote.root>/dml/`.

## Breaking Change

- Backward compatibility with selector-bearing local `remote.project` values is not supported.
- `DML_BRANCH` is not a supported configuration input.
- Repositories using the old local config format must be rewritten manually.

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
