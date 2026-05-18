## Why

`daggerml._internal.dml` currently reaches the concrete ops classes through two layers of indirection: the `DmlOps` facade and the `_OpsProxy` string-dispatch helper. Those abstractions no longer carry their own value and now add code size, duplicate remote-construction logic, and obscure the actual orchestration boundary.

## What Changes

- Remove the internal `DmlOps` facade and stop treating it as the repository/session boundary.
- Remove `_OpsProxy`, `call_ops_method`, and other string-based dispatch helpers from `daggerml._internal.dml`.
- Have the module-level helper functions in `daggerml._internal.dml` open the DB and instantiate the concrete ops classes directly.
- Preserve the existing public `Dml` and namespace surface exactly as-is; this change does not add new `Dml` methods, properties, or namespaces.
- **BREAKING** Remove internal backward-compatibility import paths and docs that describe `DmlOps` as a supported internal facade.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `unified-dml-surface`: tighten the shared `Dml` orchestration contract so helper logic constructs concrete ops classes directly without adding new caller-facing surface.
- `shared-internal-configuration`: update bootstrap/config-resolution requirements to refer to the shared `Dml` workflow and module-level helper construction rather than `DmlOps`.
- `required-remote-config`: preserve explicit `remote.root` threading while removing the `DmlOps` helper boundary.
- `thin-cli-routing`: replace stale `DmlOps` wording so CLI routing requirements point at the surviving shared orchestration boundary.

## Impact

- Affected code: `src/daggerml/_internal/dml.py`, `src/daggerml/_internal/ops/__init__.py`, and tests/docs that import or describe `DmlOps`.
- Affected contracts: OpenSpec capabilities listed above and docs under `docs/internal/ops/` and `docs/default-dml-runtime.md`.
- APIs: no caller-facing `Dml` API expansion; internal-only breaking removal of `DmlOps` and proxy-based ops construction.
