## Why

Local repositories currently conflate remote transport configuration with project publication identity. That prevents valid repos from initializing unless they can derive or provide `remote.project`, even though many runtime and mutation flows only require `remote.root`.

## What Changes

- Make local `remote.project` optional while keeping local `remote.root` as the capability gate for remote-backed mutation and execution.
- Change init semantics so `Dml.init()` no longer accepts `name`, accepts optional `remote_project` and optional `remote_root`, and rejects `remote_project` without `remote_root`.
- Restrict project-addressed sync operations such as push, pull, fetch, and init-time checkout/fetch to repositories with configured `remote.project`.
- Preserve recovery/bootstrap behavior so init only fetches or checks out project state when `remote.project` is configured.
- **BREAKING** Remove name-derived init identity flow and replace it with explicit optional `remote_project` configuration.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `shared-internal-configuration`: allow local project config to omit `remote.project` while preserving branchless validation when it is set.
- `init-input-normalization`: remove `name`-based init identity rules and redefine init inputs around optional `remote_project` and `remote_root`.
- `dmlops-init-recovery`: only fetch or pull during init recovery when `remote.project` is configured.
- `required-remote-config`: distinguish operations that require `remote.root` from project sync operations that additionally require `remote.project`.
- `remote-project-refs`: require configured `remote.project` before project-addressed push/pull/fetch/checkout behavior.

## Impact

- Affected code: shared config resolution, local project-config load/save helpers, `Dml.init`, `DmlOps.init`, and project sync operation guards.
- Affected APIs: Python `Dml.init`, CLI `dml init`, and error behavior for project sync commands without `remote.project`.
- Affected docs/specs: configuration, init, and remote sync capability contracts.
