## Why

The `dml init` inputs currently force users to provide a project name even when they already have a canonical project URI, and they do not clearly enforce a single source of truth between name and URI. This makes initialization ergonomics and validation behavior inconsistent with project identity expectations.

## What Changes

- Make `name` optional in `init` so callers can initialize from explicit URI-only inputs.
- Make `name` and `remote_project` mutually exclusive at the `init` contract boundary.
- When `name` is provided, derive `remote_project` from `name` plus resolved global config user.
- Raise a descriptive repository/config error when `name` is provided but global config user is unresolved.
- Keep existing behavior for explicit `remote_project` initialization paths.

## Capabilities

### New Capabilities
- `init-input-normalization`: Normalize and validate `init` identity inputs so exactly one identity source is used and derived URI behavior is deterministic.

### Modified Capabilities
- None.

## Impact

- Affected code: init CLI handler and `DmlOps.init` identity validation/derivation paths.
- Affected APIs: initialization argument semantics (`name` optional, `name`/`remote_project` exclusivity).
- Error behavior: clearer user-facing failure when user identity cannot be resolved for name-based initialization.
