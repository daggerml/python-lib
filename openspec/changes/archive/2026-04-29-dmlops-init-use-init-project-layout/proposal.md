## Why

`DmlOps.init` currently duplicates project-layout setup behavior that already exists in `init_project_layout`, which increases maintenance cost and makes init recovery logic harder to reason about.
Consolidating init-time layout creation through the shared helper reduces drift risk while preserving the current external init contract.

## What Changes

- Refactor `DmlOps.init` to call `init_project_layout` for `.dml` directory/bootstrap config setup.
- Preserve the current `DmlOps.init` interface, return payload, validation behavior, and recovery flow.
- Remove now-unused helper code and duplicated layout-writing paths that become obsolete after delegation.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `shared-internal-configuration`: Clarify that init layout/bootstrap file creation is delegated to shared internal project-layout helpers while preserving existing init semantics.

## Impact

- Affected code: `src/daggerml/_internal/ops/__init__.py`, `src/daggerml/_internal/config.py`, and init-focused internal tests.
- API/CLI contract impact: none expected; init inputs/outputs and error semantics remain unchanged.
- Dependencies/systems: no new dependencies; this is an internal refactor and dead-code cleanup.
