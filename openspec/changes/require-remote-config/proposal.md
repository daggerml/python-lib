## Why

Remote-backed operations now assume a configured remote root, but parts of the codebase and tests still model remote arguments and config as optional. That mismatch obscures the current contract, complicates types, and leaves dead fallback paths in APIs that should require explicit remote configuration.

## What Changes

- **BREAKING** Make remote configuration required in runtime and ops surfaces that depend on remote-backed behavior.
- Remove `Optional`, `| None`, and `None` defaults from remote-root and remote-config parameters used by remote-aware components.
- Update constructors, helpers, and tests to always pass explicit remote configuration where remote-backed behavior is exercised.
- Remove unsupported code paths that model missing remote configuration for components that always require it.
- Keep non-remote code paths using non-remote primitives where only local transaction access is needed.

## Capabilities

### New Capabilities
- `required-remote-config`: Define the contract that remote-aware runtime and ops components require explicit remote configuration rather than optional remote arguments.

### Modified Capabilities
- `execution-state`: Tighten the existing remote-root requirement so remote-backed execution helpers are modeled consistently as always requiring a valid remote root.

## Impact

- Affected code: `src/daggerml/_internal/ops/*`, `src/daggerml/_internal/exec_state.py`, runtime/config call sites, and tests/helpers that construct remote-aware ops.
- Affected APIs: constructors and helpers for remote-aware components now require explicit remote config values.
- Affected systems: remote execution, remote cache operations, and any adapter/test helper that bootstraps remote-aware ops.
