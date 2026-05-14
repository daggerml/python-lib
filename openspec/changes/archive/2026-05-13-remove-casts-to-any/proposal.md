## Why

The codebase currently relies on `cast(Any, ...)` in a handful of runtime and test paths to silence the type checker instead of expressing the real types. That makes the type surface harder to trust, hides legitimate typing mistakes, and has already spread into core contrib and execution-state code.

## What Changes

- Remove all current `cast(..., Any)` usages from runtime and test code.
- Leave the surrounding runtime logic unchanged unless removing the cast exposes a real typing or test issue that must be fixed locally.
- Update affected tests so they exercise the same behavior without routing values through `cast(..., Any)`.

## Capabilities

### New Capabilities
- `cast-free-authoring-and-tests`: Contrib authoring helpers and tests no longer contain `cast(..., Any)` no-ops.

### Modified Capabilities
- `runtime-execution-records`: Execution record construction and merge logic use the concrete runtime status type directly instead of erasing it through `Any`.

## Impact

- Affected code: `src/daggerml/contrib/api.py`, `src/daggerml/_internal/ops/index.py`, `src/daggerml/_internal/exec_state.py`, and tests covering contrib and configuration contracts.
- Affected systems: contrib dagclass compilation/run helpers, runtime execution-record persistence, and type-checked test coverage.
- No intended runtime behavior changes; this is a direct code cleanup.
