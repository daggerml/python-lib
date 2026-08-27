## Why

Fresh terminal executions can return their result without driving executor cleanup, so a funk called only once can leave external resources behind indefinitely. Cached and fresh terminal result paths need the same cleanup-before-return behavior.

## What Changes

- Define cleanup as required while `driver.cleanup` is null and a result is available.
- Give required, eligible cleanup one coordinated adapter call before returning either a cached or freshly established terminal result.
- Persist cleanup success, failure, retry state, and retry timing without delaying or invalidating result delivery.
- Reuse the current driver lock when terminal invoke handling proceeds directly to cleanup.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `runtime-execution-records`: Require cached and fresh terminal result paths to drive required, eligible cleanup before returning the result.

## Impact

- Runtime coordination in `src/daggerml/_core/exec_state.py`.
- Execution coordination contract tests, including the existing fresh-success cleanup xfail.
- Runtime and extension lifecycle documentation.
- No adapter payload, response schema, executor interface, or dependency changes.
