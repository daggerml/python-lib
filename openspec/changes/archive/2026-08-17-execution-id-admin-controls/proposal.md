## Why

Cache invalidation currently begins and propagates through cache keys, so a rebound pointer can replace the historical execution identity being traversed. Users also lack a public way to resolve a cache key to its current execution and terminal DAG identities before selecting an administrative target.

## What Changes

- Add `Dml.cache.describe(cache_key)` to report the exact execution currently named by a cache pointer, its lifecycle, and its reusable terminal DAG when available.
- **BREAKING** Change `Dml.cache.invalidate` and its generated CLI command to accept execution `Ref` identities instead of cache-key strings.
- Make invalidation queue, deduplicate, lock, mark, and traverse execution IDs without substituting identities from cache pointers.
- Treat cache pointers as propagation eligibility checks: an explicit root remains selected, but a historical caller whose pointer no longer names it is pruned without selecting the replacement execution.
- Rename the public `Dml.runtime.cancel` parameter from `index` to `execution` while preserving execution-ID delegation and cancellation modes.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `execution-admin-controls`: Define execution-ID-rooted invalidation and pointer-based caller eligibility without replacement traversal.
- `unified-dml-surface`: Add cache description and change invalidation to exact execution `Ref` inputs.
- `admin-cli-controls`: Expose cache description and execution-ref invalidation through the generated CLI.

## Impact

- Public Python API and generated CLI signatures for cache administration.
- Unified execution-state invalidation traversal and response typing.
- Cache, runtime, CLI, architecture, and user-guide documentation.
- Contract tests for public metadata, CLI generation, cache description races, and invalidation propagation.
