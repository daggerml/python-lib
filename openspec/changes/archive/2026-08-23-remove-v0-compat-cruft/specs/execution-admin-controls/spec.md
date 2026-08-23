## MODIFIED Requirements

### Requirement: Manual invalidation SHALL target execution identity
Manual invalidation SHALL accept one or more execution identities as explicit roots. It SHALL queue, deduplicate, lock, mark, and compute reverse caller propagation using execution IDs. Cache keys and cache pointers SHALL NOT supply replacement traversal identities.

Each explicit root whose exact split execution record exists SHALL remain selected even when its cache pointer is absent or names a replacement execution. A caller reached through an execution edge SHALL be selected only when its `metadata.json.cache_key` has a current pointer that still names that caller execution. If that pointer is absent or names another execution, the runtime SHALL prune that caller branch without selecting the historical caller, the replacement execution, or callers above the historical caller.

For each selected execution, the runtime SHALL acquire `driver.json.lock`, CAS-delete the cache pointer only if it still contains that execution ID, and then CAS invalidation metadata into `state.json.invalidation`. It SHALL NOT rewrite immutable `metadata.json` or interpret a unified execution object. Cacheless explicit roots SHALL be marked without cache-pointer deletion.

#### Scenario: Explicit root remains selected after pointer rebound
- **WHEN** invalidation explicitly targets execution `e1` and `exec/cache/ck1` now contains `e2`
- **THEN** the runtime SHALL preserve `exec/cache/ck1`
- **AND** it SHALL store invalidation metadata in `exec/execution/e1/state.json`
- **AND** it SHALL NOT select `e2`

#### Scenario: Current caller propagates by execution edge
- **WHEN** caller edge `p1 -> e1` exists and `exec/cache/ck-p` contains `p1`
- **AND** invalidation targets `e1`
- **THEN** the runtime SHALL select `p1` directly by that edge
- **AND** it SHALL NOT rediscover `p1` through its cache key

#### Scenario: Rebound historical caller prunes propagation
- **WHEN** caller edge `p1 -> e1` exists and `exec/cache/ck-p` contains `p2`
- **AND** invalidation targets `e1`
- **THEN** the runtime SHALL NOT invalidate `p1` or `p2`
- **AND** it SHALL NOT traverse callers above `p1`

#### Scenario: Selected execution deletes pointer before marking
- **WHEN** selected execution `e1` is still named by `exec/cache/ck1`
- **THEN** the runtime SHALL conditionally delete `exec/cache/ck1` before storing invalidation metadata in `state.json`

#### Scenario: Cacheless explicit root is marked
- **WHEN** invalidation explicitly targets existing execution `e1` whose `metadata.json` has no cache key
- **THEN** the runtime SHALL store invalidation metadata in `state.json` without attempting cache-pointer deletion

### Requirement: Invalidation SHALL write execution tombstones and drop affected cache refs
Invalidation state SHALL be stored once in exact `state.json.invalidation` as `{requested_by, requested_at}`. Readers SHALL reject an invalidated execution even when an interrupted workflow has not yet deleted its cache pointer. Separate invalidation tombstone objects and unified execution records SHALL NOT be created or read.

#### Scenario: Marked execution is immediately unusable
- **WHEN** `exec/execution/e1/state.json.invalidation` is non-null while `exec/cache/ck1` still contains `e1`
- **THEN** cache lookup treats `ck1` as invalidated
