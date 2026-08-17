## MODIFIED Requirements

### Requirement: Manual invalidation SHALL target execution identity
Manual invalidation SHALL accept one or more execution identities as explicit roots. It SHALL queue, deduplicate, lock, mark, and compute reverse caller propagation using execution IDs. Cache keys and cache pointers SHALL NOT supply replacement traversal identities.

Each explicit root whose execution record exists SHALL remain selected even when its cache pointer is absent or names a replacement execution. A caller reached through an execution edge SHALL be selected only when its execution record has a cache key whose current pointer still names that caller execution. If that pointer is absent or names another execution, the runtime SHALL prune that caller branch without selecting the historical caller, the replacement execution, or callers above the historical caller.

For each selected execution, the runtime SHALL acquire its embedded lock, CAS-delete the cache pointer only if it still contains that execution ID, and then CAS immutable invalidation metadata into the selected execution record. Cacheless explicit roots SHALL be marked without cache-pointer deletion.

#### Scenario: Explicit root remains selected after pointer rebound
- **WHEN** invalidation explicitly targets execution `e1` and `cache/ck1` now contains `e2`
- **THEN** the runtime SHALL preserve `cache/ck1`
- **AND** it SHALL store invalidation metadata in `execution/e1`
- **AND** it SHALL NOT select `e2`

#### Scenario: Current caller propagates by execution edge
- **WHEN** caller edge `p1 -> e1` exists and `cache/ck-p` contains `p1`
- **AND** invalidation targets `e1`
- **THEN** the runtime SHALL select `p1` directly by that edge
- **AND** it SHALL NOT rediscover `p1` through its cache key

#### Scenario: Rebound historical caller prunes propagation
- **WHEN** caller edge `p1 -> e1` exists and `cache/ck-p` contains `p2`
- **AND** invalidation targets `e1`
- **THEN** the runtime SHALL NOT invalidate `p1` or `p2`
- **AND** it SHALL NOT traverse callers above `p1`

#### Scenario: Selected execution deletes pointer before marking
- **WHEN** selected execution `e1` is still named by `cache/ck1`
- **THEN** the runtime SHALL conditionally delete `cache/ck1` before storing invalidation metadata in `execution/e1`

#### Scenario: Cacheless explicit root is marked
- **WHEN** invalidation explicitly targets existing execution `e1` whose record has no cache key
- **THEN** the runtime SHALL store invalidation metadata in `execution/e1` without attempting cache-pointer deletion
