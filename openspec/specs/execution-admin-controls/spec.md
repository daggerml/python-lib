## Purpose
Define manual invalidation and cancelation controls for runtime executions.

## Requirements

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

### Requirement: Invalidation SHALL write execution tombstones and drop affected cache refs
Invalidation state SHALL be stored once in the unified execution record as `{requested_by, requested_at}`. Readers SHALL reject an invalidated execution even when an interrupted workflow has not yet deleted its cache pointer. Separate invalidation tombstone objects SHALL NOT be created.

#### Scenario: Marked execution is immediately unusable
- **WHEN** `execution/e1` contains invalidation metadata while `cache/ck1` still contains `e1`
- **THEN** cache lookup treats `ck1` as invalidated

### Requirement: Manual cancellation SHALL run one resumable two-phase workflow
Manual cancellation SHALL first reconstruct and complete the reachable `cancel-pending` set without invoking adapters, then drive every selected execution to successful cancellation with bounded retries. Every invocation SHALL resume persisted `cancel-pending` work without a separate drive mode, and the persisted field name SHALL remain `cancelation`.

#### Scenario: Planning precedes adapter work
- **WHEN** cancellation selects multiple reachable executions
- **THEN** it SHALL finish Phase 1 for the complete selected set before invoking any cancel adapter

#### Scenario: Repeated call resumes persisted work
- **WHEN** cancellation starts from an execution already in `cancel-pending`
- **THEN** it SHALL reconstruct its selected descendants and run the same two phases

#### Scenario: Exhausted work remains resumable
- **WHEN** Phase 2 exhausts retries for an execution
- **THEN** that execution SHALL remain `cancel-pending` for a later cancellation call

### Requirement: Mutation-time cancellation rendezvous SHALL happen outside LMDB
When a local index mutation detects a non-active local index lifecycle, the mutation path SHALL release or abort its LMDB transaction before joining the single runtime cancellation workflow. A mutation path that sees a non-active local index SHALL use runtime cancellation as the synchronization point rather than requiring caller-managed thread or process handling.

#### Scenario: Inactive mutation gate joins drive mode outside LMDB
- **WHEN** a mutating index workflow sees local index lifecycle `inactive`
- **THEN** it SHALL abort or leave its LMDB transaction before calling `runtime.cancel(...)`
- **AND** it SHALL raise `_core.CancellationError` after that cancellation rendezvous returns

#### Scenario: Terminal local tombstone fails immediately
- **WHEN** a mutating index workflow sees local index lifecycle `canceled`
- **THEN** it SHALL abort or leave its LMDB transaction
- **AND** it SHALL raise `_core.CancellationError` without attempting cancellation again
