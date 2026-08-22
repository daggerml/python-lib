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

### Requirement: Manual cancellation SHALL support `full` and `drive` runtime modes
Manual cancellation SHALL execute as two retryable phases. In `full` mode, Phase 1 SHALL determine the complete reachable cancellation set by selecting active executions with no valid caller references, persisting cancellation requester metadata, compare-and-swapping each selected lifecycle to `cancel-pending`, conditionally deleting matching cache pointers, and removing selected executions' outgoing caller edges. Phase 1 SHALL complete without invoking adapters. Phase 2 SHALL invoke cancellation for every selected execution and compare-and-swap each lifecycle directly from `cancel-pending` to `canceled`. In `drive` mode, the runtime SHALL reconstruct and resume cancellation from persisted `cancel-pending` state. Both phases SHALL hold the applicable execution coordination lock for lifecycle mutations and SHALL retry CAS conflicts from fresh state. The persisted field name SHALL remain `cancelation`.

#### Scenario: Cancelation removes current cache binding
- **WHEN** Phase 1 selects current execution `e1` as `cancel-pending`
- **THEN** it SHALL conditionally delete `cache/ck1` if that pointer still contains `e1`

#### Scenario: Cancelation uses stored argv
- **WHEN** Phase 2 invokes cancelation for `e1`
- **THEN** it SHALL read `argv_ref` from `execution/e1`
- **AND** it SHALL not require an active or cancel-target ref

#### Scenario: Full mode separates planning from adapter work
- **WHEN** full cancellation selects multiple reachable executions
- **THEN** it SHALL finish Phase 1 for the complete selected set before invoking any cancel adapter

#### Scenario: Drive mode resumes persisted work
- **WHEN** drive mode starts from an execution whose lifecycle is `cancel-pending`
- **THEN** it SHALL reconstruct selected descendants from execution records
- **AND** it SHALL perform idempotent planning cleanup and Phase 2 adapter work

#### Scenario: Lifecycle changes require ownership and CAS
- **WHEN** either phase changes an execution lifecycle
- **THEN** it SHALL hold that execution's matching coordination owner
- **AND** it SHALL use compare-and-swap against fresh execution state

### Requirement: Mutation-time cancellation rendezvous SHALL happen outside LMDB
When a local index mutation detects a non-active local index lifecycle, the mutation path SHALL release or abort its LMDB transaction before joining runtime cancellation work. A mutation path that sees a non-active local index SHALL use runtime cancellation as the synchronization point rather than requiring caller-managed thread or process handling.

#### Scenario: Inactive mutation gate joins drive mode outside LMDB
- **WHEN** a mutating index workflow sees local index lifecycle `inactive`
- **THEN** it SHALL abort or leave its LMDB transaction before calling `runtime.cancel(..., mode="drive")`
- **AND** it SHALL raise `_core.CancellationError` after that cancellation rendezvous returns

#### Scenario: Terminal local tombstone fails immediately
- **WHEN** a mutating index workflow sees local index lifecycle `canceled`
- **THEN** it SHALL abort or leave its LMDB transaction
- **AND** it SHALL raise `_core.CancellationError` without attempting a new cancellation drive
