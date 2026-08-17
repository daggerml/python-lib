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
Cancelation planning and driving SHALL acquire each target execution's embedded lock before changing its record. Phase 1 SHALL store cancelation requester and timestamp, transition lifecycle, remove applicable caller edges, and CAS-delete the cache pointer if it still names the execution. Phase 2 SHALL retain the existing leaves-first lifecycle protocol while locking each execution mutation. The persisted field name SHALL be `cancelation`.

#### Scenario: Cancelation removes current cache binding
- **WHEN** Phase 1 marks current execution `e1` cancel-requested
- **THEN** it conditionally deletes `cache/ck1` if that pointer still contains `e1`

#### Scenario: Cancelation uses stored argv
- **WHEN** the runtime invokes cancelation for `e1`
- **THEN** it reads `argv_ref` from `execution/e1`
- **AND** it does not require an active or cancel-target ref

#### Scenario: Drive mutations require ownership
- **WHEN** Phase 2 changes an execution to cancel-ready or canceled
- **THEN** it holds that execution's matching lock owner and uses CAS

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
