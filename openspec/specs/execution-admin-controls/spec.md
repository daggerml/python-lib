### Requirement: Manual invalidation SHALL target execution identity
The system SHALL treat cache invalidation as an execution-graph operation. When a user requests invalidation for a cache key, the system SHALL resolve the current execution id from `refs/cache/<cache_key>.json`, compute the reverse caller closure over execution dependencies in a local planning database, and invalidate that execution set.

The invalidation algorithm SHALL operate as follows:

1. Initialize `seen = []`, `seen_set = set()`, and `unseen = set()`.
2. For each user-provided cache key, read `refs/cache/<cache_key>.json`.
3. If that cache ref exists, add its `execution_id` to `unseen`.
4. While `unseen` is not empty:
5. Remove one `exec_id` from `unseen`.
6. Read `exec/state/<exec_id>.json`; if it does not exist, continue.
7. Read `cache_key` from that execution state object.
8. Read `refs/cache/<cache_key>.json`; if it does not exist, continue.
9. If that cache ref points to a different `execution_id`, continue.
10. Append `exec_id` to `seen` and add it to `seen_set`.
11. Read callers of `exec_id` from `exec/edges/<exec_id>/`.
12. Add `(callers - seen_set)` to `unseen`.
13. After `unseen` is empty, iterate `exec_id` through `reversed(seen)`.
14. For each `exec_id`, write `exec/invalidate/<exec_id>.json` with create-once/CAS semantics.
15. Then delete `refs/cache/<cache_key>.json` with compare-and-swap semantics only if it still points to `exec_id`.

#### Scenario: Invalidate starts from current cache ref
- **WHEN** a user invalidates cache key `ck1`
- **THEN** the system SHALL read `refs/cache/ck1.json` to determine the current root execution id before planning propagation

#### Scenario: Historical execution is skipped when cache ref moved
- **WHEN** `exec/state/e1.json` exists but `refs/cache/ck1.json` now points to `e2` instead of `e1`
- **THEN** invalidation SHALL skip `e1`
- **AND** it SHALL NOT add callers of `e1` to the invalidation closure

### Requirement: Invalidation SHALL write execution tombstones and drop affected cache refs
For every execution id in the invalidation closure, the system SHALL write `exec/invalidate/<execution_id>.json` as an immutable control marker containing `execution_id`, `cache_key`, `requested_by`, and `requested_at`. After planning completes, the system SHALL delete every cache ref whose recorded `execution_id` is in that invalidated set.

The invalidate tombstone schema SHALL be:

- `execution_id: str`
- `cache_key: str`
- `requested_by: str`
- `requested_at: int`

#### Scenario: Invalidation writes control markers and removes cache pointers
- **WHEN** the local planner computes invalidation closure `A`
- **THEN** the system SHALL create `exec/invalidate/<execution_id>.json` for every execution in `A`
- **AND** it SHALL delete each `refs/cache/<cache_key>.json` whose stored `execution_id` belongs to `A`

#### Scenario: Cache ref delete is guarded by compare-and-swap
- **WHEN** invalidation reaches commit for execution `e1`
- **AND** `refs/cache/ck1.json` no longer points to `e1`
- **THEN** the system SHALL NOT delete that cache ref

#### Scenario: Invalidation tombstone stores requester metadata
- **WHEN** the system writes `exec/invalidate/e1.json`
- **THEN** that object SHALL contain `execution_id`, `cache_key`, `requested_by`, and `requested_at`

### Requirement: Manual cancellation SHALL support `full` and `drive` runtime modes
The system SHALL expose runtime cancellation in two modes. `mode = "full"` SHALL run Phase 1 planning followed by Phase 2 cleanup. `mode = "drive"` SHALL run Phase 2 cleanup without rerunning Phase 1. Runtime lifecycle transitions remain governed by the distributed cancellation protocol.

`F1(ex0)` SHALL operate as follows:

1. Acquire the execution coordination lock for `ex0` when `ex0` has a lock-bearing `cache_key`.
2. If `ex0` still has active callers, return without mutating `ex0`.
3. Update `exec/state/<ex0>.json` so that `lifecycle = "cancel-requested"`, remove direct caller/callee edges, and move the active argv manifest to the execution-owned cancel target only when the active ref still names `ex0`.
4. Read `spawned_execution_ids` from `exec/state/<ex0>.json`.
5. For each direct child `ex1` in that list, delete the live caller edge `exec/edges/<ex1>/<ex0>.json`.
6. If `exec/edges/<ex1>/` has no remaining caller records after that delete, recurse into `F1(ex1)`.
7. Release the execution coordination lock for `ex0` when one was acquired.

`F2(ex0)` SHALL operate as follows:

1. Mark the local index for `ex0` as `inactive`.
2. Reread `spawned_execution_ids` from `exec/state/<ex0>.json`.
3. Build the direct drive set from those spawned executions whose lifecycle is `cancel-requested`.
4. Repeat until the direct drive set is empty or the F2 timeout is reached.
5. For each `ex1` still in the direct drive set, acquire `ex1`'s execution coordination lock.
6. If `ex1.lifecycle = "cancel-ready"`, invoke the adapter cancellation path for `ex1` and remove `ex1` from the direct drive set.
7. Otherwise, release the lock and leave `ex1` in the direct drive set.
8. After the direct drive loop finishes, update `exec/state/<ex0>.json` so that `lifecycle = "cancel-ready"`.

#### Scenario: Full cancellation marks only the current execution canceled
- **WHEN** `runtime.cancel(idx1, mode="full")` completes
- **THEN** the runtime SHALL have run `F1(idx1)` and `F2(idx1)`
- **AND** it SHALL then persist `exec/state/idx1.json` with `lifecycle = "canceled"`

#### Scenario: Drive mode uses the same F2 driver
- **WHEN** `runtime.cancel(e1, mode="drive")` executes
- **THEN** the runtime SHALL skip `F1(e1)`
- **AND** it SHALL run the same `F2(e1)` direct-child driver used by `mode="full"`
- **AND** it SHALL NOT mark `exec/state/e1.json` `canceled` solely because `mode="drive"` returned

#### Scenario: Shared child is preserved until the last caller edge is removed
- **WHEN** caller `e0` removes `exec/edges/e2/e0.json`
- **AND** another caller edge for `e2` still exists
- **THEN** `F1(e0)` SHALL NOT recurse into `F1(e2)`

#### Scenario: F2 drives only direct spawned children that remain cancel-requested
- **WHEN** `ex0.spawned_execution_ids = [e1, e2, e3]`
- **AND** only `e1` and `e3` have `lifecycle = "cancel-requested"`
- **THEN** `F2(ex0)` SHALL build its direct drive set as `{e1, e3}`

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
