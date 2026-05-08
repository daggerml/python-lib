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

### Requirement: Manual cancellation SHALL target execution identity
The system SHALL treat cancellation as an execution-graph operation keyed by execution id. A user cancellation request SHALL update `exec/state/<execution_id>.json` so that the execution transitions to `cancel-requested` and records `cancel_requested_by`.

The cancellation algorithm SHALL operate as follows:

1. Initialize `seen = []`, `seen_set = set()`, and `unseen = set(user_requested_exec_ids)`.
2. While `unseen` is not empty:
3. Remove one `exec_id` from `unseen`.
4. Read `exec/state/<exec_id>.json`; if it does not exist, continue.
5. If `status` is `succeeded`, `failed`, or `cancelled`, continue.
6. Append `exec_id` to `seen` and add it to `seen_set`.
7. Add `(dependencies - seen_set)` from that state object to `unseen`.
8. After `unseen` is empty, iterate `exec_id` through `reversed(seen)`.
9. For each `exec_id`, reread `exec/state/<exec_id>.json`; if it does not exist, continue.
10. If `status` is `succeeded`, `failed`, or `cancelled`, continue.
11. Count callers of `exec_id` from `exec/edges/<exec_id>/` whose state exists and whose `status` is not `cancel-requested`, `cancelled`, `succeeded`, or `failed`.
12. If that uncancelled caller count is greater than `1`, continue.
13. Otherwise update `exec/state/<exec_id>.json` with compare-and-swap semantics so that `status = "cancel-requested"` and `cancel_requested_by` identifies the requesting user.

#### Scenario: Direct cancellation updates live execution summary
- **WHEN** a user cancels execution `e1`
- **THEN** the system SHALL update `exec/state/e1.json` so that `status = "cancel-requested"`
- **AND** `cancel_requested_by` identifies the requesting user

#### Scenario: Dependency is cancel-requested before caller
- **WHEN** execution `e1` depends on execution `e2`
- **THEN** the cancellation commit phase SHALL process `e2` before `e1`

### Requirement: Cancellation propagation SHALL stop when a callee still has a live caller
The local planner SHALL propagate cancellation only across non-terminal execution dependencies. It SHALL stop recursing when it reaches a terminal execution. Among non-terminal executions in the dependency closure, it SHALL request cancellation only when a candidate execution has no remaining live callers after accounting for callers already included in the cancelling closure.

#### Scenario: Shared dependency is preserved while another caller remains live
- **WHEN** execution `e2` depends on `e3` and a different live execution `e4` also depends on `e3`
- **THEN** cancelling `e2` SHALL NOT require `e3` to be cancelled while `e4` remains a live caller

#### Scenario: Uncancelled caller count greater than one blocks cancellation
- **WHEN** execution `e3` has two uncancelled callers
- **THEN** the cancellation algorithm SHALL skip `e3` for that iteration

#### Scenario: Sole dependency is cancelled recursively
- **WHEN** execution `e2` depends on `e3` and `e2` is the only live caller of `e3`
- **THEN** cancelling `e2` SHALL cause the planner to mark `e3` for cancellation as part of the propagated closure

#### Scenario: Terminal dependency is not cancelled
- **WHEN** execution `e2` depends on execution `e3` and `e3` is already terminal
- **THEN** cancelling `e2` SHALL NOT request cancellation for `e3`
