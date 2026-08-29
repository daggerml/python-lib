## MODIFIED Requirements

### Requirement: Manual cancellation SHALL support `full` and `drive` runtime modes
The system SHALL expose runtime cancellation in two modes. `mode = "full"` SHALL run `F1` planning followed by `F2` driving and then SHALL mark the current execution `canceled`. `mode = "drive"` SHALL run the same `F2` driver without running `F1` and SHALL NOT mark the current execution `canceled`.

`F1(ex0)` SHALL operate as follows:

1. Acquire the execution coordination lock for `ex0` when `ex0` has a lock-bearing `cache_key`.
2. If `ex0` still has active callers, return without mutating `ex0`.
3. Update `exec/state/<ex0>.json` so that `lifecycle = "cancel-pending"`.
4. Read `spawned_execution_ids` from `exec/state/<ex0>.json`.
5. For each direct child `ex1` in that list, delete the live caller edge `exec/edges/<ex1>/<ex0>.json`.
6. If `exec/edges/<ex1>/` has no remaining caller records after that delete, recurse into `F1(ex1)`.
7. Release the execution coordination lock for `ex0` when one was acquired.

`F2(ex0)` SHALL operate as follows:

1. Mark the local index for `ex0` as `inactive`.
2. Reread `spawned_execution_ids` from `exec/state/<ex0>.json`.
3. Build the direct drive set from those spawned executions whose lifecycle is `cancel-pending`.
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

#### Scenario: F2 drives only direct spawned children that remain cancel-pending
- **WHEN** `ex0.spawned_execution_ids = [e1, e2, e3]`
- **AND** only `e1` and `e3` have `lifecycle = "cancel-pending"`
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
