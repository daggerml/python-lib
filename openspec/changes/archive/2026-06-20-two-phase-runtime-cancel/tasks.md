## 1. Runtime Cancel Modes

- [x] 1.1 Add `mode="full" | "drive"` to `Dml.runtime.cancel` and thread it through the core runtime cancellation path.
- [x] 1.2 Refactor runtime cancellation into explicit `F1` and `F2` helpers with shared `F2` behavior.
- [x] 1.3 Ensure only `mode="full"` marks the current execution `canceled` after `F2`.

## 2. Execution-State Lifecycle

- [x] 2.1 Extend execution-record lifecycle handling to include `cancel-ready`.
- [x] 2.2 Implement `F1` edge-removal recursion and `cancel-pending` writes under the relevant execution locks.
- [x] 2.3 Implement `F2` drive-set rereads from `spawned_execution_ids`, direct-child `cancel-ready` dispatch, and post-drive `cancel-ready` writes.
- [x] 2.4 Preserve `child_execution_ids` as non-canceled lineage only.

## 3. Local Index Tombstones

- [x] 3.1 Add local `Index.lifecycle` with `active`, `inactive`, and `canceled`.
- [x] 3.2 Set local index lifecycle to `inactive` when `F2` begins and to `canceled` when local cancellation reaches terminal state.
- [x] 3.3 Export `_core.CancellationError` and use it as the mutation-gate failure surface.

## 4. Mutation Gates

- [x] 4.1 Guard `put_literal`, `put_import`, `set_node_name`, `start_fn`, and `commit` inside their LMDB transactions.
- [x] 4.2 When a gate sees `inactive`, abort LMDB work, run `cancel(mode="drive")` outside LMDB, then raise `CancellationError`.
- [x] 4.3 When a gate sees `canceled`, abort LMDB work and raise `CancellationError` immediately.
- [x] 4.4 Recheck local index lifecycle at the start of every transaction in multi-transaction mutation flows.

## 5. Verification

- [x] 5.1 Add or update execution-state tests for `cancel-ready`, F1 recursion, and shared-child preservation.
- [x] 5.2 Add or update mutation-race tests for local index tombstones and inactive-gate rendezvous behavior.
- [x] 5.3 Run the targeted runtime cancellation test suites.
