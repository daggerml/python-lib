## 1. Update Cancellation Coordination

- [x] 1.1 Change `IndexOps.cancel` and its helper flow to mark the synthetic index execution record `cancel-requested` before any graph traversal or caller counting begins.
- [x] 1.2 Change cancellation planning to traverse the full rooted execution graph, collect caller-callee edges, and derive `candidate_set` plus `own_executions` for the current cancellation run.
- [x] 1.3 Implement the retry loop so each candidate attempts a short-lived cache-key lock, rechecks global active callers under lock, and returns `None`, `-1`, or `+1` to drive set updates.
- [x] 1.4 Keep the cancelled-index marker and synthetic index record in sync by setting the index status to `cancelled` only after the full rooted graph has been cancelled successfully.
- [x] 1.5 Make `Dml.runtime.cancel` own the retry loop, emit diagnostics for each pass, and return a structured cancellation statistics object.

## 2. Align Runtime Locking Semantics

- [x] 2.1 Refactor any cancellation helpers that currently construct `ExecutionState` from `execution_id` so execution coordination locks are always acquired by `cache_key`.
- [x] 2.2 Preserve execution-id-based dependency traversal and adapter cancel updates while ensuring short-lived cache-key lock acquisition does not break missing-record paths or already-cancelled per-execution fast paths.
- [x] 2.3 Recompute active callers from the global reverse-edge records in S3 under lock before marking `cancel-requested` or invoking adapter cancellation.
- [x] 2.4 Ensure per-execution `cancelled` means "no more adapter calls for this execution" without treating it as permission to prune descendant traversal.

## 3. Verify Behavior

- [x] 3.1 Update contract tests for cancellation to assert full rooted-graph traversal, `candidate_set`/`own_executions` initialization, global caller ownership, and index-root `cancel-requested` seeding.
- [x] 3.2 Add or adjust tests covering per-execution `cancelled` semantics so already-cancelled executions skip adapter work but still do not prune descendant traversal.
- [x] 3.3 Add or adjust tests covering loop outcomes (`None`, `-1`, `+1`), lock-contention retry, and active-caller rechecks under lock.
- [x] 3.4 Add or adjust tests covering retry/failure behavior so a failed cancellation sweep leaves the cancelled-index marker and does not prematurely mark the synthetic index root `cancelled`.
- [x] 3.5 Add or adjust tests covering `Dml.runtime.cancel` diagnostics and the returned cancellation statistics schema and counters.
- [x] 3.6 Run the relevant cancellation and execution contract test suites and confirm they pass.

## 4. Known Limitations

- [x] 4.1 Document that unreachable remote-only bespoke adapter chains can leave `Dml.runtime.cancel` retrying until user interruption, with `cancel-requested` as the only current propagation signal.
