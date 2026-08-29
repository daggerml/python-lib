## 1. Execution Guard And Errors

- [x] 1.1 Add `BadExecutionStatusError` and `CanceledExecutionError` and thread them through the execution-state/runtime type surface.
- [x] 1.2 Implement `ExecutionState.require_mutation(execution_id, db, mode="activation" | "mutation")` with lifecycle classification and `cancel(mode="drive")` handling for `cancel-pending`.
- [x] 1.3 Add contract tests for activation vs mutation lifecycle classification, including typed wrong-status and canceled-status failures.

## 2. IndexOps State-Machine Refactor

- [x] 2.1 Refactor execution-aware `IndexOps.create(...)` to use `ExecutionState.require_mutation(..., mode="activation")` before local activation work.
- [x] 2.2 Refactor `put_literal`, `put_import`, `set_node_name`, `start_fn`, and `commit` to use `ExecutionState.require_mutation(..., mode="mutation")` before each write transaction boundary.
- [x] 2.3 Remove `Index.lifecycle` storage and any local-lifecycle mutation checks that are superseded by execution-record gating.

## 3. DML Retry Boundary

- [x] 3.1 Add a reusable retry decorator in `_core/dml.py` for replayable runtime mutation failures.
- [x] 3.2 Apply the retry decorator to runtime mutation entrypoints so retries cover the full orchestration path rather than only inner `IndexOps` calls.
- [x] 3.3 Add focused tests proving retry wraps whole runtime mutation flows instead of only lower-level index operations.

## 4. Verification

- [x] 4.1 Update or replace affected `_core` contract tests to align with execution-record-owned mutation gating and the removed local index lifecycle.
- [x] 4.2 Run the targeted runtime/core test selections needed to verify the new lifecycle guard and retry behavior.
