## 1. Cancellation Flow

- [x] 1.1 Move runtime cancellation orchestration into `IndexOps.cancel` and reduce `Dml.runtime.cancel` to a thin public entrypoint.
- [x] 1.2 Unify index-root and execution cancellation around one internal execution-id flow.
- [x] 1.3 Clear `active/<cache_key>` under lock before cancellation fan-out.
- [x] 1.4 Replace detached cancellation completion with terminal `cancelled`.

## 2. Concurrent Child Processing

- [x] 2.1 Process direct `spawned_execution_ids` with a thread pool.
- [x] 2.2 Remove each direct caller edge before invoking the child adapter stack.
- [x] 2.3 Wait for the worker pool before finalizing the caller as `cancelled`.

## 3. Adapter Contract

- [x] 3.1 Update executor cancel handlers to treat cancellation as synchronous.
- [x] 3.2 Require each adapter chain handling one child cancel to own execution and cancellation of that child job, and to delegate to `Dml.runtime.cancel(child)` at most once for that child execution when nested runtime cancellation is needed.
- [x] 3.3 Ignore adapter cancel return values for lifecycle state until a stronger contract is introduced.

## 4. State And Edge Contracts

- [x] 4.1 Update lifecycle/status validation to remove `cancel-detached` and keep `cancel-pending` plus `cancelled`.
- [x] 4.2 Update `cancellation_requested_by` handling to store the immediate requester identity: top-level user or immediate parent execution id.
- [x] 4.3 Update edge semantics and tests so persisted edges are treated as live caller relationships and cancelled relationships are removed mechanically.

## 5. Tests And Docs

- [x] 5.1 Rewrite runtime/index cancellation contract tests for synchronous completion and concurrent child processing.
- [x] 5.2 Update executor cancellation tests for runtime-owned state mutation and nested cancel delegation.
- [x] 5.3 Update runtime docs to describe the new cancellation model.
