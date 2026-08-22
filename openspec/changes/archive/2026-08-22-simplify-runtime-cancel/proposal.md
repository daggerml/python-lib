## Why

Runtime cancellation exposes separate `full` and `drive` modes even though persisted `cancel-pending` state already makes the workflow resumable. Phase 2 also accepts failed adapter cancellation as terminal, preventing `cancel()` from guaranteeing that selected work was canceled successfully.

## What Changes

- **BREAKING** Remove the `mode` parameter from runtime and DAG cancellation APIs.
- Add a bounded `max_retries` parameter to `cancel()`.
- Always run the two cancellation phases: reconstruct and complete the persisted cancellation plan, then drive adapter cancellation.
- Attempt cancellation of the selected executions concurrently and retry only executions that remain unsuccessful.
- Populate `driver.not_before` from cancellation `retry_after_ms`, respect that deadline before subsequent calls, and serialize each adapter invocation with the execution lock.
- Transition an execution from `cancel-pending` to `canceled` only after successful cancellation; leave exhausted work resumable as `cancel-pending` and surface failure.
- Remove obsolete mode branches and cancellation summary categories rather than retaining compatibility machinery.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `unified-dml-surface`: Replace mode selection with one bounded-retry cancellation API.
- `execution-admin-controls`: Define one resumable two-phase manual cancellation operation.
- `execution-state`: Require concurrent, success-gated Phase 2 retries.
- `adapter-operation-protocol`: Define cancel success, retry, and failure outcomes used by the retry driver.
- `executor-cancellation`: Make successful executor cancellation the condition for terminal cancellation state.
- `runtime-execution-records`: Align persisted driver deadlines and terminal lifecycle writes with successful cancellation retries.

## Impact

This affects `Dml.runtime.cancel`, `Dag.cancel`, `ExecutionState.cancel`, cancel adapter response handling, generated CLI arguments, cancellation tests, and runtime, adapter, and executor documentation. The implementation should become significantly smaller by deleting public mode dispatch, driver-only entry paths, and advisory-result summary buckets.
