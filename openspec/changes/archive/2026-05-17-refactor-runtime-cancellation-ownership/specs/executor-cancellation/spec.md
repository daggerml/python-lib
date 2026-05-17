## MODIFIED Requirements

### Requirement: Executors SHALL handle `cancel-requested` as an update step
When the runtime invokes an executor with `execution_status = "cancel-pending"`, the executor SHALL treat that invocation as a cancellation update rather than as a fresh launch. Executors that normally dispatch to `runnable.sub` during update SHALL continue to dispatch to `runnable.sub` once in cancellation mode before performing executor-owned cleanup. Executors that do not normally dispatch to `runnable.sub` during update SHALL cancel their own external resources directly.

#### Scenario: Update-dispatch executor forwards cancellation update
- **WHEN** an executor that normally calls `runnable.sub` on update receives `execution_status = "cancel-pending"`
- **THEN** it SHALL issue its normal update-time sub-dispatch once before executor-owned cleanup

#### Scenario: Detached-work executor cancels backend directly
- **WHEN** an executor that does not normally call `runnable.sub` on update receives `execution_status = "cancel-pending"`
- **THEN** it SHALL cancel or tear down its own external work without invoking `runnable.sub`

### Requirement: Successful cancel updates SHALL report `cancelled`
When an executor processes a cancel update without transport or runtime exceptions, it SHALL return `status = "cancel-detached"` even if backend cleanup or rollback continues asynchronously. The runtime cancellation workflow SHALL treat that result as confirmation that the cancel update was handled and ownership was detached rather than as a successful DAG execution result.

#### Scenario: Cancel update reports detached success after teardown request
- **WHEN** an executor successfully processes a `cancel-pending` update
- **THEN** it SHALL return `status = "cancel-detached"`

## ADDED Requirements

### Requirement: Executor cancellation SHALL honor detached completion semantics
Executors SHALL interpret `cancel-detached` as a control-plane completion signal rather than proof that backend cleanup has already finished. Executors that initiate asynchronous backend rollback or shutdown SHALL still return promptly once they have issued the required cancellation work.

#### Scenario: Asynchronous backend rollback still returns detached status
- **WHEN** an executor starts backend rollback or shutdown that continues asynchronously
- **THEN** it SHALL still return `status = "cancel-detached"` after issuing that work successfully
