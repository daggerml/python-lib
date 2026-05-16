## ADDED Requirements

### Requirement: Executors SHALL handle `cancel-requested` as an update step
When the runtime invokes an executor with `execution_status = "cancel-requested"`, the executor SHALL treat that invocation as a cancellation update rather than as a fresh launch. Executors that normally dispatch to `runnable.sub` during update SHALL continue to dispatch to `runnable.sub` once in cancellation mode before performing executor-owned cleanup. Executors that do not normally dispatch to `runnable.sub` during update SHALL cancel their own external resources directly.

#### Scenario: Update-dispatch executor forwards cancellation update
- **WHEN** an executor that normally calls `runnable.sub` on update receives `execution_status = "cancel-requested"`
- **THEN** it SHALL issue its normal update-time sub-dispatch once before executor-owned cleanup

#### Scenario: Detached-work executor cancels backend directly
- **WHEN** an executor that does not normally call `runnable.sub` on update receives `execution_status = "cancel-requested"`
- **THEN** it SHALL cancel or tear down its own external work without invoking `runnable.sub`

### Requirement: Executors SHALL tear down external resources during cancellation
Executor-owned cancellation SHALL tear down external resources and SHALL NOT mutate the persisted execution record `state`. Script execution SHALL terminate the supervisor-managed process tree and remove its work directory. Docker execution SHALL stop and remove the container and SHALL remove any temporary loaded image. Batch execution SHALL cancel or terminate the Batch job as appropriate and SHALL deregister the temporary job definition. CloudFormation execution SHALL initiate rollback or cancellation of the stack operation and return without waiting for the rollback to finish. SSH execution SHALL return the nested adapter's cancellation result and SHALL NOT create additional remote wrapper state.

#### Scenario: Batch cancellation tears down Batch resources
- **WHEN** the Batch executor receives `execution_status = "cancel-requested"`
- **THEN** it SHALL cancel or terminate the Batch job and deregister the temporary job definition

#### Scenario: CloudFormation cancellation returns quickly with rollback context
- **WHEN** the CloudFormation executor receives `execution_status = "cancel-requested"`
- **THEN** it SHALL start rollback or cancellation of the stack operation
- **AND** it SHALL return promptly with enough stack context for the caller to identify the affected stack

### Requirement: Successful cancel updates SHALL report `cancelled`
When an executor processes a cancel update without transport or runtime exceptions, it SHALL return `status = "cancelled"` even if backend cleanup or rollback continues asynchronously. The runtime cancellation workflow SHALL treat that result as confirmation that the cancel update was handled rather than as a successful DAG execution result.

#### Scenario: Cancel update reports success after teardown request
- **WHEN** an executor successfully processes a `cancel-requested` update
- **THEN** it SHALL return `status = "cancelled"`
