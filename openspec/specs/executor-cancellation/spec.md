### Requirement: Executors SHALL handle runtime cancel invocation as a synchronous cancellation step
When the runtime invokes an executor through the cancellation path for a direct child execution, the executor SHALL treat that invocation as synchronous cancellation work. The executor cancel contract SHALL remain separate from execution-record-only lifecycle states such as `cancel-ready`.

#### Scenario: Executor cancel invocation happens after runtime readiness gating
- **WHEN** the runtime invokes executor cancellation for child execution `e1`
- **THEN** the runtime SHALL already have observed `exec/state/e1.json` at `lifecycle = "cancel-ready"`

#### Scenario: Leaf executor tears down directly
- **WHEN** an executor has no nested runtime work to cancel
- **THEN** it SHALL tear down its own external resources and return without recursive cancellation

### Requirement: Nested adapter chains SHALL recurse through runtime cancel at most once per child execution
Executor stacks that wrap nested executors SHALL ensure that only one layer in the stack calls `Dml.runtime.cancel(child)` for a given child execution while handling one cancel update.

#### Scenario: Wrapper chain avoids duplicate recursive cancellation
- **WHEN** multiple executor layers participate in cancelling the same nested execution
- **THEN** at most one layer SHALL call `Dml.runtime.cancel(child)` for that child execution during that cancel update

### Requirement: Executors SHALL tear down external resources during cancellation
Executor-owned cancellation SHALL tear down external resources and SHALL NOT mutate the persisted execution record `state`. Script execution SHALL terminate the supervisor-managed process tree and remove its work directory. Docker execution SHALL stop and remove the container and SHALL remove any temporary loaded image. Batch execution SHALL cancel or terminate the Batch job as appropriate and SHALL deregister the temporary job definition. CloudFormation execution SHALL initiate rollback or cancellation of the stack operation and return without waiting for the rollback to finish. SSH execution SHALL return the nested adapter's cancellation result and SHALL NOT create additional remote wrapper state.

#### Scenario: Batch cancellation tears down Batch resources
- **WHEN** the Batch executor receives `execution_status = "cancel-pending"`
- **THEN** it SHALL cancel or terminate the Batch job and deregister the temporary job definition

#### Scenario: CloudFormation cancellation returns quickly with rollback context
- **WHEN** the CloudFormation executor receives `execution_status = "cancel-pending"`
- **THEN** it SHALL start rollback or cancellation of the stack operation
- **AND** it SHALL return promptly with enough stack context for the caller to identify the affected stack

### Requirement: Cancel-path return values SHALL remain advisory only
Executors SHALL return a success or failure indication from cancel handling, but the runtime SHALL continue to own execution-record lifecycle persistence, including `cancel-ready` and `canceled`.

#### Scenario: Executor return does not own cancel-ready or canceled persistence
- **WHEN** an executor returns from one cancel-path invocation
- **THEN** that return SHALL NOT itself define or persist execution-record lifecycle values such as `cancel-ready` or `canceled`
