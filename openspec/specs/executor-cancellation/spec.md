## Purpose
Define executor responsibilities and retry safety during runtime cancelation.

## Requirements

### Requirement: Executors SHALL handle runtime cancel invocation as a synchronous cancellation step
When the runtime invokes an executor through an `AdapterCancelRequest` for a `cancel-pending` execution, the executor SHALL treat that invocation as synchronous cancellation work for the identified execution attempt. The executor cancel contract SHALL remain separate from execution-record lifecycle ownership.

#### Scenario: Executor cancel invocation uses a cancel request
- **WHEN** Phase 2 invokes executor cancellation for selected execution `e1`
- **THEN** the executor receives `AdapterCancelRequest` data for `e1`
- **AND** the executor does not receive a mixed invoke envelope with cancellation fields

#### Scenario: Leaf executor tears down directly
- **WHEN** an executor has no nested runtime work to cancel
- **THEN** it SHALL tear down its own external resources and return without recursive cancellation

### Requirement: Nested adapter chains SHALL recurse through runtime cancel at most once per child execution
Executor stacks that wrap nested executors SHALL ensure that only one layer in the stack calls `Dml.runtime.cancel(child)` for a given child execution while handling one cancel update.

#### Scenario: Wrapper chain avoids duplicate recursive cancellation
- **WHEN** multiple executor layers participate in cancelling the same nested execution
- **THEN** at most one layer SHALL call `Dml.runtime.cancel(child)` for that child execution during that cancel update

### Requirement: Executors SHALL tear down external resources during cancellation
Executor-owned cancellation SHALL tear down external resources and SHALL NOT mutate the persisted execution record `state`. Script execution SHALL terminate the supervisor-managed process tree and remove its work directory. Docker execution SHALL stop and remove the container and SHALL remove any temporary loaded image. Batch execution SHALL cancel or terminate the Batch job as appropriate and SHALL deregister the temporary job definition. SSH execution SHALL return the nested adapter's cancellation result and SHALL NOT create additional remote wrapper state.

#### Scenario: Batch cancellation tears down Batch resources
- **WHEN** the Batch executor receives an `AdapterCancelRequest`
- **THEN** it SHALL cancel or terminate the Batch job and deregister the temporary job definition

### Requirement: Successful executor cancellation SHALL gate terminal cancellation
Executors SHALL return `cancelled` only after their synchronous cancellation step succeeds. The runtime SHALL remain responsible for lifecycle persistence and SHALL transition `cancel-pending` to `canceled` only for that successful outcome.

#### Scenario: Successful executor cancellation becomes terminal
- **WHEN** an executor successfully completes cancellation and returns `cancelled`
- **THEN** the runtime SHALL persist the execution as `canceled`

#### Scenario: Unsuccessful executor cancellation remains pending
- **WHEN** executor cancellation returns another outcome or raises
- **THEN** the runtime SHALL leave the execution `cancel-pending` for bounded retry

### Requirement: Cancel adapter cleanup SHALL be safe to retry
Executor cancellation SHALL be safe to retry when a runtime resumes an interrupted `cancel-pending` execution or concurrent cancellation drivers invoke cleanup for the same selected execution.

#### Scenario: Repeated cancel does not corrupt external cleanup
- **WHEN** the same `cancel-pending` execution receives repeated cancel requests
- **THEN** the executor SHALL tolerate every repeated request
- **AND** it SHALL leave the external resource stopped or in a terminal cleanup state

### Requirement: Executors SHALL prune normally completed resources through cleanup
Every executor SHALL expose idempotent cleanup distinct from invoke and cancel. Cleanup SHALL prune external resources after result publication and SHALL return success, retry with resumable state and optional delay, or a failure code with diagnostics. Cleanup SHALL NOT publish results or determine execution lifecycle.

#### Scenario: Active finalization requests retry
- **WHEN** result is published but an executor-owned process or remote job is still finalizing
- **THEN** cleanup returns retry without terminating required finalization work

#### Scenario: Completed resource is pruned
- **WHEN** executor-owned resources are safe to remove
- **THEN** cleanup removes them and returns success

#### Scenario: Repeated successful cleanup is harmless
- **WHEN** cleanup is repeated after resources were removed
- **THEN** it returns success without recreating or corrupting resources

### Requirement: Built-in executors SHALL move normal teardown out of invoke
Normal terminal invoke handling SHALL not be the sole teardown path. Script cleanup SHALL reap completed supervisor work and remove its work directory. Docker cleanup SHALL remove the terminal container and temporary image. Batch cleanup SHALL prune the job's temporary definition and any execution-owned resources after safe terminal observation. SSH cleanup SHALL preserve nested cleanup semantics across fresh remote calls. Cancellation MAY continue to tear down the same resources for canceled work.

#### Scenario: Published result bypasses further invoke
- **WHEN** a result is published before another invoke observes terminal work
- **THEN** explicit cleanup still prunes the built-in executor's resources

#### Scenario: Cancellation teardown remains available
- **WHEN** an execution is selected for cancellation before result publication
- **THEN** executor cancellation tears down its resources without requiring cleanup first

### Requirement: Ephemeral nested adapter drivers SHALL finish nested cleanup
When Docker or Batch runs a nested adapter in an ephemeral environment using its internal polling loop, that nested driver SHALL complete or terminally record nested cleanup before the environment exits. Outer cleanup SHALL independently prune the wrapper container, job, image, or job definition.

#### Scenario: Containerized nested execution completes
- **WHEN** a nested adapter publishes its result inside Docker execution
- **THEN** the nested driver performs nested cleanup before exiting
- **AND** outer Docker cleanup later removes wrapper resources
