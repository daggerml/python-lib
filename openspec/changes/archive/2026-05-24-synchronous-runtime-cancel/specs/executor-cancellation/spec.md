## MODIFIED Requirements

### Requirement: Executors SHALL handle `cancel-pending` as a synchronous cancellation step
When the runtime invokes an executor with `execution_status = "cancel-pending"`, the executor SHALL treat that call as synchronous cancellation work. The executor MAY delegate nested cancellation by calling `Dml.runtime.cancel(child)` for a direct child execution it is responsible for, then SHALL tear down its own external resources and return.

#### Scenario: Nested executor delegates once then tears down
- **WHEN** an executor owns nested execution work beneath execution `e1`
- **THEN** it MAY call `Dml.runtime.cancel(child)` once for each direct child execution it is responsible for
- **AND** it SHALL then perform its own teardown before returning

#### Scenario: Leaf executor tears down directly
- **WHEN** an executor has no nested runtime work to cancel
- **THEN** it SHALL tear down its own external resources and return without recursive cancellation

### Requirement: Nested adapter chains SHALL recurse through runtime cancel at most once per child execution
Executor stacks that wrap nested executors SHALL ensure that only one layer in the stack calls `Dml.runtime.cancel(child)` for a given child execution while handling one cancel update.

#### Scenario: Wrapper chain avoids duplicate recursive cancellation
- **WHEN** multiple executor layers participate in cancelling the same nested execution
- **THEN** at most one layer SHALL call `Dml.runtime.cancel(child)` for that child execution during that cancel update

### Requirement: Cancel-path return values SHALL be advisory only
Executors SHALL return a success or failure indication from cancel handling, but the runtime SHALL treat that indication as advisory and SHALL NOT use it as the source of truth for lifecycle state. A successful cancel update SHALL return `status = "cancelled"`. A failed cancel update SHALL return `status = "failed"` with an error payload.

#### Scenario: Runtime ignores successful cancel return for lifecycle ownership
- **WHEN** an executor returns a successful cancel response
- **THEN** the runtime SHALL still own the `cancelled` lifecycle write

#### Scenario: Successful cancel update returns cancelled
- **WHEN** an executor completes its cancel-path work without error
- **THEN** it SHALL return `status = "cancelled"`

#### Scenario: Failed cancel update returns failed
- **WHEN** an executor encounters an error while handling a cancel update
- **THEN** it SHALL return `status = "failed"`
- **AND** it SHALL include an error payload
