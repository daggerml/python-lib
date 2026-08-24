## MODIFIED Requirements

### Requirement: Shared `Dml` exposes one bounded runtime cancellation operation
The shared `Dml` runtime namespace SHALL expose `dml.runtime.cancel(execution: Ref, max_retries=3)`. It SHALL validate the runtime ref and nonnegative integer retry count, run the complete cancellation workflow, and raise an observable cancellation failure when `cancel-pending` work remains unsuccessful after the initial attempt and at most `max_retries` retry rounds. It SHALL return when every selected execution either reaches `canceled`, is observed concurrently as `canceled`, or is warned and dropped because Phase 2 observes `pending`, `running`, `succeeded`, or `failed` without invoking its cancel adapter.

#### Scenario: Runtime cancellation succeeds
- **WHEN** every selected `cancel-pending` execution confirms cancellation within its retry budget
- **THEN** `dml.runtime.cancel(...)` SHALL return successfully

#### Scenario: Concurrent completion succeeds
- **WHEN** Phase 2 observes a selected execution as already `canceled`
- **THEN** `dml.runtime.cancel(...)` SHALL treat that execution as complete

#### Scenario: Unexpected lifecycle is warned and dropped
- **WHEN** Phase 2 observes a selected execution as `pending`, `running`, `succeeded`, or `failed`
- **THEN** it SHALL warn and drop that execution without adapter invocation
- **AND** that execution SHALL not remain in the bounded retry set

#### Scenario: Runtime cancellation exhausts retries
- **WHEN** any selected execution remains `cancel-pending` and unsuccessful after its retry budget
- **THEN** `dml.runtime.cancel(...)` SHALL raise a cancellation failure identifying the remaining executions

#### Scenario: Runtime cancellation rejects invalid input
- **WHEN** a caller supplies a string execution identity, a negative retry count, or a boolean retry count
- **THEN** cancellation SHALL fail before cancellation state is changed
