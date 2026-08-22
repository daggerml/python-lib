## MODIFIED Requirements

### Requirement: Executors SHALL handle runtime cancel invocation as a synchronous cancellation step
When the runtime invokes an executor through an `AdapterCancelRequest` for a `cancel-pending` execution, the executor SHALL treat that invocation as synchronous cancellation work for the identified execution attempt. The executor cancel contract SHALL remain separate from execution-record lifecycle ownership.

#### Scenario: Executor cancel invocation uses a cancel request
- **WHEN** Phase 2 invokes executor cancellation for selected execution `e1`
- **THEN** the executor receives `AdapterCancelRequest` data for `e1`
- **AND** the executor does not receive a mixed invoke envelope with cancellation fields

#### Scenario: Leaf executor tears down directly
- **WHEN** an executor has no nested runtime work to cancel
- **THEN** it SHALL tear down its own external resources and return without recursive cancellation

### Requirement: Cancel-path return values SHALL remain advisory only
Executors SHALL return a success or failure indication from cancel handling, but the runtime SHALL continue to own execution-record lifecycle persistence, including the compare-and-swap from `cancel-pending` to `canceled`.

#### Scenario: Executor return does not own lifecycle persistence
- **WHEN** an executor returns from one cancel-path invocation
- **THEN** that return SHALL NOT itself define or persist execution-record lifecycle values
- **AND** the runtime SHALL remain responsible for the `canceled` transition

## ADDED Requirements

### Requirement: Cancel adapter cleanup SHALL be safe to retry
Executor cancellation SHALL be safe to retry when a runtime resumes an interrupted `cancel-pending` execution or concurrent cancellation drivers invoke cleanup for the same selected execution.

#### Scenario: Repeated cancel does not corrupt external cleanup
- **WHEN** the same `cancel-pending` execution receives repeated cancel requests
- **THEN** the executor SHALL tolerate every repeated request
- **AND** it SHALL leave the external resource stopped or in a terminal cleanup state

## REMOVED Requirements

### Requirement: Cancel adapter cleanup SHALL be safe after readiness timeout
**Reason**: Cancellation no longer has a readiness lifecycle or timeout; retry safety applies to interrupted and concurrent Phase 2 drivers instead.

**Migration**: Make cancel handling idempotent for repeated `cancel-pending` cleanup calls without relying on a readiness timeout.
