## MODIFIED Requirements

### Requirement: Executors SHALL handle runtime cancel invocation as a synchronous cancellation step
When the runtime invokes an executor through an `AdapterCancelRequest`, the executor SHALL treat that invocation as synchronous cancellation work for the identified execution attempt. The executor cancel contract SHALL remain separate from execution-record-only lifecycle states such as `cancel-requested` and `cancel-ready`.

#### Scenario: Executor cancel invocation uses a cancel request
- **WHEN** the runtime dispatches cancellation for child execution `e1`
- **THEN** the executor receives `AdapterCancelRequest` data for `e1`
- **AND** the executor does not receive a mixed invoke envelope with cancellation fields

#### Scenario: Leaf executor tears down directly
- **WHEN** an executor has no nested runtime work to cancel
- **THEN** it tears down its own external resources and returns without recursive cancellation

## ADDED Requirements

### Requirement: Cancel adapter cleanup SHALL be safe after readiness timeout
Executor cancellation SHALL be safe to retry when normal distributed handoff and the 60-second readiness timeout race or when a runtime resumes after interruption.

#### Scenario: Repeated cancel does not corrupt external cleanup
- **WHEN** the same execution receives normal and timeout-driven cancel requests
- **THEN** the executor tolerates the repeated request
- **AND** it leaves the external resource stopped or in a terminal cleanup state
