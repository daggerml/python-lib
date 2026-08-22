## ADDED Requirements

### Requirement: Cancel responses SHALL determine cancellation progress
A cancel response with status `cancelled` SHALL confirm successful cancellation. A retry response SHALL persist returned adapter state and populate the shared `driver.not_before` from `retry_after_ms` before another invocation. A failure response, malformed output, or adapter invocation error SHALL not confirm cancellation and SHALL leave the execution eligible for another bounded attempt. One execution's unsuccessful response SHALL not prevent collection of other concurrent cancellation outcomes.

#### Scenario: Cancel response confirms success
- **WHEN** a cancel adapter returns status `cancelled`
- **THEN** the runtime SHALL treat that execution's cancellation attempt as successful

#### Scenario: Cancel response remains unsuccessful
- **WHEN** a cancel adapter returns any other outcome or fails to produce a valid response
- **THEN** the runtime SHALL retain that execution for a bounded retry

#### Scenario: Cancel retry controls the next request
- **WHEN** a cancel adapter returns `retry` with adapter state and `retry_after_ms`
- **THEN** the runtime SHALL persist that state and deadline
- **AND** it SHALL not issue the next cancel request before `driver.not_before`
