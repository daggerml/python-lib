## ADDED Requirements

### Requirement: Shared `Dml` exposes one bounded runtime cancellation operation
The shared `Dml` runtime namespace SHALL expose `dml.runtime.cancel(execution: Ref, max_retries=3)`. It SHALL validate the runtime ref and nonnegative integer retry count, run the complete cancellation workflow, return only after every selected execution is canceled successfully, and raise an observable cancellation failure when work remains after the initial attempt and at most `max_retries` retry rounds.

#### Scenario: Runtime cancellation succeeds
- **WHEN** every selected execution confirms cancellation within its retry budget
- **THEN** `dml.runtime.cancel(...)` SHALL return successfully

#### Scenario: Runtime cancellation exhausts retries
- **WHEN** any selected execution remains unsuccessful after its retry budget
- **THEN** `dml.runtime.cancel(...)` SHALL raise a cancellation failure identifying the remaining executions

#### Scenario: Runtime cancellation rejects invalid input
- **WHEN** a caller supplies a string execution identity, a negative retry count, or a boolean retry count
- **THEN** cancellation SHALL fail before cancellation state is changed

## REMOVED Requirements

### Requirement: Shared `Dml` exposes runtime cancel with explicit mode selection
**Reason**: Persisted `cancel-pending` state makes separate full and drive entry modes unnecessary.

**Migration**: Call `dml.runtime.cancel(execution, max_retries=...)`; repeated calls automatically resume persisted cancellation work.
