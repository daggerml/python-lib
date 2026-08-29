## MODIFIED Requirements

### Requirement: Executors SHALL tear down external resources during cancellation
Executor-owned cancellation SHALL tear down external resources and SHALL NOT mutate the persisted execution record `state`. Script execution SHALL terminate the supervisor-managed process tree and remove its work directory. Docker execution SHALL stop and remove the container and SHALL remove any temporary loaded image. Batch execution SHALL cancel or terminate the Batch job as appropriate and SHALL deregister the temporary job definition. SSH execution SHALL return the nested adapter's cancellation result and SHALL NOT create additional remote wrapper state.

#### Scenario: Batch cancellation tears down Batch resources
- **WHEN** the Batch executor receives an `AdapterCancelRequest`
- **THEN** it SHALL cancel or terminate the Batch job and deregister the temporary job definition
