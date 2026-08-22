## MODIFIED Requirements

### Requirement: Adapter operations SHALL use separate invoke and cancel contracts
Invoke and cancel requests SHALL remain distinct. Both SHALL identify the execution and may carry current `adapter_state`; invoke requests SHALL carry invocation data, while cancel requests SHALL read target argv identity from the unified execution record's `argv_ref`. Neither adapter operation SHALL directly mutate execution state.

#### Scenario: Invoke receives current adapter state
- **WHEN** the runtime starts or checks execution `e1`
- **THEN** it sends the adapter `execution_id`, invocation data, and current `adapter_state`

#### Scenario: Cancel reads execution-owned argv
- **WHEN** the runtime cancels execution `e1`
- **THEN** its cancel request carries `argv_ref` from `execution/e1`
- **AND** it does not resolve active or cancel-target refs

### Requirement: Adapter operation responses SHALL remain operation-specific
Running invoke responses SHALL return object state for persistence as `adapter_state`; terminal invoke and cancel responses may omit or return null state. Invoke status `running` SHALL mean retry, `succeeded` SHALL mean success, and any other reported status SHALL be treated as an error. Unrecoverable malformed protocol output SHALL raise a deliberate repository error. The runtime SHALL commit an error DAG, store it as `result_ref`, mark lifecycle `failed`, and retain the cache pointer for a reported non-success outcome. Adapter cancel responses SHALL remain advisory to runtime-owned cancelation lifecycle.

#### Scenario: Retry updates adapter state
- **WHEN** an invoke response reports `running`
- **THEN** the lock owner persists its returned state and later retries the same execution ID

#### Scenario: Success updates adapter state
- **WHEN** an invoke response reports `succeeded`
- **THEN** the lock owner persists its returned state before completing result handling

#### Scenario: Other outcome becomes cached error DAG
- **WHEN** an invoke response is neither valid `running` nor valid `succeeded`
- **THEN** the runtime commits an error DAG to `result_ref`
- **AND** the current cache pointer remains bound to that execution

### Requirement: Adapter operation dispatch SHALL preserve executor responsibilities
The adapter SHALL dispatch a fresh invoke when `adapter_state` is null and an idempotent status check when state is present. Executors SHALL maintain sufficient durable external state so repeated calls for one execution ID report the same running or terminal work without duplicating it. Adapter-owned IO MAY use `io/<execution_id>/` but SHALL NOT mutate the execution record.

#### Scenario: Null state starts execution
- **WHEN** an invoke request has null `adapter_state`
- **THEN** the adapter starts work for that execution ID

#### Scenario: Stored state checks execution
- **WHEN** an invoke request has object `adapter_state`
- **THEN** the adapter performs an idempotent status check for the same execution ID

#### Scenario: Repeated terminal check is stable
- **WHEN** a terminal execution is checked again after a stale caller discarded its response
- **THEN** the adapter returns terminal status and state without repeating the work
