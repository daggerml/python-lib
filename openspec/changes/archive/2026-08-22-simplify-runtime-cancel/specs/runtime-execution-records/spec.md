## MODIFIED Requirements

### Requirement: Adapter cancellation SHALL advance directly from cancel-pending to canceled
For every adapter-backed execution in the Phase 1 cancellation set, Phase 2 SHALL build an `AdapterCancelRequest` from that execution's record, invoke the adapter synchronously while holding the execution lock, and compare-and-swap lifecycle from `cancel-pending` directly to `canceled` only after status `cancelled`. Retry responses SHALL persist adapter state and `driver.not_before`; interrupted, failed, or exhausted cancellation SHALL remain recoverable from `cancel-pending`.

#### Scenario: Adapter cancellation completes
- **WHEN** the applicable cancel adapter returns `cancelled` for a `cancel-pending` execution
- **THEN** the runtime SHALL compare-and-swap that execution directly to `canceled`

#### Scenario: Cancellation resumes after interruption
- **WHEN** adapter work is interrupted before `canceled` is persisted
- **THEN** the execution SHALL remain `cancel-pending`
- **AND** a later cancellation call SHALL be able to repeat the idempotent cancel operation

### Requirement: Adapter operations SHALL follow the runtime-owned execution contract
The runtime SHALL use distinct invoke, cleanup, and cancel requests. Repeated invoke requests SHALL carry current `adapter_state` and SHALL be the only start-or-status-check operation. Cleanup requests SHALL require a populated `result_ref` and SHALL carry that ref with current adapter state. Cancel requests SHALL carry `argv_ref` from metadata, respect persisted `driver.not_before`, and persist retry continuation state. Adapters SHALL NOT receive or mutate complete metadata, state, or driver objects.

#### Scenario: First adapter call uses null adapter state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the invoke request includes null `adapter_state`

#### Scenario: Repeated invoke uses durable state
- **WHEN** an invoke response previously supplied adapter state
- **THEN** the next invoke for the same execution ID includes that state
- **AND** no separate poll operation is used

#### Scenario: Cleanup receives the published result
- **WHEN** the caller drives cleanup for execution `e1`
- **THEN** the cleanup request includes `e1`'s non-null result ref and current adapter state

#### Scenario: Cancel update uses execution-owned target
- **WHEN** the runtime invokes cancellation for a selected execution
- **THEN** it sends that execution ID and `metadata.argv_ref`

#### Scenario: Runtime ignores cancel return for terminal lifecycle write
- **WHEN** a cancel adapter returns an outcome other than `cancelled`
- **THEN** runtime-owned coordination SHALL leave the lifecycle `cancel-pending`

#### Scenario: Every selected adapter-backed execution receives its own cancel update
- **WHEN** Phase 1 selects a parent and one or more spawned adapter-backed executions
- **THEN** Phase 2 processes each selected execution's cancel adapter

#### Scenario: Cancellation requester is stable across the selected set
- **WHEN** root cancellation selects nested executions
- **THEN** persisted cancellation records identify and preserve that requester

#### Scenario: Pending is rejected
- **WHEN** an adapter returns status pending
- **THEN** the runtime treats it as a failure code requiring diagnostics, not a retry status
