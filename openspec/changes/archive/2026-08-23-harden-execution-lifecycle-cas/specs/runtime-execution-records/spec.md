## MODIFIED Requirements

### Requirement: Adapter cancellation SHALL advance directly from cancel-pending to canceled
For every adapter-backed execution from the Phase 1 cancellation set that Phase 2 still observes as `cancel-pending`, Phase 2 SHALL build an `AdapterCancelRequest` from that execution's record, invoke the adapter synchronously while holding the execution lock, and compare-and-swap lifecycle directly to `canceled` only after status `cancelled`. Retry responses SHALL persist adapter state and `driver.not_before`; interrupted, failed, or exhausted cancellation SHALL remain recoverable from `cancel-pending`. Phase 2 SHALL accept `canceled` as concurrent completion and SHALL warn then drop `pending`, `running`, `succeeded`, or `failed` without invoking the cancel adapter.

#### Scenario: Adapter cancellation completes
- **WHEN** the applicable cancel adapter returns `cancelled` for a `cancel-pending` execution
- **THEN** the runtime SHALL compare-and-swap that execution directly to `canceled`

#### Scenario: Cancellation resumes after interruption
- **WHEN** adapter work is interrupted before `canceled` is persisted
- **THEN** the execution SHALL remain `cancel-pending`
- **AND** a later cancellation call SHALL be able to repeat the idempotent cancel operation

#### Scenario: Already canceled selection is complete
- **WHEN** Phase 2 observes a selected execution as `canceled`
- **THEN** it SHALL drop that execution without adapter invocation

#### Scenario: Unexpected selected lifecycle does not reach adapter
- **WHEN** Phase 2 observes a selected execution as `pending`, `running`, `succeeded`, or `failed`
- **THEN** it SHALL warn with the execution ID and lifecycle
- **AND** it SHALL drop that execution without adapter invocation

### Requirement: Adapter operations SHALL follow the runtime-owned execution contract
The runtime SHALL use distinct invoke, cleanup, and cancel requests. Repeated invoke requests SHALL carry current `adapter_state` and SHALL be the only start-or-status-check operation. Cleanup requests SHALL require a populated `result_ref` and SHALL carry that ref with current adapter state. Cancel requests SHALL be sent only for `cancel-pending`, carry `argv_ref` from metadata, respect persisted `driver.not_before`, and persist retry continuation state. Adapters SHALL NOT receive or mutate complete metadata, state, or driver objects.

#### Scenario: First adapter call uses null adapter state
- **WHEN** the runtime invokes an adapter for a new execution
- **THEN** the invoke request SHALL include null `adapter_state`

#### Scenario: Repeated invoke uses durable state
- **WHEN** an invoke response previously supplied adapter state
- **THEN** the next invoke for the same execution ID SHALL include that state
- **AND** no separate poll operation SHALL be used

#### Scenario: Cleanup receives the published result
- **WHEN** the caller drives cleanup for execution `e1`
- **THEN** the cleanup request SHALL include `e1`'s non-null result ref and current adapter state

#### Scenario: Cancel update uses execution-owned target
- **WHEN** the runtime invokes cancellation for a `cancel-pending` execution
- **THEN** it SHALL send that execution ID and `metadata.argv_ref`

#### Scenario: Runtime ignores unsuccessful cancel return for terminal lifecycle write
- **WHEN** a cancel adapter returns an outcome other than `cancelled`
- **THEN** runtime-owned coordination SHALL leave lifecycle `cancel-pending`

#### Scenario: Every remaining cancel-pending execution receives its cancel update
- **WHEN** Phase 1 selects a parent and one or more spawned adapter-backed executions
- **THEN** Phase 2 SHALL process the cancel adapter for each execution it still observes as `cancel-pending`

#### Scenario: Lifecycle drift is filtered before adapter request
- **WHEN** Phase 2 observes selected work outside `cancel-pending`
- **THEN** it SHALL apply lifecycle filtering without sending a cancel request

#### Scenario: Cancellation requester is stable across the selected set
- **WHEN** root cancellation selects nested executions
- **THEN** persisted cancellation records SHALL identify and preserve that requester

#### Scenario: Pending response is rejected
- **WHEN** an adapter returns status pending
- **THEN** the runtime SHALL treat it as a failure code requiring diagnostics, not a retry status

## ADDED Requirements

### Requirement: Execution state mutations SHALL obey field authority
Every mutation of `state.json` SHALL use compare-and-swap against the latest object. Mutations that change `spawned_execution_ids`, `child_execution_ids`, or the inseparable `result_ref` and `result_source` pair MAY proceed without the driver lock. Every mutation that changes lifecycle, cancelation, invalidation, or any other non-derived state field SHALL require the current driver lock owner and a lifecycle that permits that operation. Updating `updated_at` as part of an otherwise authorized mutation SHALL use the authority of that mutation. No persisted schema fields or lifecycle values SHALL be added.

#### Scenario: Result publication is lock-free CAS
- **WHEN** a running execution publishes a runtime result
- **THEN** it SHALL atomically CAS-update `result_ref`, `result_source`, and `updated_at` without requiring the driver lock

#### Scenario: Lineage summary mutation is lock-free CAS
- **WHEN** a caller registers or completes a direct child
- **THEN** it SHALL CAS-update the caller's lineage arrays and `updated_at` without requiring the caller's driver lock

#### Scenario: Lifecycle mutation requires lock ownership
- **WHEN** a writer attempts to change execution lifecycle
- **THEN** it SHALL hold the current execution driver lock
- **AND** it SHALL conditionally update the latest `state.json`

#### Scenario: Control mutation requires lock ownership
- **WHEN** a writer attempts to change cancelation or invalidation metadata
- **THEN** it SHALL hold the current execution driver lock
- **AND** it SHALL conditionally update the latest `state.json`

#### Scenario: Cancel-pending rejects unrelated control mutation
- **WHEN** a control writer rereads lifecycle `cancel-pending`
- **THEN** it SHALL not change invalidation or other unrelated state fields
- **AND** only normal terminal-child bookkeeping or cancellation completion MAY change that state

### Requirement: Execution lifecycle transitions SHALL be absorbing and guarded
The runtime SHALL permit only `pending -> running`, `pending -> cancel-pending`, `running -> succeeded`, `running -> failed`, `running -> cancel-pending`, and `cancel-pending -> canceled`. Every lifecycle CAS retry SHALL reread and revalidate its source lifecycle. The lifecycle values `succeeded`, `failed`, and `canceled` SHALL be absorbing and SHALL never transition to another lifecycle.

#### Scenario: Activation requires pending
- **WHEN** activation attempts to mark an execution running
- **THEN** it SHALL succeed only if the latest lifecycle is `pending`

#### Scenario: Normal completion requires running
- **WHEN** a coordinator attempts to mark an execution succeeded or failed
- **THEN** it SHALL succeed only if the latest lifecycle is `running`

#### Scenario: Cancellation completion requires cancel-pending
- **WHEN** a cancellation coordinator attempts to mark an execution canceled
- **THEN** it SHALL succeed only if the latest lifecycle is `cancel-pending`

#### Scenario: Terminal lifecycle is absorbing
- **WHEN** an execution lifecycle is `succeeded`, `failed`, or `canceled`
- **THEN** no writer SHALL change that lifecycle

### Requirement: Lock-free state writers SHALL respect lifecycle
Runtime result publication SHALL modify only a `running` execution. Initial spawned-child registration SHALL modify only a `running` caller. Normally terminal child bookkeeping SHALL atomically remove the child from `spawned_execution_ids` and add it to `child_execution_ids` when the caller is `running` or `cancel-pending`. If bookkeeping observes `cancel-pending`, it SHALL persist the lineage update before surfacing cancellation. Lock-free state mutation SHALL reject `pending`, `succeeded`, `failed`, and `canceled`, except where no field value would change.

#### Scenario: Cancellation blocks result publication
- **WHEN** result publication rereads lifecycle `cancel-pending` or `canceled`
- **THEN** it SHALL reject the publication without changing result fields

#### Scenario: Cancellation blocks initial child registration
- **WHEN** initial child registration rereads a caller lifecycle other than `running`
- **THEN** it SHALL not add the child to `spawned_execution_ids`

#### Scenario: Terminal child completes during caller cancellation
- **WHEN** a normally terminal child remains spawned and its caller is `cancel-pending`
- **THEN** the runtime SHALL CAS-move the child from spawned to completed lineage
- **AND** it SHALL surface cancellation only after that update succeeds

#### Scenario: Terminal state rejects lock-free mutation
- **WHEN** a lock-free writer rereads `succeeded`, `failed`, or `canceled`
- **THEN** it SHALL not change result or lineage state
