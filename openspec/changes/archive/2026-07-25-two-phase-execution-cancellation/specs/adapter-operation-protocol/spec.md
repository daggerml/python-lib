## ADDED Requirements

### Requirement: Adapter operations SHALL use separate invoke and cancel contracts
The runtime SHALL define distinct `AdapterInvokeRequest` / `AdapterInvokeResponse` and `AdapterCancelRequest` / `AdapterCancelResponse` contracts. An invoke request SHALL contain invocation data and resume state. A cancel request SHALL contain cancellation data and SHALL carry the argv pointer needed to identify the target without resolving the mutable active pointer.

#### Scenario: Invoke request excludes cancellation-only fields
- **WHEN** the runtime starts or resumes an adapter execution
- **THEN** it sends an `AdapterInvokeRequest`
- **AND** the request does not use cancellation requester or cancellation lifecycle fields

#### Scenario: Cancel request carries execution-owned argv identity
- **WHEN** the runtime cancels execution `e1`
- **THEN** it sends an `AdapterCancelRequest` containing `execution_id = "e1"`
- **AND** it includes the argv pointer obtained from `refs/cancel-targets/e1.json`
- **AND** it does not resolve the argv through `active/<cache_key>`

### Requirement: Adapter operation responses SHALL remain operation-specific
`AdapterInvokeResponse` SHALL report invocation progress or result in its `status` field using `running`, `succeeded`, or `failed`. `AdapterCancelResponse` SHALL report cancellation outcome separately and SHALL NOT define runtime execution lifecycle values such as `cancel-requested`, `cancel-ready`, or `canceled`.

#### Scenario: Invoke response reports execution progress
- **WHEN** an adapter invocation is still running
- **THEN** the adapter returns `AdapterInvokeResponse` with `status = "running"` and resumable state

#### Scenario: Cancel response does not own runtime lifecycle
- **WHEN** an adapter completes a cancellation request
- **THEN** the adapter returns `AdapterCancelResponse`
- **AND** the runtime, rather than the response, decides when to persist `canceled` or `cancel-ready`

### Requirement: Adapter operation dispatch SHALL preserve executor responsibilities
An invoke operation SHALL dispatch to executor `start()` when no resume state exists and to `poll()` when resume state exists. A cancel operation SHALL dispatch to executor `cancel()`.

#### Scenario: Invoke dispatches a fresh start
- **WHEN** an `AdapterInvokeRequest` has no resume state
- **THEN** the adapter dispatches to executor `start()`

#### Scenario: Cancel dispatches cleanup
- **WHEN** an `AdapterCancelRequest` is received
- **THEN** the adapter dispatches to executor `cancel()`
