## MODIFIED Requirements

### Requirement: Adapter operations SHALL use separate invoke and cancel contracts
Adapters SHALL accept distinct `invoke`, `cleanup`, and `cancel` requests. Invoke SHALL identify execution, runnable, remote, scratch URI, cache key, and current adapter state. Cleanup SHALL carry those fields plus a non-null result ref. Cancel SHALL carry cancelation metadata and argv identity. Repeated invoke SHALL serve both initial launch and status checks; there SHALL be no poll operation. No operation SHALL directly mutate execution files.

#### Scenario: Invoke infers launch or continuation
- **WHEN** invoke receives an execution ID and null or object adapter state
- **THEN** the adapter uses those values to idempotently start or inspect that execution

#### Scenario: Cleanup receives result context
- **WHEN** cleanup is requested
- **THEN** the request includes the execution's published result ref and current adapter state

#### Scenario: Poll operation is rejected
- **WHEN** an adapter receives `operation = "poll"`
- **THEN** it rejects the request as unsupported

#### Scenario: Invoke receives current adapter state
- **WHEN** the runtime starts or checks execution `e1`
- **THEN** it sends the adapter `execution_id`, invocation data, and current `adapter_state`

#### Scenario: Cancel reads execution-owned argv
- **WHEN** the runtime cancels execution `e1`
- **THEN** its cancel request carries `argv_ref` from `metadata.json`
- **AND** it does not resolve active or cancel-target refs

### Requirement: Adapter operation responses SHALL remain operation-specific
Invoke and cleanup SHALL return an object with nonempty string `status`, optional object-or-null `adapter_state`, optional nonnegative integer `retry_after_ms`, and optional string-or-null `error`. Status `success` SHALL mean the requested operation completed. Status `retry` SHALL mean the operation remains incomplete and SHALL require resumable object adapter state; its retry delay SHALL be advisory input to shared caller backpressure. Every other status SHALL be a failure code and SHALL require nonempty error text. Malformed output SHALL raise a deliberate protocol error.

Invoke failure SHALL cause the caller to publish a cached adapter-error DAG and failed lifecycle. Cleanup success SHALL mark cleanup complete, cleanup retry SHALL leave cleanup pending, and cleanup failure SHALL record failed cleanup diagnostics. No cleanup response SHALL change execution lifecycle or result.

#### Scenario: Invoke retry persists continuation and delay
- **WHEN** invoke returns retry with object adapter state and optional retry-after
- **THEN** the driver owner persists the state and shared delay

#### Scenario: Invoke success requires a published result
- **WHEN** invoke returns success
- **THEN** the caller rereads semantic state and accepts success only when result ref is populated
- **AND** otherwise it publishes an adapter protocol error DAG

#### Scenario: Invoke failure becomes cached execution failure
- **WHEN** invoke returns any status other than success or retry
- **THEN** the caller publishes an adapter-error DAG with failed lifecycle

#### Scenario: Cleanup retry affects only driver state
- **WHEN** cleanup returns retry
- **THEN** cleanup remains pending and shared not-before is updated
- **AND** result and lifecycle remain unchanged

#### Scenario: Cleanup failure is observable but not an execution failure
- **WHEN** cleanup returns a failure code with error text
- **THEN** driver cleanup records failed status and that error
- **AND** the cached execution outcome remains reusable

#### Scenario: Retry updates adapter state
- **WHEN** an invoke response reports `retry`
- **THEN** the lock owner persists its returned state and later retries the same execution ID

#### Scenario: Success updates adapter state
- **WHEN** an invoke response reports `success`
- **THEN** the lock owner persists its returned state before completing result handling

#### Scenario: Other outcome becomes cached error DAG
- **WHEN** an invoke response is neither valid `retry` nor valid `success`
- **THEN** the runtime commits an error DAG to `result_ref`
- **AND** the current cache pointer remains bound to that execution

### Requirement: Adapter operation dispatch SHALL preserve executor responsibilities
The adapter SHALL dispatch invoke with null adapter state to executor start and invoke with object state to executor status inspection. It SHALL dispatch cleanup to an idempotent executor cleanup method regardless of whether adapter state is null, and cancel to executor cancellation. Executors SHALL retain sufficient durable state for repeated operations without duplicating work or corrupting cleanup.

#### Scenario: Null state starts execution
- **WHEN** invoke has null adapter state
- **THEN** the executor idempotently starts or rediscovers work for that execution ID

#### Scenario: Stored state checks execution
- **WHEN** invoke has object adapter state
- **THEN** the executor performs an idempotent status check

#### Scenario: Repeated cleanup is safe
- **WHEN** cleanup is repeated after a lost response
- **THEN** the executor leaves resources pruned and returns a stable success

#### Scenario: Repeated terminal check is stable
- **WHEN** terminal work is checked again after a stale caller discarded its response
- **THEN** the adapter returns stable status and state without repeating the work

## ADDED Requirements

### Requirement: Cleanup request SHALL use an explicit schema
Cleanup SHALL accept exactly `operation = "cleanup"`, nonempty `execution_id`, nonempty `cache_key`, remote object containing nonempty `root`, runnable object, object-or-null `adapter_state`, nonempty `scratch_uri`, and syntactically typed non-null DAG `result_ref`. Unspecified fields SHALL be rejected.

#### Scenario: Valid cleanup request is dispatched
- **WHEN** every required cleanup field is valid
- **THEN** the adapter dispatches cleanup for the identified execution

#### Scenario: Cleanup without result is rejected
- **WHEN** cleanup omits result ref or supplies null
- **THEN** the adapter rejects the request without invoking executor cleanup
