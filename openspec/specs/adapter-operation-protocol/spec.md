## Purpose
Define distinct adapter operation contracts so invocation and cancellation have separate payloads, response semantics, and executor dispatch paths.

## Requirements

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
Invoke and cleanup SHALL return an object with nonempty string `status`, optional object-or-null `adapter_state`, optional non-boolean nonnegative integer `retry_after_ms`, and optional string-or-null `error`. Status `success` SHALL mean the requested operation completed and SHALL NOT carry error text or retry delay. Status `retry` SHALL be the only nonterminal status, SHALL require resumable object adapter state, SHALL NOT carry error text, and MAY carry retry delay. A provider-specific failure status SHALL require nonempty error text and SHALL NOT carry retry delay. The retired status `running`, contradictory status/error combinations, unknown fields, and malformed output SHALL raise a deliberate protocol error rather than being normalized or passed through.

Invoke failure SHALL cause the caller to publish a cached adapter-error DAG and failed lifecycle. Cleanup success SHALL mark cleanup complete, cleanup retry SHALL leave cleanup pending, and cleanup failure SHALL record failed cleanup diagnostics. No cleanup response SHALL change execution lifecycle or result.

#### Scenario: Invoke retry persists continuation and delay
- **WHEN** invoke returns `retry` with object adapter state and optional valid retry-after
- **THEN** the driver owner persists the state and shared delay

#### Scenario: Invoke success requires a published result
- **WHEN** invoke returns `success`
- **THEN** the caller rereads semantic state and accepts success only when result ref is populated
- **AND** otherwise it publishes an adapter protocol error DAG

#### Scenario: Invoke failure becomes cached execution failure
- **WHEN** invoke returns a valid provider failure code with nonempty error text
- **THEN** the caller publishes an adapter-error DAG with failed lifecycle

#### Scenario: Retired running status is rejected
- **WHEN** any adapter operation returns status `running`
- **THEN** the caller reports malformed adapter output and does not interpret it as retry or provider failure

#### Scenario: Cleanup retry affects only driver state
- **WHEN** cleanup returns valid `retry`
- **THEN** cleanup remains pending and shared not-before is updated
- **AND** result and lifecycle remain unchanged

#### Scenario: Cleanup failure is observable but not an execution failure
- **WHEN** cleanup returns a valid failure code with nonempty error text
- **THEN** driver cleanup records failed status and that error
- **AND** the cached execution outcome remains reusable

#### Scenario: Contradictory response is rejected
- **WHEN** a response combines success with error text, failure without diagnostics, retry without object state, or retry-only fields with a terminal status
- **THEN** the caller reports a deliberate protocol error

#### Scenario: Retry updates adapter state
- **WHEN** an invoke response reports valid `retry`
- **THEN** the lock owner persists its returned object state and later retries the same execution ID

#### Scenario: Success updates adapter state
- **WHEN** an invoke response reports valid `success`
- **THEN** the lock owner persists its returned object-or-null state before completing result handling

#### Scenario: Other outcome becomes cached error DAG
- **WHEN** an invoke response is a valid diagnostic failure rather than `retry` or `success`
- **THEN** the runtime commits an error DAG to `result_ref`
- **AND** the current cache pointer remains bound to that execution

### Requirement: Cancel responses SHALL determine cancellation progress
A cancel response with status `cancelled` SHALL confirm successful cancellation and SHALL NOT carry error text or retry delay. Status `retry` SHALL require object adapter state, SHALL NOT carry error text, and MAY carry a non-boolean nonnegative retry delay that is persisted to shared `driver.not_before`. A provider failure status SHALL be a nonempty code other than `cancelled`, `retry`, `running`, or `success`, SHALL require nonempty error text, and SHALL NOT carry retry delay. A failure response, malformed output, or adapter invocation error SHALL not confirm cancellation and SHALL leave the execution eligible for another bounded attempt. One execution's unsuccessful response SHALL not prevent collection of other concurrent cancellation outcomes.

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

### Requirement: Adapter operation dispatch SHALL preserve executor responsibilities
The adapter SHALL dispatch invoke with null adapter state to executor start and invoke with object state to executor status inspection. It SHALL dispatch cleanup to an idempotent executor cleanup method regardless of whether adapter state is null, and cancel to executor cancellation. Cancel dispatch SHALL pass the request's `argv_ref` unchanged to the executor plugin as keyword `argv_ref`; it SHALL NOT rename that value to `argv_ptr` or another compatibility alias. Executors SHALL retain sufficient durable state for repeated operations without duplicating work or corrupting cleanup.

#### Scenario: Null state starts execution
- **WHEN** invoke has null adapter state
- **THEN** the executor idempotently starts or rediscovers work for that execution ID

#### Scenario: Stored state checks execution
- **WHEN** invoke has object adapter state
- **THEN** the executor performs an idempotent status check

#### Scenario: Cancellation preserves argv ref name
- **WHEN** cancel carries `argv_ref`
- **THEN** executor cancellation receives the same value as keyword `argv_ref`
- **AND** no old/new argument translation occurs

#### Scenario: Repeated cleanup is safe
- **WHEN** cleanup is repeated after a lost response
- **THEN** the executor leaves resources pruned and returns a stable success

#### Scenario: Repeated terminal check is stable
- **WHEN** a terminal execution is checked again after a stale caller discarded its response
- **THEN** the adapter returns stable status and state without repeating the work

### Requirement: Cleanup request SHALL use an explicit schema
Cleanup SHALL accept exactly `operation = "cleanup"`, nonempty `execution_id`, nonempty `cache_key`, remote object containing nonempty `root`, runnable object, object-or-null `adapter_state`, nonempty `scratch_uri`, and syntactically typed non-null DAG `result_ref`. Unspecified fields SHALL be rejected.

#### Scenario: Valid cleanup request is dispatched
- **WHEN** every required cleanup field is valid
- **THEN** the adapter dispatches cleanup for the identified execution

#### Scenario: Cleanup without result is rejected
- **WHEN** cleanup omits result ref or supplies null
- **THEN** the adapter rejects the request without invoking executor cleanup
