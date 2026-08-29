## MODIFIED Requirements

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
