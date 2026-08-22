## MODIFIED Requirements

### Requirement: ExecutionState SHALL expose a public mutation lifecycle guard
The runtime SHALL expose a canonical mutation guard that reads `state.json`, classifies lifecycle for activation or mutation, and returns current semantic state or raises a typed execution-status error. Activation SHALL accept only `pending`; mutation SHALL accept only `running`; and `cancel-pending` or `canceled` SHALL raise `CanceledExecutionError`. The guard SHALL NOT require the driver lock merely to read or conditionally mutate semantic state.

#### Scenario: Activation accepts pending state
- **WHEN** the activation guard reads lifecycle pending
- **THEN** it returns current semantic state

#### Scenario: Mutation accepts running state
- **WHEN** the mutation guard reads lifecycle running
- **THEN** it returns current semantic state

#### Scenario: Cancellation blocks runtime publication
- **WHEN** either guard reads cancel-pending or canceled
- **THEN** it raises `CanceledExecutionError`
- **AND** no result or lineage mutation is persisted

#### Scenario: Activation guard accepts pending execution
- **WHEN** the activation guard reads lifecycle pending
- **THEN** it returns current semantic state

#### Scenario: Mutation guard accepts running execution
- **WHEN** the mutation guard reads lifecycle running
- **THEN** it returns current semantic state

#### Scenario: Activation guard rejects non-pending non-cancel states
- **WHEN** the activation guard reads running, succeeded, or failed
- **THEN** it raises `BadExecutionStatusError`

#### Scenario: Mutation guard rejects non-running non-cancel states
- **WHEN** the mutation guard reads pending, succeeded, or failed
- **THEN** it raises `BadExecutionStatusError`

#### Scenario: Cancel-pending blocks mutation
- **WHEN** the guard reads cancel-pending
- **THEN** it raises `CanceledExecutionError` without persisting the mutation

#### Scenario: Canceled blocks mutation
- **WHEN** the guard reads canceled
- **THEN** it raises `CanceledExecutionError` without invoking adapter cancellation

### Requirement: Execution coordination retries SHALL not silently abandon CAS mutations
Every state or driver compare-and-swap workflow SHALL retry conflicts with bounded exponential backoff and jitter while its latest semantic preconditions remain valid. Each retry SHALL reread the object it intends to mutate. Retry exhaustion SHALL be observable as an error rather than success, and a later caller SHALL be able to resume from durable state.

#### Scenario: State conflict is retried from latest semantics
- **WHEN** result, lifecycle, lineage, or control state CAS conflicts
- **THEN** the runtime rereads state and retries only while the intended transition remains valid

#### Scenario: Driver conflict checks ownership
- **WHEN** a driver mutation conflicts
- **THEN** the caller rereads driver state and continues only if it still owns the lock

#### Scenario: Exhaustion is observable and resumable
- **WHEN** a bounded retry deadline is exhausted
- **THEN** the operation raises a coordination error
- **AND** another caller can resume from the persisted files

#### Scenario: Registration conflict is retried from the latest record
- **WHEN** a child-registration state CAS conflicts
- **THEN** the runtime rereads current semantic state and reevaluates lifecycle before retrying

#### Scenario: Cancellation conflict is re-evaluated
- **WHEN** a cancellation state CAS conflicts
- **THEN** the runtime rereads lifecycle and valid caller references before retrying

#### Scenario: Exhaustion is observable to the caller
- **WHEN** the bounded coordination retry deadline is exhausted
- **THEN** the runtime raises an error rather than reporting success

### Requirement: Execution mutations SHALL be serialized by embedded owner locks
Each `driver.json` SHALL contain a nullable owner lock. Lock acquisition SHALL use compare-and-swap to replace a null or expired lock with a fresh UUID4 owner. Every adapter invocation and `driver.json` mutation other than acquisition SHALL require the current owner and compare-and-swap against the latest driver object. `state.json` mutations SHALL use guarded CAS without requiring this lock. Unlock SHALL clear the lock only when the stored owner matches.

#### Scenario: One driver acquires an unlocked execution
- **WHEN** two callers concurrently attempt to acquire the null driver lock
- **THEN** exactly one conditional update succeeds

#### Scenario: Funk publication proceeds during adapter ownership
- **WHEN** one caller holds the driver lock during an adapter call
- **THEN** the funk runtime may still publish a valid result to state

#### Scenario: Stale driver cannot persist adapter response
- **WHEN** an adapter call returns after another owner has stolen the lock
- **THEN** the stale caller does not persist adapter or retry state

#### Scenario: One caller acquires an unlocked execution
- **WHEN** two callers concurrently attempt to replace a null driver lock
- **THEN** exactly one conditional update succeeds

#### Scenario: Stale owner cannot mutate after a steal
- **WHEN** owner `o2` steals an expired lock from `o1`
- **THEN** a driver mutation from `o1` fails after rereading owner `o2`

#### Scenario: Stale unlock preserves replacement owner
- **WHEN** owner `o1` attempts to unlock after owner `o2` steals the lock
- **THEN** the runtime does not clear `o2`'s lock

### Requirement: Lock expiry SHALL use S3 response time
The runtime SHALL determine driver-lock expiry using `LastModified + lock.ttl <= Date`, where both timestamps come from the same `driver.json` response. It SHALL NOT use caller wall-clock time for lock expiry. Expiry SHALL permit lock stealing but SHALL NOT revoke an unchanged owner by itself.

#### Scenario: Driver timestamps report an expired lock
- **WHEN** a driver response has `LastModified + lock.ttl <= Date`
- **THEN** another caller may attempt to replace the lock owner by CAS

#### Scenario: Owner mutation refreshes the lease basis
- **WHEN** the lock owner successfully mutates driver state
- **THEN** subsequent expiry checks use the updated driver object timestamps

#### Scenario: Owner mutation refreshes lease basis
- **WHEN** the lock owner successfully mutates driver state
- **THEN** subsequent expiry checks use the updated driver object timestamps

#### Scenario: S3 timestamps report an expired lock
- **WHEN** a driver response has `LastModified + lock.ttl <= Date`
- **THEN** another caller may attempt to replace the owner by CAS

#### Scenario: Expired owner remains authoritative until stolen
- **WHEN** an adapter response arrives after TTL but the driver still contains that caller's owner
- **THEN** that caller may persist the response through owner-checked CAS

### Requirement: Cache resolution SHALL coordinate one current execution
On a cache miss, the runtime SHALL create fresh metadata, state, and driver objects for one UUID7 execution before conditionally creating `cache/<cache_key>` containing only that execution ID. If pointer creation conflicts, the runtime SHALL conditionally delete only its unchanged three new objects and reread the winner. UUID ordering SHALL NOT select the winner.

#### Scenario: Concurrent cache miss has one winner
- **WHEN** multiple callers prepare different three-object executions for one absent cache key
- **THEN** conditional cache-pointer creation selects exactly one current execution
- **AND** losers remove only unchanged objects they created

#### Scenario: Complete execution exists before pointer publication
- **WHEN** cache pointer creation succeeds for `e1`
- **THEN** all three required execution objects for `e1` already exist

#### Scenario: Execution exists before pointer publication
- **WHEN** a caller successfully creates `cache/ck1` containing `e1`
- **THEN** all three execution objects for `e1` already exist

#### Scenario: Lost claim cleans only the losing record
- **WHEN** execution `e2` loses cache-pointer creation to `e1`
- **THEN** its caller conditionally deletes only the unchanged objects created for `e2`

## ADDED Requirements

### Requirement: Shared retry delay SHALL coordinate adapter backpressure
An adapter `retry` response MAY include nonnegative `retry_after_ms`. The current driver owner SHALL persist `not_before` as a shared absolute timestamp derived from that delay, or from the runtime's standard retry delay when the hint is absent. Before invoke or cleanup, every caller SHALL acquire the driver lock, reread state and driver, and skip the adapter call while `not_before` remains in the future. Cancelation SHALL not be delayed by `not_before`.

#### Scenario: Backpressure delays all callers
- **WHEN** one invoke returns retry with `retry_after_ms = 5000`
- **THEN** the owner persists a shared not-before timestamp
- **AND** other callers do not invoke or clean up that execution before it expires

#### Scenario: Current state determines delayed operation
- **WHEN** not-before expires
- **THEN** the next owner derives invoke or cleanup from current result and cleanup state
- **AND** no operation discriminator is stored with the delay

#### Scenario: Cancellation bypasses delay
- **WHEN** cancellation selects an execution whose not-before is in the future
- **THEN** cancellation coordination may invoke cancel without waiting for that timestamp
